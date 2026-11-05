#include "helpers.h"

#include "private.h"

#include <yt/yt/library/query/base/join_profiler.h>
#include <yt/yt/library/query/base/query.h>

#include <yt/yt/library/query/engine_api/evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/library/query/row_comparer_api/row_comparer_generator.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/push_based_shuffle_client/config.h>
#include <yt/yt/ytlib/push_based_shuffle_client/record_format.h>
#include <yt/yt/ytlib/push_based_shuffle_client/sort_reader.h>
#include <yt/yt/ytlib/push_based_shuffle_client/sort_reader_adapter.h>
#include <yt/yt/ytlib/push_based_shuffle_client/sorted_merging_reader.h>

#include <yt/yt/ytlib/query_client/functions_cache.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/ytlib/table_client/key_set.h>
#include <yt/yt/ytlib/table_client/partitioner.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NControllerAgent::NProto;
using namespace NPushBasedShuffleClient;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;

using NScheduler::LocalClusterName;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = JobProxyClientLogger;

////////////////////////////////////////////////////////////////////////////////

void RunQuery(
    const NScheduler::NProto::TQuerySpec& querySpec,
    const TSchemalessReaderFactory& readerFactory,
    const TSchemalessWriterFactory& writerFactory,
    const std::optional<std::string>& udfDirectory)
{
    auto query = FromProto<TConstQueryPtr>(querySpec.query());
    auto resultSchema = query->GetTableSchema();
    auto resultNameTable = TNameTable::FromSchema(*resultSchema);
    auto writer = writerFactory(resultNameTable, resultSchema);

    auto externalCGInfo = New<TExternalCGInfo>();
    FromProto(&externalCGInfo->Functions, querySpec.external_functions());

    auto functionGenerators = New<TFunctionProfilerMap>();
    auto aggregateGenerators = New<TAggregateProfilerMap>();
    MergeFrom(functionGenerators.Get(), *GetBuiltinFunctionProfilers());
    MergeFrom(aggregateGenerators.Get(), *GetBuiltinAggregateProfilers());
    if (udfDirectory) {
        FetchFunctionImplementationsFromFiles(
            functionGenerators,
            aggregateGenerators,
            externalCGInfo,
            *udfDirectory);
    }

    auto evaluator = CreateEvaluator(New<TExecutorConfig>());
    auto reader = CreateSchemafulReaderAdapter(readerFactory, query->GetReadSchema());

    YT_TLOG_INFO("Reading, evaluating query and writing");
    evaluator->Run(
        query,
        reader,
        writer,
        TJoinProfilerRegistry({}, {}, GetDefaultMemoryChunkProvider(), Logger()),
        functionGenerators,
        aggregateGenerators,
        /*sdk*/ {},
        GetDefaultMemoryChunkProvider(),
        TQueryOptions(),
        MostFreshFeatureFlags(),
        MakeFuture(MostFreshFeatureFlags()));
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TDataSliceDescriptor> UnpackDataSliceDescriptors(const TTableInputSpec& inputTableSpec)
{
    return FromProto<std::vector<TDataSliceDescriptor>>(
        inputTableSpec.chunk_specs(),
        inputTableSpec.chunk_spec_count_per_data_slice(),
        inputTableSpec.virtual_row_index_per_data_slice());
}

std::vector<TDataSliceDescriptor> UnpackDataSliceDescriptors(const ::google::protobuf::RepeatedPtrField<TTableInputSpec>& specList)
{
    std::vector<TDataSliceDescriptor> dataSliceDescriptors;
    for (const auto& inputSpec : specList) {
        auto descriptors = UnpackDataSliceDescriptors(inputSpec);
        dataSliceDescriptors.insert(dataSliceDescriptors.end(), descriptors.begin(), descriptors.end());
    }
    return dataSliceDescriptors;
}

namespace {

int GetShuffleKeyColumnCount(const TSortColumns& sortColumnsWithIdentity)
{
    int keyColumnCount = std::ssize(sortColumnsWithIdentity) - IdentityColumnCount;
    YT_VERIFY(keyColumnCount > 0);
    YT_VERIFY(sortColumnsWithIdentity[keyColumnCount].Name == ShuffleProducerIdColumnName);
    YT_VERIFY(sortColumnsWithIdentity[keyColumnCount + 1].Name == ShuffleRowIdColumnName);
    return keyColumnCount;
}

} // namespace

ISchemalessMultiChunkReaderPtr CreatePushBasedShuffleMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    const TValidTaskJobIndexes& validTaskJobIndexes,
    const TSortColumns& sortColumns)
{
    int keyColumnCount = GetShuffleKeyColumnCount(sortColumns);

    return CreateIdentityAwareSortedMergingReader(
        readers,
        GetComparator(sortColumns),
        // TODO(apollo1321): Drop the ids from the merging reader API: they only re-check
        // that the name table built from the sort columns puts the identity at these positions.
        TIdentityColumnIds{
            .WriterId = keyColumnCount,
            .RowId = keyColumnCount + 1,
        },
        TValidWriterIds(
            validTaskJobIndexes.task_job_indexes().begin(),
            validTaskJobIndexes.task_job_indexes().end()));
}

namespace {

class TThreadOwningSortReader
    : public ISortReader
{
public:
    TThreadOwningSortReader(
        TActionQueuePtr readerQueue,
        IThreadPoolPtr sortThreadPool,
        ISortReaderPtr underlyingReader)
        : ReaderQueue_(std::move(readerQueue))
        , SortThreadPool_(std::move(sortThreadPool))
        , UnderlyingReader_(std::move(underlyingReader))
    { }

    TFuture<TSharedRange<TUnversionedRow>> Read() final
    {
        return UnderlyingReader_->Read();
    }

    void AddChunk(
        TChunkId chunkId,
        TChunkReplicaWithMediumList replicas,
        i64 startRecordIndex,
        std::optional<i64> rangeEndRecordIndex) final
    {
        UnderlyingReader_->AddChunk(chunkId, std::move(replicas), startRecordIndex, rangeEndRecordIndex);
    }

    void SetNoMoreChunks() final
    {
        UnderlyingReader_->SetNoMoreChunks();
    }

    void FinishAtCurrentCommittedRecordCount() final
    {
        UnderlyingReader_->FinishAtCurrentCommittedRecordCount();
    }

private:
    const TActionQueuePtr ReaderQueue_;
    const IThreadPoolPtr SortThreadPool_;
    const ISortReaderPtr UnderlyingReader_;
};

} // namespace

ISchemalessMultiChunkReaderPtr CreatePushBasedShuffleSortReader(
    const TJobSpecExt& jobSpecExt,
    const TPushBasedShuffleSortReaderSpec& readerSpec,
    TNameTablePtr nameTable,
    const TSortColumns& outputSortColumns,
    const TMultiChunkReaderHostPtr& chunkReaderHost,
    TClosure onInputFetched)
{
    auto keySortColumns = outputSortColumns;
    TSortReaderMode mode;
    if (readerSpec.has_valid_task_job_indexes()) {
        mode = TValidWriterIds(
            readerSpec.valid_task_job_indexes().task_job_indexes().begin(),
            readerSpec.valid_task_job_indexes().task_job_indexes().end());
    } else {
        keySortColumns.resize(GetShuffleKeyColumnCount(keySortColumns));
        // Intermediate sort: names the identity values the reader appends to rows,
        // so the writer stores them in the chunk's identity columns.
        mode = TIdentityColumnIds{
            .WriterId = nameTable->RegisterName(ShuffleProducerIdColumnName),
            .RowId = nameTable->RegisterName(ShuffleRowIdColumnName),
        };
    }

    auto readerQueue = New<TActionQueue>("ShuffleReader");
    auto sortThreadPool = CreateThreadPool(readerSpec.sort_thread_count(), "ShuffleSort");

    auto localChunkReaderHost = chunkReaderHost->CreateHostForCluster(LocalClusterName);
    auto reader = CreateSortReader(
        ConvertTo<TSortReaderConfigPtr>(
            TYsonString(readerSpec.sort_reader_config())),
        ConvertTo<TPartitionReaderConfigPtr>(
            TYsonString(readerSpec.partition_reader_config())),
        FromProto<NCompression::ECodec>(readerSpec.codec()),
        localChunkReaderHost->Client,
        localChunkReaderHost,
        readerSpec.read_quorum(),
        GetComparator(keySortColumns),
        std::move(mode),
        std::move(onInputFetched),
        readerQueue->GetInvoker(),
        sortThreadPool->GetInvoker());

    YT_VERIFY(jobSpecExt.input_table_specs_size() == 1);
    for (const auto& chunkSpec : jobSpecExt.input_table_specs(0).chunk_specs()) {
        auto upperRecordIndex = YT_OPTIONAL_FROM_PROTO(chunkSpec.upper_limit(), row_index);

        reader->AddChunk(
            FromProto<TChunkId>(chunkSpec.chunk_id()),
            FromProto<TChunkReplicaWithMediumList>(chunkSpec.replicas()),
            chunkSpec.lower_limit().row_index(),
            upperRecordIndex);
    }
    reader->SetNoMoreChunks();

    return CreateSortReaderAdapter(
        New<TThreadOwningSortReader>(
            std::move(readerQueue),
            std::move(sortThreadPool),
            std::move(reader)),
        std::move(nameTable),
        jobSpecExt.input_row_count());
}

////////////////////////////////////////////////////////////////////////////////

IPartitionerPtr CreatePartitioner(const TPartitionJobSpecExt& partitionJobSpecExt)
{
    if (partitionJobSpecExt.has_wire_partition_lower_bound_prefixes()) {
        TKeySetReader keySetReader(TSharedRef::FromString(partitionJobSpecExt.wire_partition_lower_bound_prefixes()));
        auto keys = keySetReader.GetKeys();
        YT_VERIFY(std::ssize(keys) == partitionJobSpecExt.partition_lower_bound_inclusivenesses_size());

        std::vector<TOwningKeyBound> partitionLowerBounds;
        partitionLowerBounds.reserve(keys.size() + 1);

        partitionLowerBounds.push_back(TOwningKeyBound::MakeUniversal(/*isUpper*/ false));

        for (int index = 0; index < std::ssize(keys); ++index) {
            TUnversionedOwningRow owningKey(keys[index]);
            bool isInclusive = partitionJobSpecExt.partition_lower_bound_inclusivenesses(index);
            partitionLowerBounds.push_back(TOwningKeyBound::FromRow(owningKey, /*isInclusive*/ isInclusive, /*isUpper*/ false));
        }

        auto comparator = GetComparator(FromProto<TSortColumns>(partitionJobSpecExt.sort_columns()));
        // COMPAT(gritukan)
        if (comparator.GetLength() == 0) {
            int keyColumnCount = partitionJobSpecExt.reduce_key_column_count();
            comparator = TComparator(std::vector<ESortOrder>(keyColumnCount, ESortOrder::Ascending));
        }

        return CreateOrderedPartitioner(std::move(partitionLowerBounds), comparator);
    } else {
        return CreateHashPartitioner(
            partitionJobSpecExt.partition_count(),
            partitionJobSpecExt.reduce_key_column_count(),
            partitionJobSpecExt.partition_task_level());
    }
}

////////////////////////////////////////////////////////////////////////////////

constexpr int JobFirstOutputTableFDDefault = 1;
constexpr int JobFirstOutputTableFDWithRedirectStdoutToStderr = 4;

int GetJobFirstOutputTableFDFromSpec(const TUserJobSpec& spec)
{
    return spec.redirect_stdout_to_stderr()
        ? JobFirstOutputTableFDWithRedirectStdoutToStderr
        : JobFirstOutputTableFDDefault;
}

////////////////////////////////////////////////////////////////////////////////

TComparator BuildComparator(
    const TTableSchemaPtr& schema,
    bool enableCodegen)
{
    TCallback<NQueryClient::TUUComparerSignature> cgComparer;
    if (!enableCodegen) {
        YT_TLOG_DEBUG("Using default comparator because codegen is disabled")
            .With("Schema", schema);
    } else if (!schema->IsCGComparatorApplicable()) {
        YT_TLOG_DEBUG("Using default comparator because codegen is not applicable")
            .With("Schema", schema);
    } else {
        YT_TLOG_DEBUG("Using codegen comparator")
            .With("Schema", schema);
        cgComparer = NQueryClient::GenerateStaticTableKeyComparer(schema->GetKeyColumnTypes());
    }
    return schema->ToComparator(std::move(cgComparer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
