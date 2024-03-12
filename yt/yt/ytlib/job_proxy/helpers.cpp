#include "helpers.h"

#include "private.h"

#include <yt/yt/library/query/base/query.h>

#include <yt/yt/library/query/engine_api/evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/query_client/functions_cache.h>

#include <yt/yt/ytlib/table_client/key_set.h>
#include <yt/yt/ytlib/table_client/partitioner.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NControllerAgent::NProto;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger& Logger = JobProxyClientLogger;

////////////////////////////////////////////////////////////////////////////////

void RunQuery(
    const TQuerySpec& querySpec,
    const TSchemalessReaderFactory& readerFactory,
    const TSchemalessWriterFactory& writerFactory,
    const std::optional<TString>& udfDirectory)
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

    YT_LOG_INFO("Reading, evaluating query and writing");
    evaluator->Run(
        query,
        reader,
        writer,
        nullptr,
        functionGenerators,
        aggregateGenerators,
        GetDefaultMemoryChunkProvider(),
        TQueryBaseOptions());
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TDataSliceDescriptor> UnpackDataSliceDescriptors(const TTableInputSpec& inputTableSpec)
{
    return FromProto<std::vector<TDataSliceDescriptor>>(
        inputTableSpec.chunk_specs(),
        inputTableSpec.chunk_spec_count_per_data_slice(),
        inputTableSpec.virtual_row_index_per_data_slice());
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
    } else if (partitionJobSpecExt.has_wire_partition_keys()) {
        auto wirePartitionKeys = TSharedRef::FromString(partitionJobSpecExt.wire_partition_keys());

        auto comparator = GetComparator(FromProto<TSortColumns>(partitionJobSpecExt.sort_columns()));
        // COMPAT(gritukan)
        if (comparator.GetLength() == 0) {
            int keyColumnCount = partitionJobSpecExt.reduce_key_column_count();
            comparator = TComparator(std::vector<ESortOrder>(keyColumnCount, ESortOrder::Ascending));
        }

        return CreateOrderedPartitioner(wirePartitionKeys, comparator);
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

} // namespace NYT::NJobProxy
