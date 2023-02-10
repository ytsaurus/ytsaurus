#include "user_job_io_factory.h"

#include "job_spec_helper.h"
#include "helpers.h"

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/ytlib/table_client/partitioner.h>
#include <yt/yt/ytlib/table_client/partition_sort_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/sorted_merging_reader.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/ytlib/job_proxy/private.h>

#include <limits>
#include <vector>

namespace NYT::NJobProxy {

using namespace NApi;
using namespace NChunkClient;
using namespace NJobTrackerClient;
using namespace NJobTrackerClient::NProto;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

ISchemalessMultiChunkWriterPtr CreateTableWriter(
    NNative::IClientPtr client,
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TString localHostName,
    TChunkListId chunkListId,
    TTransactionId transactionId,
    TTableSchemaPtr tableSchema,
    const TChunkTimestamps& chunkTimestamps,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    const std::optional<TDataSink>& dataSink)
{
    auto nameTable = New<TNameTable>();
    nameTable->SetEnableColumnNameValidation();

    return CreateSchemalessMultiChunkWriter(
        std::move(config),
        std::move(options),
        std::move(nameTable),
        std::move(tableSchema),
        TLegacyOwningKey(),
        std::move(client),
        std::move(localHostName),
        CellTagFromId(chunkListId),
        transactionId,
        dataSink,
        chunkListId,
        chunkTimestamps,
        std::move(trafficMeter),
        std::move(throttler));
}

std::vector<TUnversionedRow> FetchReaderKeyPrefixes(
     ISchemalessMultiChunkReaderPtr reader,
     int keyLength,
     TRowBufferPtr buffer)
{
    std::vector<TUnversionedRow> keys;
    while (true) {
        auto batch = reader->Read();
        if (!batch) {
            break;
        }
        auto rows = batch->MaterializeRows();

        if (rows.empty()) {
            // Reader is not ready, wait.
            reader->GetReadyEvent().Get().ThrowOnError();
        }

        for (auto row : rows) {
            if (!keys.empty() && ComparePrefix(row.begin(), keys.back().begin(), keyLength) == 0) {
                continue;
            }

            auto key = buffer->CaptureRow(row.FirstNElements(keyLength));
            keys.push_back(key);
        }
    }

    return keys;
}

TSharedRange<TUnversionedRow> DedupRows(
    const TSortColumns& sortColumns,
    std::vector<std::vector<TUnversionedRow>> tableKeys)
{
    std::vector<TUnversionedRow> keys;

    ssize_t capacity = 0;
    for (auto key : tableKeys) {
        capacity += std::ssize(key);
    }

    keys.reserve(capacity);

    for (auto key : tableKeys) {
        std::copy(key.begin(), key.end(), back_inserter(keys));
    }

    auto sortComparator = GetComparator(sortColumns);
    std::sort(keys.begin(), keys.end(), [&] (TUnversionedRow lhs, TUnversionedRow rhs) {
        return sortComparator.CompareKeys(TKey::FromRow(lhs), TKey::FromRow(rhs)) < 0;
    });

    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

    return MakeSharedRange<TUnversionedRow>(keys, MakeSharedRange(std::move(tableKeys)));
}

ISchemalessMultiChunkReaderPtr CreateRegularReader(
    const IJobSpecHelperPtr& jobSpecHelper,
    TChunkReaderHostPtr chunkReaderHost,
    bool isParallel,
    TNameTablePtr nameTable,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    const auto& schedulerJobSpecExt = jobSpecHelper->GetSchedulerJobSpecExt();
    std::vector<TDataSliceDescriptor> dataSliceDescriptors;
    for (const auto& inputSpec : schedulerJobSpecExt.input_table_specs()) {
        auto descriptors = UnpackDataSliceDescriptors(inputSpec);
        dataSliceDescriptors.insert(dataSliceDescriptors.end(), descriptors.begin(), descriptors.end());
    }

    auto dataSourceDirectory = jobSpecHelper->GetDataSourceDirectory();

    auto options = ConvertTo<TTableReaderOptionsPtr>(TYsonString(schedulerJobSpecExt.table_reader_options()));

    auto createReader = isParallel
        ? CreateSchemalessParallelMultiReader
        : CreateSchemalessSequentialMultiReader;
    const auto& tableReaderConfig = jobSpecHelper->GetJobIOConfig()->TableReader;
    return createReader(
        tableReaderConfig,
        std::move(options),
        std::move(chunkReaderHost),
        dataSourceDirectory,
        std::move(dataSliceDescriptors),
        /*hintKeyPrefixes*/ nullptr,
        std::move(nameTable),
        chunkReadOptions,
        columnFilter,
        /*partitionTag*/ std::nullopt,
        multiReaderMemoryManager->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize),
        /*interruptDescriptorKeyLength*/ 0);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TUserJobIOFactoryBase
    : public IUserJobIOFactory
{
    TUserJobIOFactoryBase(
        IJobSpecHelperPtr jobSpecHelper,
        const TClientChunkReadOptions& chunkReadOptions,
        TChunkReaderHostPtr chunkReaderHost,
        TString localHostName,
        IThroughputThrottlerPtr outBandwidthThrottler)
        : JobSpecHelper_(std::move(jobSpecHelper))
        , ChunkReadOptions_(chunkReadOptions)
        , ChunkReaderHost_(std::move(chunkReaderHost))
        , LocalHostName_(std::move(localHostName))
        , OutBandwidthThrottler_(std::move(outBandwidthThrottler))
    { }

    void Initialize()
    {
        // Initialize parallel reader memory manager.
        {
            auto totalReaderMemoryLimit = GetTotalReaderMemoryLimit();
            TParallelReaderMemoryManagerOptions parallelReaderMemoryManagerOptions{
                .TotalReservedMemorySize = totalReaderMemoryLimit,
                .MaxInitialReaderReservedMemory = totalReaderMemoryLimit
            };
            MultiReaderMemoryManager_ = CreateParallelReaderMemoryManager(
                parallelReaderMemoryManagerOptions,
                NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
        }
    }

    ISchemalessMultiChunkWriterPtr CreateWriter(
        NApi::NNative::IClientPtr client,
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        TChunkListId chunkListId,
        TTransactionId transactionId,
        TTableSchemaPtr tableSchema,
        const TChunkTimestamps& chunkTimestamps,
        const std::optional<TDataSink>& dataSink) override
    {
        return CreateTableWriter(
            std::move(client),
            std::move(config),
            std::move(options),
            LocalHostName_,
            chunkListId,
            transactionId,
            std::move(tableSchema),
            chunkTimestamps,
            // NB: This is ok, since traffic meter is shared between readers and writers.
            ChunkReaderHost_->TrafficMeter,
            OutBandwidthThrottler_,
            dataSink);
    }

protected:
    const IJobSpecHelperPtr JobSpecHelper_;
    const TClientChunkReadOptions ChunkReadOptions_;
    const TChunkReaderHostPtr ChunkReaderHost_;
    const TString LocalHostName_;
    const IThroughputThrottlerPtr OutBandwidthThrottler_;
    IMultiReaderMemoryManagerPtr MultiReaderMemoryManager_;

    virtual i64 GetTotalReaderMemoryLimit() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TMapJobIOFactory
    : public TUserJobIOFactoryBase
{
public:
    TMapJobIOFactory(
        IJobSpecHelperPtr jobSpecHelper,
        bool useParallelReader,
        const TClientChunkReadOptions& chunkReadOptions,
        TChunkReaderHostPtr chunkReaderHost,
        TString localHostName,
        IThroughputThrottlerPtr outBandwidthThrottler)
        : TUserJobIOFactoryBase(
            std::move(jobSpecHelper),
            chunkReadOptions,
            std::move(chunkReaderHost),
            std::move(localHostName),
            std::move(outBandwidthThrottler))
        , UseParallelReader_(useParallelReader)
    {
        TUserJobIOFactoryBase::Initialize();
    }

    ISchemalessMultiChunkReaderPtr CreateReader(
        TClosure /*onNetworkReleased*/,
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) override
    {
        return CreateRegularReader(
            JobSpecHelper_,
            ChunkReaderHost_,
            UseParallelReader_,
            std::move(nameTable),
            columnFilter,
            ChunkReadOptions_,
            MultiReaderMemoryManager_);
    }

protected:
    i64 GetTotalReaderMemoryLimit() const override
    {
        return JobSpecHelper_->GetJobIOConfig()->TableReader->MaxBufferSize;
    }

private:
    const bool UseParallelReader_;
};

////////////////////////////////////////////////////////////////////////////////

class TSortedReduceJobIOFactory
    : public TUserJobIOFactoryBase
{
public:
    TSortedReduceJobIOFactory(
        IJobSpecHelperPtr jobSpecHelper,
        bool interruptAtKeyEdge,
        const TClientChunkReadOptions& chunkReadOptions,
        TChunkReaderHostPtr chunkReaderHost,
        TString localHostName,
        IThroughputThrottlerPtr outBandwidthThrottler)
        : TUserJobIOFactoryBase(
            std::move(jobSpecHelper),
            chunkReadOptions,
            std::move(chunkReaderHost),
            std::move(localHostName),
            std::move(outBandwidthThrottler))
        , InterruptAtKeyEdge_(interruptAtKeyEdge)
        , Logger(JobProxyClientLogger)
        , Buffer_(New<TRowBuffer>())
    {
        TUserJobIOFactoryBase::Initialize();
    }

    ISchemalessMultiChunkReaderPtr CreateReader(
        TClosure /*onNetworkReleased*/,
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) override
    {
        YT_VERIFY(nameTable->GetSize() == 0 && columnFilter.IsUniversal());

        const auto& reduceJobSpecExt = JobSpecHelper_->GetJobSpec().GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        auto keyColumns = FromProto<TKeyColumns>(reduceJobSpecExt.key_columns());
        auto sortColumns = FromProto<TSortColumns>(reduceJobSpecExt.sort_columns());

        // COMPAT(gritukan)
        if (sortColumns.empty()) {
            for (const auto& keyColumn : keyColumns) {
                sortColumns.push_back({keyColumn, ESortOrder::Ascending});
            }
        }

        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        nameTable = TNameTable::FromSortColumns(sortColumns);
        const auto& schedulerJobSpecExt = JobSpecHelper_->GetSchedulerJobSpecExt();
        auto options = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
            schedulerJobSpecExt.table_reader_options()));

        // We must always enable table index to merge rows with the same index in the proper order.
        options->EnableTableIndex = true;

        // We must always enable key widening to prevent out of range access of key prefixes in sorted merging/joining readers.
        options->EnableKeyWidening = true;

        auto dataSourceDirectory = JobSpecHelper_->GetDataSourceDirectory();

        // COMPAT(max42, onionalex): remove after all CAs are 22.2+.
        for (auto& dataSource : dataSourceDirectory->DataSources()) {
            if (!dataSource.Schema() || dataSource.Schema()->Columns().empty()) {
                dataSource.Schema() = TTableSchema::FromSortColumns(sortColumns);
            }
        }

        // Ff the primary table small, read it out completely into the memory to obtain
        // join keys.
        i64 inputRowCount = 0;
        for (const auto& inputSpec : schedulerJobSpecExt.input_table_specs()) {
            for (const auto& chunkSpec : inputSpec.chunk_specs()) {
                if (chunkSpec.has_row_count_override()) {
                    inputRowCount += chunkSpec.row_count_override();
                    continue;
                }
                if (HasProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions())) {
                    const auto& misc = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());
                    inputRowCount += misc.row_count();
                    continue;
                }
                // No estimate possible.
                inputRowCount = std::numeric_limits<i64>::max();
                break;
            }
        }

        std::vector<std::vector<TUnversionedRow>> primaryKeyPrefixes;
        TSharedRange<TUnversionedRow> hintKeyPrefixes;

        if (reduceJobSpecExt.has_foreign_table_lookup_keys_threshold() &&
            inputRowCount < reduceJobSpecExt.foreign_table_lookup_keys_threshold() &&
            schedulerJobSpecExt.foreign_input_table_specsSize() > 0)
        {
            primaryKeyPrefixes.resize(schedulerJobSpecExt.input_table_specs_size());
            i64 primaryRowCount = 0;
            // TODO(orlovorlov): surface it in `yt get-job` output that a preliminary
            // pass was performed.

            for (int i = 0; i < schedulerJobSpecExt.input_table_specs_size(); i++) {
                const auto& inputSpec = schedulerJobSpecExt.input_table_specs(i);
                auto dataSliceDescriptors = UnpackDataSliceDescriptors(inputSpec);
                const auto& tableReaderConfig = JobSpecHelper_->GetJobIOConfig()->TableReader;
                auto memoryManager = MultiReaderMemoryManager_->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize);

                // TODO(orlovorlov) YT-18240: only read key columns here.
                auto reader = CreateSchemalessSequentialMultiReader(
                    tableReaderConfig,
                    options,
                    ChunkReaderHost_,
                    dataSourceDirectory,
                    std::move(dataSliceDescriptors),
                    /*hintKeyPrefixes*/ nullptr,
                    nameTable,
                    ChunkReadOptions_,
                    columnFilter,
                    /*partitionTag*/ std::nullopt,
                    memoryManager,
                    sortColumns.size());

                primaryKeyPrefixes[i] = FetchReaderKeyPrefixes(reader, reduceJobSpecExt.join_key_column_count(), Buffer_);
                primaryRowCount += std::ssize(primaryKeyPrefixes[i]);
            }
            hintKeyPrefixes = DedupRows(sortColumns, std::move(primaryKeyPrefixes));

            YT_LOG_INFO("Read all keys from primary table in a preliminary pass "
                        "(EstimatedRowCount: %v, ActualRowCount: %v, DedupedRowCount: %v, "
                        "NumForeignTables: %v)",
                        inputRowCount, primaryRowCount, std::ssize(hintKeyPrefixes),
                        schedulerJobSpecExt.foreign_input_table_specsSize());
        }

        for (const auto& inputSpec : schedulerJobSpecExt.input_table_specs()) {
            // ToDo(psushin): validate that input chunks are sorted.
            auto dataSliceDescriptors = UnpackDataSliceDescriptors(inputSpec);
            const auto& tableReaderConfig = JobSpecHelper_->GetJobIOConfig()->TableReader;
            auto memoryManager = MultiReaderMemoryManager_->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize);

            auto reader = CreateSchemalessSequentialMultiReader(
                tableReaderConfig,
                options,
                ChunkReaderHost_,
                dataSourceDirectory,
                std::move(dataSliceDescriptors),
                /*hintKeyPrefixes*/ nullptr,
                nameTable,
                ChunkReadOptions_,
                columnFilter,
                /*partitionTag*/ std::nullopt,
                memoryManager,
                sortColumns.size());

            primaryReaders.emplace_back(reader);
        }

        const auto foreignKeyColumnCount = reduceJobSpecExt.join_key_column_count();
        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;

        for (const auto& inputSpec : schedulerJobSpecExt.foreign_input_table_specs()) {
            auto dataSliceDescriptors = UnpackDataSliceDescriptors(inputSpec);

            const auto& tableReaderConfig = JobSpecHelper_->GetJobIOConfig()->TableReader;
            auto reader = CreateSchemalessSequentialMultiReader(
                tableReaderConfig,
                options,
                ChunkReaderHost_,
                dataSourceDirectory,
                std::move(dataSliceDescriptors),
                hintKeyPrefixes,
                nameTable,
                ChunkReadOptions_,
                columnFilter,
                /*partitionTag*/ std::nullopt,
                MultiReaderMemoryManager_->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize));

            foreignReaders.emplace_back(reader);
        }

        auto sortComparator = GetComparator(sortColumns);
        auto reduceComparator = sortComparator.Trim(reduceJobSpecExt.reduce_key_column_count());
        auto joinComparator = sortComparator.Trim(foreignKeyColumnCount);
        return CreateSortedJoiningReader(
            primaryReaders,
            sortComparator,
            reduceComparator,
            foreignReaders,
            joinComparator,
            InterruptAtKeyEdge_);
    }

protected:
    i64 GetTotalReaderMemoryLimit() const override
    {
        auto readerMemoryLimit = JobSpecHelper_->GetJobIOConfig()->TableReader->MaxBufferSize;
        const auto& schedulerJobSpecExt = JobSpecHelper_->GetSchedulerJobSpecExt();
        auto readerCount = schedulerJobSpecExt.input_table_specs_size() + schedulerJobSpecExt.foreign_input_table_specs_size();
        return readerMemoryLimit * readerCount;
    }

private:
    const bool InterruptAtKeyEdge_;
    NLogging::TLogger Logger;
    const TRowBufferPtr Buffer_;
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionMapJobIOFactory
    : public TUserJobIOFactoryBase
{
public:
    explicit TPartitionMapJobIOFactory(
        IJobSpecHelperPtr jobSpecHelper,
        const TClientChunkReadOptions& chunkReadOptions,
        TChunkReaderHostPtr chunkReaderHost,
        TString localHostName,
        IThroughputThrottlerPtr outBandwidthThrottler)
        : TUserJobIOFactoryBase(
            std::move(jobSpecHelper),
            chunkReadOptions,
            std::move(chunkReaderHost),
            std::move(localHostName),
            std::move(outBandwidthThrottler))
    {
        TUserJobIOFactoryBase::Initialize();
    }

    ISchemalessMultiChunkReaderPtr CreateReader(
        TClosure /*onNetworkReleased*/,
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) override
    {
        const auto& partitionJobSpecExt = JobSpecHelper_->GetJobSpec().GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);

        return CreateRegularReader(
            JobSpecHelper_,
            ChunkReaderHost_,
            /*isParallel*/ !partitionJobSpecExt.use_sequential_reader(),
            std::move(nameTable),
            columnFilter,
            ChunkReadOptions_,
            MultiReaderMemoryManager_);
    }

    ISchemalessMultiChunkWriterPtr CreateWriter(
        NNative::IClientPtr client,
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        TChunkListId chunkListId,
        TTransactionId transactionId,
        TTableSchemaPtr tableSchema,
        const TChunkTimestamps& chunkTimestamps,
        const std::optional<TDataSink>& dataSink) override
    {
        const auto& jobSpec = JobSpecHelper_->GetJobSpec();
        const auto& jobSpecExt = jobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
        auto partitioner = CreatePartitioner(jobSpecExt);

        // We pass partitioning columns through schema but input stream is not sorted.
        options->ValidateSorted = false;

        // TODO(max42): currently ReturnBoundaryKeys are set exactly for the writers
        // that correspond to the map-sink edge. Think more about how this may be done properly.
        if (!options->ReturnBoundaryKeys) {
            auto keyColumns = FromProto<TKeyColumns>(jobSpecExt.sort_key_columns());
            auto sortColumns = FromProto<TSortColumns>(jobSpecExt.sort_columns());
            // COMPAT(gritukan)
            if (sortColumns.empty()) {
                for (const auto& keyColumn : keyColumns) {
                    sortColumns.push_back(TColumnSortSchema{
                        .Name = keyColumn,
                        .SortOrder = ESortOrder::Ascending
                    });
                }
            }

            auto nameTable = TNameTable::FromKeyColumns(keyColumns);
            nameTable->SetEnableColumnNameValidation();
            if (tableSchema->Columns().empty()) {
                tableSchema = TTableSchema::FromSortColumns(sortColumns);
            }

            // This writer is used for partitioning.
            return CreatePartitionMultiChunkWriter(
                std::move(config),
                std::move(options),
                std::move(nameTable),
                std::move(tableSchema),
                std::move(client),
                LocalHostName_,
                CellTagFromId(chunkListId),
                transactionId,
                chunkListId,
                std::move(partitioner),
                dataSink,
                ChunkReaderHost_->TrafficMeter,
                OutBandwidthThrottler_);
        } else {
            // This writer is used for mapper output tables.
            return CreateTableWriter(
                std::move(client),
                std::move(config),
                std::move(options),
                LocalHostName_,
                chunkListId,
                transactionId,
                std::move(tableSchema),
                chunkTimestamps,
                ChunkReaderHost_->TrafficMeter,
                OutBandwidthThrottler_,
                dataSink);
        }
    }

protected:
    i64 GetTotalReaderMemoryLimit() const override
    {
        return JobSpecHelper_->GetJobIOConfig()->TableReader->MaxBufferSize;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionReduceJobIOFactory
    : public TUserJobIOFactoryBase
{
public:
    TPartitionReduceJobIOFactory(
        IJobSpecHelperPtr jobSpecHelper,
        const TClientChunkReadOptions& chunkReadOptions,
        TChunkReaderHostPtr chunkReaderHost,
        TString localHostName,
        IThroughputThrottlerPtr outBandwidthThrottler)
        : TUserJobIOFactoryBase(
            std::move(jobSpecHelper),
            chunkReadOptions,
            std::move(chunkReaderHost),
            std::move(localHostName),
            std::move(outBandwidthThrottler))
    {
        TUserJobIOFactoryBase::Initialize();
    }

    ISchemalessMultiChunkReaderPtr CreateReader(
        TClosure onNetworkReleased,
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) override
    {
        YT_VERIFY(nameTable->GetSize() == 0 && columnFilter.IsUniversal());

        const auto& schedulerJobSpecExt = JobSpecHelper_->GetSchedulerJobSpecExt();

        YT_VERIFY(schedulerJobSpecExt.input_table_specs_size() == 1);

        const auto& inputSpec = schedulerJobSpecExt.input_table_specs(0);
        auto dataSliceDescriptors = UnpackDataSliceDescriptors(inputSpec);
        auto dataSourceDirectory = JobSpecHelper_->GetDataSourceDirectory();

        const auto& reduceJobSpecExt = JobSpecHelper_->GetJobSpec().GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        auto keyColumns = FromProto<TKeyColumns>(reduceJobSpecExt.key_columns());
        auto sortColumns = FromProto<TSortColumns>(reduceJobSpecExt.sort_columns());

        // COMPAT(gritukan)
        if (sortColumns.empty()) {
            for (const auto& keyColumn : keyColumns) {
                sortColumns.push_back({keyColumn, ESortOrder::Ascending});
            }
        }

        nameTable = TNameTable::FromKeyColumns(keyColumns);

        std::optional<int> partitionTag;
        if (schedulerJobSpecExt.has_partition_tag()) {
            partitionTag = schedulerJobSpecExt.partition_tag();
        } else if (reduceJobSpecExt.has_partition_tag()) {
            partitionTag = reduceJobSpecExt.partition_tag();
        }
        YT_VERIFY(partitionTag);

        auto comparator = GetComparator(FromProto<TSortColumns>(reduceJobSpecExt.sort_columns()));

        return CreatePartitionSortReader(
            JobSpecHelper_->GetJobIOConfig()->TableReader,
            ChunkReaderHost_,
            GetComparator(sortColumns),
            nameTable,
            onNetworkReleased,
            dataSourceDirectory,
            std::move(dataSliceDescriptors),
            schedulerJobSpecExt.input_row_count(),
            schedulerJobSpecExt.is_approximate(),
            *partitionTag,
            ChunkReadOptions_,
            MultiReaderMemoryManager_);
    }

protected:
    i64 GetTotalReaderMemoryLimit() const override
    {
        return JobSpecHelper_->GetJobIOConfig()->TableReader->MaxBufferSize;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVanillaJobIOFactory
    : public TUserJobIOFactoryBase
{
public:
    TVanillaJobIOFactory(
        IJobSpecHelperPtr jobSpecHelper,
        const TClientChunkReadOptions& chunkReadOptions,
        TChunkReaderHostPtr chunkReaderHost,
        TString localHostName,
        IThroughputThrottlerPtr outBandwidthThrottler)
        : TUserJobIOFactoryBase(
            std::move(jobSpecHelper),
            chunkReadOptions,
            std::move(chunkReaderHost),
            std::move(localHostName),
            std::move(outBandwidthThrottler))
    { }

    ISchemalessMultiChunkReaderPtr CreateReader(
        TClosure /*onNetworkReleased*/,
        TNameTablePtr /*nameTable*/,
        const TColumnFilter& /*columnFilter*/) override
    {
        return nullptr;
    }

protected:
    i64 GetTotalReaderMemoryLimit() const override
    {
        return 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

IUserJobIOFactoryPtr CreateUserJobIOFactory(
    const IJobSpecHelperPtr& jobSpecHelper,
    const TClientChunkReadOptions& chunkReadOptions,
    TChunkReaderHostPtr chunkReaderHost,
    TString localHostName,
    IThroughputThrottlerPtr outBandwidthThrottler)
{
    const auto jobType = jobSpecHelper->GetJobType();
    switch (jobType) {
        case EJobType::Map:
            return New<TMapJobIOFactory>(
                jobSpecHelper,
                true,
                chunkReadOptions,
                std::move(chunkReaderHost),
                std::move(localHostName),
                std::move(outBandwidthThrottler));

        case EJobType::OrderedMap:
            return New<TMapJobIOFactory>(
                jobSpecHelper,
                false,
                chunkReadOptions,
                std::move(chunkReaderHost),
                std::move(localHostName),
                std::move(outBandwidthThrottler));

        case EJobType::SortedReduce:
            return New<TSortedReduceJobIOFactory>(
                jobSpecHelper,
                true,
                chunkReadOptions,
                std::move(chunkReaderHost),
                std::move(localHostName),
                std::move(outBandwidthThrottler));

        case EJobType::JoinReduce:
            return New<TSortedReduceJobIOFactory>(
                jobSpecHelper,
                false,
                chunkReadOptions,
                std::move(chunkReaderHost),
                std::move(localHostName),
                std::move(outBandwidthThrottler));

        case EJobType::PartitionMap:
            return New<TPartitionMapJobIOFactory>(
                jobSpecHelper,
                chunkReadOptions,
                std::move(chunkReaderHost),
                std::move(localHostName),
                std::move(outBandwidthThrottler));

        // ToDo(psushin): handle separately to form job result differently.
        case EJobType::ReduceCombiner:
        case EJobType::PartitionReduce:
            return New<TPartitionReduceJobIOFactory>(
                jobSpecHelper,
                chunkReadOptions,
                std::move(chunkReaderHost),
                std::move(localHostName),
                std::move(outBandwidthThrottler));

        case EJobType::Vanilla:
            return New<TVanillaJobIOFactory>(
                jobSpecHelper,
                chunkReadOptions,
                std::move(chunkReaderHost),
                std::move(localHostName),
                std::move(outBandwidthThrottler));

        default:
            THROW_ERROR_EXCEPTION(
                "Job has an invalid type %Qlv while a user job is expected",
                jobType);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
