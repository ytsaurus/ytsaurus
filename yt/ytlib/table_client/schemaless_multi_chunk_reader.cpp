#include "schemaless_multi_chunk_reader.h"

#include "cached_versioned_chunk_meta.h"
#include "chunk_reader_base.h"
#include "chunk_state.h"
#include "columnar_chunk_reader_base.h"
#include "config.h"
#include "helpers.h"
#include "overlapping_reader.h"
#include "private.h"
#include "row_merger.h"
#include "row_sampler.h"
#include "schemaless_block_reader.h"
#include "schemaless_multi_chunk_reader.h"
#include "versioned_chunk_reader.h"
#include "remote_dynamic_store_reader.h"

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/table_chunk_format/public.h>
#include <yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/ytlib/table_chunk_format/null_column_reader.h>

#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/multi_reader_manager.h>
#include <yt/ytlib/chunk_client/parallel_reader_memory_manager.h>
#include <yt/ytlib/chunk_client/reader_factory.h>
#include <yt/ytlib/chunk_client/replication_reader.h>

#include <yt/ytlib/tablet_client/helpers.h>

#include <yt/ytlib/query_client/column_evaluator.h>

#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_base.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/versioned_reader.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/numeric_helpers.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;
using namespace NYPath;
using namespace NYTree;
using namespace NRpc;
using namespace NApi;

using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TReadLimit;
using NChunkClient::TReadRange;
using NChunkClient::NProto::TMiscExt;
using NChunkClient::TChunkReaderStatistics;

using NYT::FromProto;
using NYT::TRange;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TColumnarChunkMetaPtr DownloadChunkMeta(
    IChunkReaderPtr chunkReader,
    const TClientBlockReadOptions& blockReadOptions,
    std::optional<int> partitionTag)
{
    // Download chunk meta.
    std::vector<int> extensionTags = {
        TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value,
        TProtoExtensionTag<NProto::TTableSchemaExt>::Value,
        TProtoExtensionTag<NProto::TBlockMetaExt>::Value,
        TProtoExtensionTag<NProto::TColumnMetaExt>::Value,
        TProtoExtensionTag<NProto::TNameTableExt>::Value,
        TProtoExtensionTag<NProto::TKeyColumnsExt>::Value
    };

    auto asyncChunkMeta = chunkReader->GetMeta(
        blockReadOptions,
        partitionTag,
        extensionTags);
    auto chunkMeta = WaitFor(asyncChunkMeta)
        .ValueOrThrow();

    return New<TColumnarChunkMeta>(*chunkMeta);
}

TChunkReaderConfigPtr PatchConfig(TChunkReaderConfigPtr config, i64 memoryEstimate)
{
    if (memoryEstimate > config->WindowSize + config->GroupSize) {
        return config;
    }

    auto newConfig = CloneYsonSerializable(config);
    newConfig->WindowSize = std::max(memoryEstimate / 2, (i64) 1);
    newConfig->GroupSize = std::max(memoryEstimate / 2, (i64) 1);
    return newConfig;
}

std::vector<IReaderFactoryPtr> CreateReaderFactories(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    const TKeyColumns& keyColumns,
    std::optional<int> partitionTag,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    std::vector<IReaderFactoryPtr> factories;
    for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
        const auto& dataSource = dataSourceDirectory->DataSources()[dataSliceDescriptor.GetDataSourceIndex()];

        switch (dataSource.GetType()) {
            case EDataSourceType::UnversionedTable: {
                const auto& chunkSpec = dataSliceDescriptor.GetSingleChunk();

                auto memoryEstimate = GetChunkReaderMemoryEstimate(chunkSpec, config);
                auto createReader = [=] {
                    try {
                        auto remoteReader = CreateRemoteReader(
                            chunkSpec,
                            config,
                            options,
                            client,
                            nodeDirectory,
                            localDescriptor,
                            localNodeId,
                            blockCache,
                            trafficMeter,
                            bandwidthThrottler,
                            rpsThrottler);

                        TReadRange range{
                            chunkSpec.has_lower_limit() ? TReadLimit(chunkSpec.lower_limit()) : TReadLimit(),
                            chunkSpec.has_upper_limit() ? TReadLimit(chunkSpec.upper_limit()) : TReadLimit()
                        };

                        auto asyncChunkMeta = BIND(DownloadChunkMeta, remoteReader, blockReadOptions, partitionTag)
                            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
                            .Run();
                        auto chunkMeta = WaitFor(asyncChunkMeta)
                            .ValueOrThrow();
                        chunkMeta->RenameColumns(dataSource.ColumnRenameDescriptors());

                        auto chunkState = New<TChunkState>(
                            blockCache,
                            chunkSpec,
                            nullptr,
                            NullTimestamp,
                            nullptr,
                            nullptr,
                            nullptr);

                        auto chunkReaderMemoryManager =
                            multiReaderMemoryManager->CreateChunkReaderMemoryManager(memoryEstimate);

                        return CreateSchemalessChunkReader(
                            std::move(chunkState),
                            std::move(chunkMeta),
                            PatchConfig(config, memoryEstimate),
                            options,
                            remoteReader,
                            nameTable,
                            blockReadOptions,
                            keyColumns,
                            dataSource.OmittedInaccessibleColumns(),
                            columnFilter.IsUniversal() ? CreateColumnFilter(dataSource.Columns(), nameTable) : columnFilter,
                            range,
                            partitionTag,
                            chunkReaderMemoryManager);
                    } catch(const std::exception& ex) {
                        THROW_ERROR_EXCEPTION("Error creating chunk reader")
                            << TErrorAttribute("chunk_id", chunkSpec.chunk_id())
                            << ex;
                    }
                };

                auto canCreateReader = [=] {
                    return multiReaderMemoryManager->GetFreeMemorySize() >= memoryEstimate;
                };

                factories.emplace_back(CreateReaderFactory(createReader, canCreateReader, dataSliceDescriptor));
                break;
            }

            case EDataSourceType::VersionedTable: {
                auto memoryEstimate = GetDataSliceDescriptorReaderMemoryEstimate(dataSliceDescriptor, config);
                int dataSourceIndex = dataSliceDescriptor.GetDataSourceIndex();
                const auto& dataSource = dataSourceDirectory->DataSources()[dataSourceIndex];
                auto createReader = [=] {
                    return CreateSchemalessMergingMultiChunkReader(
                        config,
                        options,
                        client,
                        localDescriptor,
                        localNodeId,
                        blockCache,
                        nodeDirectory,
                        dataSourceDirectory,
                        dataSliceDescriptor,
                        nameTable,
                        blockReadOptions,
                        columnFilter.IsUniversal() ? CreateColumnFilter(dataSource.Columns(), nameTable) : columnFilter,
                        trafficMeter,
                        bandwidthThrottler,
                        rpsThrottler);
                };

                auto canCreateReader = [=] {
                    return multiReaderMemoryManager->GetFreeMemorySize() >= memoryEstimate;
                };

                factories.emplace_back(CreateReaderFactory(createReader, canCreateReader, dataSliceDescriptor));
                break;
            }

            default:
                YT_ABORT();
        }
    }

    return factories;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMultiChunkReader
    : public ISchemalessMultiChunkReader
{
public:
    TSchemalessMultiChunkReader(
        IMultiReaderManagerPtr multiReaderManager,
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns,
        const std::vector<TDataSliceDescriptor>& dataSliceDescriptors);

    ~TSchemalessMultiChunkReader();

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

    virtual i64 GetSessionRowIndex() const override;
    virtual i64 GetTotalRowCount() const override;
    virtual i64 GetTableRowIndex() const override;

    virtual const TNameTablePtr& GetNameTable() const override;
    virtual const TKeyColumns& GetKeyColumns() const override;

    virtual void Interrupt() override;

    virtual void SkipCurrentReader() override;

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override;

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override;

    void Open()
    {
        MultiReaderManager_->Open();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return MultiReaderManager_->GetReadyEvent();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return MultiReaderManager_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return MultiReaderManager_->GetDecompressionStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return MultiReaderManager_->IsFetchingCompleted();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return MultiReaderManager_->GetFailedChunkIds();
    }

private:
    IMultiReaderManagerPtr MultiReaderManager_;

    const TNameTablePtr NameTable_;
    const TKeyColumns KeyColumns_;

    ISchemalessChunkReaderPtr CurrentReader_;
    std::atomic<i64> RowIndex_ = {0};
    std::atomic<i64> RowCount_ = {-1};

    TInterruptDescriptor FinishedInterruptDescriptor_;

    std::atomic<bool> Finished_ = {false};

    void OnReaderSwitched();
};

////////////////////////////////////////////////////////////////////////////////

TSchemalessMultiChunkReader::TSchemalessMultiChunkReader(
    IMultiReaderManagerPtr multiReaderManager,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors)
    : MultiReaderManager_(std::move(multiReaderManager))
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , RowCount_(GetCumulativeRowCount(dataSliceDescriptors))
{
    MultiReaderManager_->SubscribeReaderSwitched(BIND(&TSchemalessMultiChunkReader::OnReaderSwitched, MakeWeak(this)));
    if (dataSliceDescriptors.empty()) {
        Finished_ = true;
    }
}

TSchemalessMultiChunkReader::~TSchemalessMultiChunkReader()
{
    const auto& Logger = MultiReaderManager_->GetLogger();
    // Log all statistics. NB:
    YT_LOG_DEBUG("Reader data statistics (DataStatistics: %v)", MultiReaderManager_->GetDataStatistics());
    YT_LOG_DEBUG("Reader decompression codec statistics (CodecStatistics: %v)", MultiReaderManager_->GetDecompressionStatistics());
}

bool TSchemalessMultiChunkReader::Read(std::vector<TUnversionedRow>* rows)
{
    rows->clear();

    if (!MultiReaderManager_->GetReadyEvent().IsSet() || !MultiReaderManager_->GetReadyEvent().Get().IsOK()) {
        return true;
    }

    if (Finished_) {
        RowCount_ = RowIndex_.load();
        return false;
    }

    bool readerFinished = !CurrentReader_->Read(rows);
    if (!rows->empty()) {
        RowIndex_ += rows->size();
        return true;
    }

    if (readerFinished) {
        // This must fill read descriptors with values from finished readers.
        auto interruptDescriptor = CurrentReader_->GetInterruptDescriptor({});
        FinishedInterruptDescriptor_.MergeFrom(std::move(interruptDescriptor));
    }

    if (!MultiReaderManager_->OnEmptyRead(readerFinished)) {
        Finished_ = true;
    }

    return true;
}

void TSchemalessMultiChunkReader::OnReaderSwitched()
{
    CurrentReader_ = dynamic_cast<ISchemalessChunkReader*>(MultiReaderManager_->GetCurrentSession().Reader.Get());
    YT_VERIFY(CurrentReader_);
}

i64 TSchemalessMultiChunkReader::GetTotalRowCount() const
{
    return RowCount_;
}

i64 TSchemalessMultiChunkReader::GetSessionRowIndex() const
{
    return RowIndex_;
}

i64 TSchemalessMultiChunkReader::GetTableRowIndex() const
{
    return CurrentReader_ ? CurrentReader_->GetTableRowIndex() : 0;
}

const TNameTablePtr& TSchemalessMultiChunkReader::GetNameTable() const
{
    return NameTable_;
}

const TKeyColumns& TSchemalessMultiChunkReader::GetKeyColumns() const
{
    return KeyColumns_;
}

void TSchemalessMultiChunkReader::Interrupt()
{
    if (!Finished_.exchange(true)) {
        MultiReaderManager_->Interrupt();
    }
}

void TSchemalessMultiChunkReader::SkipCurrentReader()
{
    if (!MultiReaderManager_->GetReadyEvent().IsSet() || !MultiReaderManager_->GetReadyEvent().Get().IsOK()) {
        return;
    }

    // Pretend that current reader already finished.
    if (!MultiReaderManager_->OnEmptyRead(/* readerFinished */ true)) {
        Finished_ = true;
    }
}

TInterruptDescriptor TSchemalessMultiChunkReader::GetInterruptDescriptor(
    TRange<TUnversionedRow> unreadRows) const
{
    static TRange<TUnversionedRow> emptyRange;
    auto state = MultiReaderManager_->GetUnreadState();

    auto result = FinishedInterruptDescriptor_;
    if (state.CurrentReader) {
        auto chunkReader = dynamic_cast<ISchemalessChunkReader*>(state.CurrentReader.Get());
        YT_VERIFY(chunkReader);
        result.MergeFrom(chunkReader->GetInterruptDescriptor(unreadRows));
    }
    for (const auto& activeReader : state.ActiveReaders) {
        auto chunkReader = dynamic_cast<ISchemalessChunkReader*>(activeReader.Get());
        YT_VERIFY(chunkReader);
        auto interruptDescriptor = chunkReader->GetInterruptDescriptor(emptyRange);
        result.MergeFrom(std::move(interruptDescriptor));
    }
    for (const auto& factory : state.ReaderFactories) {
        result.UnreadDataSliceDescriptors.emplace_back(factory->GetDataSliceDescriptor());
    }
    return result;
}

const TDataSliceDescriptor& TSchemalessMultiChunkReader::GetCurrentReaderDescriptor() const
{
    return CurrentReader_->GetCurrentReaderDescriptor();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSequentialMultiReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    const TKeyColumns& keyColumns,
    std::optional<int> partitionTag,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    if (!multiReaderMemoryManager) {
        multiReaderMemoryManager = CreateParallelReaderMemoryManager(
            TParallelReaderMemoryManagerOptions{
                .TotalReservedMemorySize = config->MaxBufferSize,
                .MaxInitialReaderReservedMemory = config->WindowSize
            },
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }

    auto reader = New<TSchemalessMultiChunkReader>(
        CreateSequentialMultiReaderManager(
            config,
            options,
            CreateReaderFactories(
                config,
                options,
                client,
                localDescriptor,
                localNodeId,
                blockCache,
                nodeDirectory,
                dataSourceDirectory,
                dataSliceDescriptors,
                nameTable,
                blockReadOptions,
                columnFilter,
                keyColumns,
                partitionTag,
                trafficMeter,
                std::move(bandwidthThrottler),
                std::move(rpsThrottler),
                multiReaderMemoryManager),
            std::move(multiReaderMemoryManager)),
        nameTable,
        keyColumns,
        dataSliceDescriptors);

    reader->Open();
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessParallelMultiReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    const TKeyColumns& keyColumns,
    std::optional<int> partitionTag,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    if (!multiReaderMemoryManager) {
        multiReaderMemoryManager = CreateParallelReaderMemoryManager(
            TParallelReaderMemoryManagerOptions{
                .TotalReservedMemorySize = config->MaxBufferSize,
                .MaxInitialReaderReservedMemory = config->WindowSize
            },
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }

    auto reader = New<TSchemalessMultiChunkReader>(
        CreateParallelMultiReaderManager(
            config,
            options,
            CreateReaderFactories(
                config,
                options,
                client,
                localDescriptor,
                localNodeId,
                blockCache,
                nodeDirectory,
                dataSourceDirectory,
                dataSliceDescriptors,
                nameTable,
                blockReadOptions,
                columnFilter,
                keyColumns,
                partitionTag,
                trafficMeter,
                std::move(bandwidthThrottler),
                std::move(rpsThrottler),
                multiReaderMemoryManager),
            std::move(multiReaderMemoryManager)),
        nameTable,
        keyColumns,
        dataSliceDescriptors);

    reader->Open();
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMergingMultiChunkReader
    : public ISchemalessMultiChunkReader
{
public:
    static ISchemalessMultiChunkReaderPtr Create(
        TTableReaderConfigPtr config,
        TTableReaderOptionsPtr options,
        NNative::IClientPtr client,
        const TNodeDescriptor& localDescriptor,
        std::optional<TNodeId> localNodeId,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const TDataSourceDirectoryPtr& dataSourceDirectory,
        const TDataSliceDescriptor& dataSliceDescriptor,
        TNameTablePtr nameTable,
        const TClientBlockReadOptions& blockReadOptions,
        TColumnFilter columnFilter,
        TTrafficMeterPtr trafficMeter,
        IThroughputThrottlerPtr bandwidthThrottler,
        IThroughputThrottlerPtr rpsThrottler);

    virtual TFuture<void> GetReadyEvent() override
    {
        auto promise = NewPromise<void>();
        promise.TrySetFrom(ErrorPromise_.ToFuture());
        promise.TrySetFrom(UnderlyingReader_->GetReadyEvent());
        return promise.ToFuture();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        // ToDo(psushin): every reader must implement this method eventually.
        return std::vector<TChunkId>();
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        rows->clear();
        SchemafulRows_.clear();
        MemoryPool_.Clear();

        if (Interrupting_) {
            return false;
        }

        if (ErrorPromise_.IsSet()) {
            return true;
        }

        SchemafulRows_.reserve(rows->capacity());
        HasMore_ = UnderlyingReader_->Read(&SchemafulRows_);
        if (SchemafulRows_.empty()) {
            return HasMore_;
        }

        LastKey_ = GetKeyPrefix(SchemafulRows_.back(), Schema_.GetKeyColumnCount());

        YT_VERIFY(HasMore_);

        try {
            for (int index = 0; index < SchemafulRows_.size(); ++index) {
                auto schemalessRow = TMutableUnversionedRow::Allocate(&MemoryPool_, SchemaColumnCount_ + SystemColumnCount_);
                auto schemafulRow = SchemafulRows_[index];

                int schemalessValueIndex = 0;
                for (int valueIndex = 0; valueIndex < schemafulRow.GetCount(); ++valueIndex) {
                    const auto& value = schemafulRow[valueIndex];
                    auto id = IdMapping_[value.Id];

                    if (id >= 0) {
                        ValidateDataValue(value);
                        schemalessRow[schemalessValueIndex] = value;
                        schemalessRow[schemalessValueIndex].Id = id;
                        ++schemalessValueIndex;
                    }
                }

                schemalessRow.SetCount(SchemaColumnCount_);

                if (Options_->EnableRangeIndex) {
                    *schemalessRow.End() = MakeUnversionedInt64Value(RangeIndex_, RangeIndexId_);
                    schemalessRow.SetCount(schemalessRow.GetCount() + 1);
                }
                if (Options_->EnableTableIndex) {
                    *schemalessRow.End() = MakeUnversionedInt64Value(TableIndex_, TableIndexId_);
                    schemalessRow.SetCount(schemalessRow.GetCount() + 1);
                }

                rows->push_back(schemalessRow);
            }

            RowIndex_ += rows->size();
        } catch (const std::exception& ex) {
            SchemafulRows_.clear();
            rows->clear();

            ErrorPromise_.Set(ex);
        }

        return true;
    }

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override
    {
        std::vector<TDataSliceDescriptor> unreadDescriptors;
        std::vector<TDataSliceDescriptor> readDescriptors;

        TOwningKey firstUnreadKey;
        if (!unreadRows.Empty()) {
            auto firstSchemafulUnreadRow = SchemafulRows_[SchemafulRows_.size() - unreadRows.Size()];
            firstUnreadKey = GetKeyPrefix(firstSchemafulUnreadRow, Schema_.GetKeyColumnCount());
        } else if (LastKey_) {
            firstUnreadKey = GetKeySuccessor(LastKey_);
        }

        if (!unreadRows.Empty() || HasMore_) {
            unreadDescriptors.emplace_back(DataSliceDescriptor_);
        }
        if (LastKey_) {
            readDescriptors.emplace_back(DataSliceDescriptor_);
        }

        YT_VERIFY(firstUnreadKey || readDescriptors.empty());

        if (firstUnreadKey) {
            // TODO: Estimate row count and data size.
            for (auto& descriptor : unreadDescriptors) {
                for (auto& chunk : descriptor.ChunkSpecs) {
                    ToProto(chunk.mutable_lower_limit()->mutable_key(), firstUnreadKey);
                }
            }
            for (auto& descriptor : readDescriptors) {
                for (auto& chunk : descriptor.ChunkSpecs) {
                    ToProto(chunk.mutable_upper_limit()->mutable_key(), firstUnreadKey);
                }
            }
        }

        return {std::move(unreadDescriptors), std::move(readDescriptors)};
    }

    virtual void Interrupt() override
    {
        Interrupting_ = true;
        ErrorPromise_.TrySet(TError());
    }

    virtual void SkipCurrentReader() override
    {
        // Merging reader doesn't support sub-reader skipping.
    }

    virtual bool IsFetchingCompleted() const override
    {
        return false;
    }

    virtual i64 GetSessionRowIndex() const override
    {
        return RowIndex_;
    }

    virtual i64 GetTotalRowCount() const override
    {
        return RowCount_;
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual const TKeyColumns& GetKeyColumns() const override
    {
        return KeyColumns_;
    }

    virtual i64 GetTableRowIndex() const override
    {
        // Not supported for versioned data.
        return -1;
    }

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_ABORT();
    }

private:
    const TTableReaderOptionsPtr Options_;
    const ISchemafulReaderPtr UnderlyingReader_;
    const TDataSliceDescriptor DataSliceDescriptor_;
    const TTableSchema Schema_;
    const std::vector<int> IdMapping_;
    const TNameTablePtr NameTable_;
    const i64 RowCount_;

    // We keep rows received from underlying schemaful reader
    // to define proper lower limit during interrupt.
    std::vector<TUnversionedRow> SchemafulRows_;

    std::atomic<bool> Interrupting_ = {false};

    // We must assume that there is more data if we read nothing to the moment.
    std::atomic<bool> HasMore_ = {true};
    TOwningKey LastKey_;

    i64 RowIndex_ = 0;

    TChunkedMemoryPool MemoryPool_;

    int TableIndexId_ = -1;
    int RangeIndexId_ = -1;
    int TableIndex_ = -1;
    int RangeIndex_ = -1;
    int SystemColumnCount_ = 0;

    // Number of "active" columns in id mapping.
    int SchemaColumnCount_ = 0;

    // Columns that output row stream is sorted by. May not coincide with schema key columns,
    // because some column may be filtered out by the column filter.
    TKeyColumns KeyColumns_;

    TPromise<void> ErrorPromise_ = NewPromise<void>();

    IMultiReaderMemoryManagerPtr ParallelReaderMemoryManager_;

    TSchemalessMergingMultiChunkReader(
        TTableReaderOptionsPtr options,
        ISchemafulReaderPtr underlyingReader,
        const TDataSliceDescriptor& dataSliceDescriptor,
        TTableSchema schema,
        std::vector<int> idMapping,
        TNameTablePtr nameTable,
        i64 rowCount,
        IMultiReaderMemoryManagerPtr parallelReaderMemoryManager)
        : Options_(options)
        , UnderlyingReader_(std::move(underlyingReader))
        , DataSliceDescriptor_(dataSliceDescriptor)
        , Schema_(std::move(schema))
        , IdMapping_(idMapping)
        , NameTable_(nameTable)
        , RowCount_(rowCount)
        , ParallelReaderMemoryManager_(std::move(parallelReaderMemoryManager))
    {
        if (!DataSliceDescriptor_.ChunkSpecs.empty()) {
            TableIndex_ = DataSliceDescriptor_.ChunkSpecs.front().table_index();
            RangeIndex_ = DataSliceDescriptor_.ChunkSpecs.front().range_index();
        }

        if (Options_->EnableRangeIndex) {
            ++SystemColumnCount_;
            RangeIndexId_ = NameTable_->GetIdOrRegisterName(RangeIndexColumnName);
        }

        if (Options_->EnableTableIndex) {
            ++SystemColumnCount_;
            TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
        }

        for(auto id : IdMapping_) {
            if (id >= 0) {
                ++SchemaColumnCount_;
            }
        }

        for (int index = 0; index < Schema_.GetKeyColumnCount(); ++index) {
            if (IdMapping_[index] < 0) {
                break;
            }

            KeyColumns_.push_back(Schema_.Columns()[index].Name());
        }
    }

    DECLARE_NEW_FRIEND();
};

////////////////////////////////////////////////////////////////////////////////

std::pair<TTableSchema, TColumnFilter> CreateVersionedReadParameters(
    const TTableSchema& schema,
    const TColumnFilter& columnFilter)
{
    if (columnFilter.IsUniversal()) {
        return std::make_pair(schema, columnFilter);
    }

    std::vector<NTableClient::TColumnSchema> columns;
    for (int index = 0; index < schema.GetKeyColumnCount(); ++index) {
        columns.push_back(schema.Columns()[index]);
    }

    TColumnFilter::TIndexes columnFilterIndexes;
    for (int index : columnFilter.GetIndexes()) {
        if (index >= schema.GetKeyColumnCount()) {
            columnFilterIndexes.push_back(columns.size());
            columns.push_back(schema.Columns()[index]);
        } else {
            columnFilterIndexes.push_back(index);
        }
    }

    return std::make_pair(TTableSchema(std::move(columns)), TColumnFilter(std::move(columnFilterIndexes)));
}

ISchemalessMultiChunkReaderPtr TSchemalessMergingMultiChunkReader::Create(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const TDataSliceDescriptor& dataSliceDescriptor,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    TColumnFilter columnFilter,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    auto Logger = TableClientLogger;
    if (blockReadOptions.ReadSessionId) {
        Logger.AddTag("ReadSessionId: %v", blockReadOptions.ReadSessionId);
    }

    const auto& dataSource = dataSourceDirectory->DataSources()[dataSliceDescriptor.GetDataSourceIndex()];
    const auto& chunkSpecs = dataSliceDescriptor.ChunkSpecs;

    YT_VERIFY(dataSource.Schema());
    const auto& tableSchema = *dataSource.Schema();
    auto timestamp = dataSource.GetTimestamp();
    auto retentionTimestamp = dataSource.GetRetentionTimestamp();
    const auto& renameDescriptors = dataSource.ColumnRenameDescriptors();

    if (!columnFilter.IsUniversal()) {
        TColumnFilter::TIndexes transformedIndexes;
        for (auto index : columnFilter.GetIndexes()) {
            if (auto* column = tableSchema.FindColumn(nameTable->GetName(index))) {
                auto columnIndex = tableSchema.GetColumnIndex(*column);
                if (std::find(transformedIndexes.begin(), transformedIndexes.end(), columnIndex) ==
                    transformedIndexes.end())
                {
                    transformedIndexes.push_back(columnIndex);
                }
            }
        }
        columnFilter = TColumnFilter(std::move(transformedIndexes));
    }

    ValidateColumnFilter(columnFilter, tableSchema.GetColumnCount());

    TTableSchema versionedReadSchema;
    TColumnFilter versionedColumnFilter;
    std::tie(versionedReadSchema, versionedColumnFilter) = CreateVersionedReadParameters(
        tableSchema,
        columnFilter);

    std::vector<int> idMapping(versionedReadSchema.GetColumnCount());

    try {
        for (int columnIndex = 0; columnIndex < versionedReadSchema.Columns().size(); ++columnIndex) {
            const auto& column = versionedReadSchema.Columns()[columnIndex];
            if (versionedColumnFilter.ContainsIndex(columnIndex)) {
                idMapping[columnIndex] = nameTable->GetIdOrRegisterName(column.Name());
            } else {
                // We should skip this column in schemaless reading.
                idMapping[columnIndex] = -1;
            }
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to update name table for schemaless merging multi chunk reader")
            << ex;
    }

    std::vector<TOwningKey> boundaries;
    boundaries.reserve(chunkSpecs.size());

    auto extractMinKey = [] (const TChunkSpec& chunkSpec) {
        auto type = TypeFromId(FromProto<TChunkId>(chunkSpec.chunk_id()));

        if (chunkSpec.has_lower_limit()) {
            auto limit = FromProto<TReadLimit>(chunkSpec.lower_limit());
            if (limit.HasKey()) {
                return limit.GetKey();
            }
        } else if (IsChunkTabletStoreType(type)) {
            YT_VERIFY(chunkSpec.has_chunk_meta());
            if (FindProtoExtension<NProto::TBoundaryKeysExt>(chunkSpec.chunk_meta().extensions())) {
                auto boundaryKeysExt = GetProtoExtension<NProto::TBoundaryKeysExt>(chunkSpec.chunk_meta().extensions());
                return FromProto<TOwningKey>(boundaryKeysExt.min());
            }
        }
        return TOwningKey();
    };

    for (const auto& chunkSpec : chunkSpecs) {
        TOwningKey minKey = extractMinKey(chunkSpec);
        boundaries.push_back(minKey);
    }

    YT_LOG_DEBUG("Create overlapping range reader (Boundaries: %v, Stores: %v, ColumnFilter: %v)",
        boundaries,
        MakeFormattableView(chunkSpecs, [] (TStringBuilderBase* builder, const TChunkSpec& chunkSpec) {
            FormatValue(builder, FromProto<TChunkId>(chunkSpec.chunk_id()), TStringBuf());
        }),
        columnFilter);

    auto performanceCounters = New<TChunkReaderPerformanceCounters>();

    auto parallelReaderMemoryManager = CreateParallelReaderMemoryManager(
        TParallelReaderMemoryManagerOptions{
            .TotalReservedMemorySize = config->MaxBufferSize,
            .MaxInitialReaderReservedMemory = config->WindowSize
        },
        NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());

    auto createVersionedChunkReader = [
        config,
        options,
        client,
        localDescriptor,
        localNodeId,
        blockCache,
        nodeDirectory,
        blockReadOptions,
        chunkSpecs,
        tableSchema,
        versionedReadSchema,
        performanceCounters,
        timestamp,
        trafficMeter,
        bandwidthThrottler,
        rpsThrottler,
        renameDescriptors,
        parallelReaderMemoryManager,
        Logger
    ] (TChunkSpec chunkSpec) -> IVersionedReaderPtr {
        auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
        auto replicas = NYT::FromProto<TChunkReplicaList>(chunkSpec.replicas());

        TReadLimit lowerLimit;
        TReadLimit upperLimit;

        if (chunkSpec.has_lower_limit()) {
            lowerLimit = NYT::FromProto<TReadLimit>(chunkSpec.lower_limit());
        } else {
            lowerLimit.SetKey(MinKey());
        }

        if (chunkSpec.has_upper_limit()) {
            upperLimit = NYT::FromProto<TReadLimit>(chunkSpec.upper_limit());
        } else {
            upperLimit.SetKey(MaxKey());
        }

        if (lowerLimit.HasRowIndex() || upperLimit.HasRowIndex()) {
            THROW_ERROR_EXCEPTION("Row index limit is not supported");
        }

        YT_LOG_DEBUG("Create versioned chunk reader (ChunkId: %v, Range: <%v : %v>)",
            chunkId,
            lowerLimit,
            upperLimit);

        auto remoteReader = CreateRemoteReader(
            chunkSpec,
            config,
            options,
            client,
            nodeDirectory,
            localDescriptor,
            localNodeId,
            blockCache,
            trafficMeter,
            bandwidthThrottler,
            rpsThrottler);

        auto asyncChunkMeta = TCachedVersionedChunkMeta::Load(
            remoteReader,
            blockReadOptions,
            versionedReadSchema,
            renameDescriptors,
            nullptr /* memoryTracker */);
        auto chunkMeta = WaitFor(asyncChunkMeta)
            .ValueOrThrow();
        auto chunkState = New<TChunkState>(
            blockCache,
            chunkSpec,
            nullptr,
            chunkSpec.has_override_timestamp() ? chunkSpec.override_timestamp() : NullTimestamp,
            nullptr,
            performanceCounters,
            nullptr);
        auto chunkReaderMemoryManager =
            parallelReaderMemoryManager->CreateChunkReaderMemoryManager(chunkMeta->Misc().uncompressed_data_size());

        return CreateVersionedChunkReader(
            config,
            std::move(remoteReader),
            std::move(chunkState),
            std::move(chunkMeta),
            blockReadOptions,
            lowerLimit.GetKey(),
            upperLimit.GetKey(),
            TColumnFilter(),
            timestamp,
            false,
            chunkReaderMemoryManager);
    };

    auto createVersionedReader = [
        config,
        options,
        client,
        blockCache,
        nodeDirectory,
        blockReadOptions,
        chunkSpecs,
        tableSchema,
        columnFilter,
        performanceCounters,
        timestamp,
        trafficMeter,
        bandwidthThrottler,
        rpsThrottler,
        renameDescriptors,
        Logger,
        createVersionedChunkReader
    ] (int index) -> IVersionedReaderPtr {
        const auto& chunkSpec = chunkSpecs[index];
        auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
        auto type = TypeFromId(chunkId);

        if (type == EObjectType::SortedDynamicTabletStore) {
            return CreateRetryingRemoteDynamicStoreReader(
                chunkSpec,
                tableSchema,
                config->DynamicStoreReader,
                client,
                nodeDirectory,
                trafficMeter,
                bandwidthThrottler,
                rpsThrottler,
                blockReadOptions,
                columnFilter,
                timestamp,
                BIND(createVersionedChunkReader));
        } else {
            return createVersionedChunkReader(chunkSpec);
        }
    };

    struct TSchemalessMergingMultiChunkReaderBufferTag
    { };

    auto rowMerger = std::make_unique<TSchemafulRowMerger>(
        New<TRowBuffer>(TSchemalessMergingMultiChunkReaderBufferTag()),
        versionedReadSchema.Columns().size(),
        versionedReadSchema.GetKeyColumnCount(),
        TColumnFilter(),
        client->GetNativeConnection()->GetColumnEvaluatorCache()->Find(versionedReadSchema),
        retentionTimestamp);

    auto schemafulReader = CreateSchemafulOverlappingRangeReader(
        std::move(boundaries),
        std::move(rowMerger),
        createVersionedReader,
        [] (
            const TUnversionedValue* lhsBegin,
            const TUnversionedValue* lhsEnd,
            const TUnversionedValue* rhsBegin,
            const TUnversionedValue* rhsEnd)
        {
            return CompareRows(lhsBegin, lhsEnd, rhsBegin, rhsEnd);
        });

    i64 rowCount = NChunkClient::GetCumulativeRowCount(chunkSpecs);

    return New<TSchemalessMergingMultiChunkReader>(
        std::move(options),
        std::move(schemafulReader),
        dataSliceDescriptor,
        versionedReadSchema,
        std::move(idMapping),
        std::move(nameTable),
        rowCount,
        std::move(parallelReaderMemoryManager));
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessMergingMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const TDataSliceDescriptor& dataSliceDescriptor,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    NChunkClient::TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    return TSchemalessMergingMultiChunkReader::Create(
        config,
        options,
        client,
        localDescriptor,
        localNodeId,
        blockCache,
        nodeDirectory,
        dataSourceDirectory,
        dataSliceDescriptor,
        nameTable,
        blockReadOptions,
        columnFilter,
        trafficMeter,
        std::move(bandwidthThrottler),
        std::move(rpsThrottler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
