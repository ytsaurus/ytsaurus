#include "schemaless_multi_chunk_reader.h"

#include "cached_versioned_chunk_meta.h"
#include "chunk_reader_base.h"
#include "chunk_state.h"
#include "columnar_chunk_reader_base.h"
#include "config.h"
#include "dictionary_compression_session.h"
#include "granule_filter.h"
#include "helpers.h"
#include "hunks.h"
#include "overlapping_reader.h"
#include "private.h"
#include "row_merger.h"
#include "schemaless_block_reader.h"
#include "schemaless_multi_chunk_reader.h"
#include "table_read_spec.h"
#include "versioned_chunk_reader.h"
#include "remote_dynamic_store_reader.h"

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/table_chunk_format/public.h>
#include <yt/yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/null_column_reader.h>

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/multi_reader_manager.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>
#include <yt/yt/ytlib/chunk_client/reader_factory.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/ytlib/node_tracker_client/node_status_directory.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/numeric_helpers.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTabletClient;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;
using namespace NYPath;
using namespace NYTree;
using namespace NRpc;
using namespace NApi;
using namespace NLogging;

using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TReadRange;
using NChunkClient::NProto::TMiscExt;

using NYT::FromProto;
using NYT::TRange;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TFuture<TColumnarChunkMetaPtr> DownloadChunkMeta(
    IChunkReaderPtr chunkReader,
    const TClientChunkReadOptions& chunkReadOptions,
    std::optional<int> partitionTag)
{
    // Download chunk meta.
    std::vector<int> extensionTags{
        TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value,
        TProtoExtensionTag<NProto::TTableSchemaExt>::Value,
        TProtoExtensionTag<NProto::TDataBlockMetaExt>::Value,
        TProtoExtensionTag<NProto::TColumnMetaExt>::Value,
        TProtoExtensionTag<NProto::TNameTableExt>::Value,
        TProtoExtensionTag<NProto::TKeyColumnsExt>::Value,
        TProtoExtensionTag<NProto::THunkChunkRefsExt>::Value,
        TProtoExtensionTag<NProto::THunkChunkMetasExt>::Value,
    };
    if (chunkReadOptions.GranuleFilter) {
        extensionTags.push_back(TProtoExtensionTag<NProto::TColumnarStatisticsExt>::Value);
    }

    return chunkReader->GetMeta(
        chunkReadOptions,
        partitionTag,
        extensionTags)
        .Apply(BIND([] (const TRefCountedChunkMetaPtr& chunkMeta) {
            return New<TColumnarChunkMeta>(*chunkMeta);
        }));
}

TChunkReaderConfigPtr PatchConfig(TChunkReaderConfigPtr config, i64 memoryEstimate)
{
    if (memoryEstimate > config->WindowSize + config->GroupSize) {
        return config;
    }

    auto newConfig = CloneYsonStruct(config);
    newConfig->WindowSize = std::max(memoryEstimate / 2, (i64) 1);
    newConfig->GroupSize = std::max(memoryEstimate / 2, (i64) 1);
    return newConfig;
}

std::vector<IReaderFactoryPtr> CreateReaderFactories(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    std::optional<THintKeyPrefixes> hintKeyPrefixes,
    TNameTablePtr nameTable,
    const TClientChunkReadOptions& chunkReadOptions,
    const TColumnFilter& columnFilter,
    std::optional<int> partitionTag,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager,
    int interruptDescriptorKeyLength)
{
    auto chunkFragmentReader = CreateChunkFragmentReader(
        config,
        chunkReaderHost->Client,
        CreateTrivialNodeStatusDirectory(),
        /*profiler*/ {},
        /*mediumThrottler*/ GetUnlimitedThrottler(),
        /*throttlerProvider*/ {});

    std::vector<IReaderFactoryPtr> factories;
    for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
        const auto& dataSource = dataSourceDirectory->DataSources()[dataSliceDescriptor.GetDataSourceIndex()];

        auto wrapReader = [=] (ISchemalessChunkReaderPtr chunkReader) {
            auto dictionaryCompressionFactory = CreateSimpleDictionaryCompressionFactory(
                chunkFragmentReader,
                config,
                nameTable,
                chunkReaderHost);
            return CreateHunkDecodingSchemalessChunkReader(
                config,
                std::move(chunkReader),
                chunkFragmentReader,
                std::move(dictionaryCompressionFactory),
                dataSource.Schema(),
                chunkReadOptions);
        };

        switch (dataSource.GetType()) {
            case EDataSourceType::UnversionedTable: {
                const auto& chunkSpec = dataSliceDescriptor.GetSingleChunk();

                // TODO(ifsmirnov): estimate reader memory for dynamic stores.
                auto memoryEstimate = GetChunkReaderMemoryEstimate(chunkSpec, config);

                auto createChunkReaderFromSpecAsync = BIND([=] (
                    const TChunkSpec& chunkSpec,
                    TChunkReaderMemoryManagerHolderPtr chunkReaderMemoryManagerHolder)
                {
                    IChunkReaderPtr remoteReader;
                    try {
                        remoteReader = CreateRemoteReader(
                            chunkSpec,
                            config,
                            options,
                            chunkReaderHost);
                    } catch (const std::exception& ex) {
                        return MakeFuture<ISchemalessChunkReaderPtr>(ex);
                    }

                    auto asyncChunkMeta = DownloadChunkMeta(remoteReader, chunkReadOptions, partitionTag);

                    return asyncChunkMeta.Apply(BIND([=] (const TColumnarChunkMetaPtr& chunkMeta) {
                        TReadRange readRange;

                        auto sortColumns = dataSource.Schema() ? dataSource.Schema()->GetSortColumns() : TSortColumns{};

                        int keyColumnCount = sortColumns.size();
                        if (chunkSpec.has_lower_limit()) {
                            FromProto(&readRange.LowerLimit(), chunkSpec.lower_limit(), /*isUpper*/ false, keyColumnCount);
                        }
                        if (chunkSpec.has_upper_limit()) {
                            FromProto(&readRange.UpperLimit(), chunkSpec.upper_limit(), /*isUpper*/ true, keyColumnCount);
                        }

                        if (chunkReadOptions.GranuleFilter && chunkMeta->ColumnarStatisticsExt()) {
                            auto allColumnStatistics = FromProto<TColumnarStatistics>(*chunkMeta->ColumnarStatisticsExt(), chunkMeta->Misc().row_count());

                            if (chunkReadOptions.GranuleFilter->CanSkip(allColumnStatistics, chunkMeta->ChunkNameTable())) {
                                readRange = TReadRange::MakeEmpty();
                            }
                        }

                        auto chunkState = New<TChunkState>(TChunkState{
                            .BlockCache = chunkReaderHost->BlockCache,
                            .ChunkSpec = chunkSpec,
                            .VirtualValueDirectory = dataSource.GetVirtualValueDirectory(),
                            .TableSchema = dataSource.Schema(),
                            .DataSource = dataSource,
                        });

                        YT_LOG_DEBUG("Create chunk reader (HintCount: %v, ChunkFormat: %v, Sorted: %v)",
                            hintKeyPrefixes ? std::ssize(hintKeyPrefixes->HintPrefixes) : -1,
                            chunkMeta->GetChunkFormat(), chunkMeta->Misc().sorted());

                        if (!hintKeyPrefixes || !chunkMeta->Misc().sorted() ||
                            chunkMeta->GetChunkFormat() != EChunkFormat::TableUnversionedSchemalessHorizontal)
                        {
                            return CreateSchemalessRangeChunkReader(
                                std::move(chunkState),
                                std::move(chunkMeta),
                                PatchConfig(config, memoryEstimate),
                                options,
                                remoteReader,
                                nameTable,
                                chunkReadOptions,
                                sortColumns,
                                dataSource.OmittedInaccessibleColumns(),
                                columnFilter.IsUniversal() ? CreateColumnFilter(dataSource.Columns(), nameTable) : columnFilter,
                                readRange,
                                partitionTag,
                                chunkReaderMemoryManagerHolder
                                    ? chunkReaderMemoryManagerHolder
                                    : multiReaderMemoryManager->CreateChunkReaderMemoryManager(memoryEstimate),
                                dataSliceDescriptor.VirtualRowIndex,
                                interruptDescriptorKeyLength);
                        } else {
                            YT_LOG_DEBUG("Only reading hint prefixes (Count: %v)", hintKeyPrefixes->HintPrefixes.size());
                            return CreateSchemalessKeyRangesChunkReader(
                                std::move(chunkState),
                                std::move(chunkMeta),
                                PatchConfig(config, memoryEstimate),
                                options,
                                remoteReader,
                                nameTable,
                                chunkReadOptions,
                                sortColumns,
                                dataSource.OmittedInaccessibleColumns(),
                                columnFilter.IsUniversal() ? CreateColumnFilter(dataSource.Columns(), nameTable) : columnFilter,
                                hintKeyPrefixes->HintPrefixes,
                                partitionTag,
                                chunkReaderMemoryManagerHolder
                                    ? chunkReaderMemoryManagerHolder
                                    : multiReaderMemoryManager->CreateChunkReaderMemoryManager(memoryEstimate));
                        }
                    }).AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
                });

                std::optional<std::vector<TString>> columnsToRead;
                if (!columnFilter.IsUniversal()) {
                    columnsToRead = std::vector<TString>{};
                    columnsToRead->reserve(columnFilter.GetIndexes().size());
                    for (auto columnIndex : columnFilter.GetIndexes()) {
                        columnsToRead->emplace_back(nameTable->GetName(columnIndex));
                    }
                } else {
                    columnsToRead = dataSource.Columns();
                }

                auto createReader = BIND([=] {
                    auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
                    if (TypeFromId(chunkId) == EObjectType::OrderedDynamicTabletStore) {
                        return MakeFuture<IReaderBasePtr>(CreateRetryingRemoteOrderedDynamicStoreReader(
                            chunkSpec,
                            dataSource.Schema(),
                            config->DynamicStoreReader,
                            options,
                            nameTable,
                            chunkReaderHost,
                            chunkReadOptions,
                            columnsToRead,
                            multiReaderMemoryManager->CreateChunkReaderMemoryManager(
                                DefaultRemoteDynamicStoreReaderMemoryEstimate),
                            createChunkReaderFromSpecAsync));
                    }

                    return createChunkReaderFromSpecAsync(chunkSpec, nullptr).Apply(
                        BIND([=] (const ISchemalessChunkReaderPtr& reader) -> IReaderBasePtr {
                            return wrapReader(reader);
                        })
                    );
                });

                auto canCreateReader = BIND([=] {
                    return multiReaderMemoryManager->GetFreeMemorySize() >= memoryEstimate;
                });

                factories.push_back(CreateReaderFactory(createReader, canCreateReader, dataSliceDescriptor));
                break;
            }

            case EDataSourceType::VersionedTable: {
                auto memoryEstimate = GetDataSliceDescriptorReaderMemoryEstimate(dataSliceDescriptor, config);
                int dataSourceIndex = dataSliceDescriptor.GetDataSourceIndex();
                const auto& dataSource = dataSourceDirectory->DataSources()[dataSourceIndex];
                auto createReader = BIND([=] () -> IReaderBasePtr {
                    return wrapReader(CreateSchemalessMergingMultiChunkReader(
                        config,
                        options,
                        chunkReaderHost,
                        dataSourceDirectory,
                        dataSliceDescriptor,
                        nameTable,
                        chunkReadOptions,
                        columnFilter.IsUniversal() ? CreateColumnFilter(dataSource.Columns(), nameTable) : columnFilter));
                });

                auto canCreateReader = BIND([=] {
                    return multiReaderMemoryManager->GetFreeMemorySize() >= memoryEstimate;
                });

                factories.push_back(CreateReaderFactory(createReader, canCreateReader, dataSliceDescriptor));
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

TReaderInterruptionOptions TReaderInterruptionOptions::InterruptibleWithEmptyKey()
{
    return TReaderInterruptionOptions{true, 0};
}

TReaderInterruptionOptions TReaderInterruptionOptions::InterruptibleWithKeyLength(
    int descriptorKeyLength)
{
    return {true, descriptorKeyLength};
}

TReaderInterruptionOptions TReaderInterruptionOptions::NonInterruptible()
{
    return TReaderInterruptionOptions{false, -1};
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMultiChunkReader
    : public ISchemalessMultiChunkReader
{
public:
    TSchemalessMultiChunkReader(
        IMultiReaderManagerPtr multiReaderManager,
        TNameTablePtr nameTable,
        const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
        bool interruptible);

    ~TSchemalessMultiChunkReader();

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override;

    i64 GetSessionRowIndex() const override;
    i64 GetTotalRowCount() const override;
    i64 GetTableRowIndex() const override;

    const TNameTablePtr& GetNameTable() const override;

    void Interrupt() override;

    void SkipCurrentReader() override;

    TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override;

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override;

    TTimingStatistics GetTimingStatistics() const override
    {
        // We take wait time from multi reader manager as all ready event bookkeeping is delegated to it.
        // Read time is accounted from our own read timer (reacall that multi reader manager deals with chunk readers
        // while Read() is a table reader level methdd).
        auto statistics = MultiReaderManager_->GetTimingStatistics();
        statistics.ReadTime = ReadTimer_.GetElapsedTime();
        statistics.IdleTime -= statistics.ReadTime;
        return statistics;
    }

    TFuture<void> GetReadyEvent() const override
    {
        return MultiReaderManager_->GetReadyEvent();
    }

    TDataStatistics GetDataStatistics() const override
    {
        return MultiReaderManager_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return MultiReaderManager_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return MultiReaderManager_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return MultiReaderManager_->GetFailedChunkIds();
    }

private:
    const IMultiReaderManagerPtr MultiReaderManager_;
    const TNameTablePtr NameTable_;
    bool Interruptible_;

    ISchemalessChunkReaderPtr CurrentReader_;
    std::atomic<i64> RowIndex_ = 0;
    std::atomic<i64> RowCount_ = -1;

    TInterruptDescriptor FinishedInterruptDescriptor_;

    std::atomic<bool> Finished_ = false;

    TWallTimer ReadTimer_ = TWallTimer(false /*active */);

    void OnReaderSwitched();
};

////////////////////////////////////////////////////////////////////////////////

TSchemalessMultiChunkReader::TSchemalessMultiChunkReader(
    IMultiReaderManagerPtr multiReaderManager,
    TNameTablePtr nameTable,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    bool interruptible)
    : MultiReaderManager_(std::move(multiReaderManager))
    , NameTable_(nameTable)
    , Interruptible_(interruptible)
    , RowCount_(GetCumulativeRowCount(dataSliceDescriptors))
{
    if (dataSliceDescriptors.empty()) {
        Finished_ = true;
    }
    MultiReaderManager_->SubscribeReaderSwitched(BIND(&TSchemalessMultiChunkReader::OnReaderSwitched, MakeWeak(this)));
    MultiReaderManager_->Open();
}

TSchemalessMultiChunkReader::~TSchemalessMultiChunkReader()
{
    const auto& Logger = MultiReaderManager_->GetLogger();
    YT_LOG_DEBUG("Multi chunk reader timing statistics (TimingStatistics: %v)", TSchemalessMultiChunkReader::GetTimingStatistics());
}

IUnversionedRowBatchPtr TSchemalessMultiChunkReader::Read(const TRowBatchReadOptions& options)
{
    auto readGuard = TTimerGuard<TWallTimer>(&ReadTimer_);

    if (!MultiReaderManager_->GetReadyEvent().IsSet() || !MultiReaderManager_->GetReadyEvent().Get().IsOK()) {
        return CreateEmptyUnversionedRowBatch();
    }

    if (Finished_) {
        RowCount_ = RowIndex_.load();
        return nullptr;
    }

    auto batch = CurrentReader_->Read(options);
    if (batch && !batch->IsEmpty()) {
        RowIndex_ += batch->GetRowCount();
        return batch;
    }

    if (!batch && Interruptible_) {
        // This must fill read descriptors with values from finished readers.
        auto interruptDescriptor = CurrentReader_->GetInterruptDescriptor({});
        FinishedInterruptDescriptor_.MergeFrom(std::move(interruptDescriptor));
    }

    if (!MultiReaderManager_->OnEmptyRead(!batch)) {
        Finished_ = true;
    }

    return batch ? batch : CreateEmptyUnversionedRowBatch();
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
    if (!MultiReaderManager_->OnEmptyRead(/*readerFinished*/ true)) {
        Finished_ = true;
    }
}

TInterruptDescriptor TSchemalessMultiChunkReader::GetInterruptDescriptor(
    TRange<TUnversionedRow> unreadRows) const
{
    if (!Interruptible_) {
        THROW_ERROR_EXCEPTION("InterruptDescriptor request from a non-interruptible reader");
    }

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
    TChunkReaderHostPtr chunkReaderHost,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    std::optional<THintKeyPrefixes> hintKeyPrefixes,
    TNameTablePtr nameTable,
    const TClientChunkReadOptions& chunkReadOptions,
    TReaderInterruptionOptions interruptionOptions,
    const TColumnFilter& columnFilter,
    std::optional<int> partitionTag,
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

    return New<TSchemalessMultiChunkReader>(
        CreateSequentialMultiReaderManager(
            config,
            options,
            CreateReaderFactories(
                config,
                options,
                std::move(chunkReaderHost),
                dataSourceDirectory,
                dataSliceDescriptors,
                hintKeyPrefixes,
                nameTable,
                chunkReadOptions,
                columnFilter,
                partitionTag,
                multiReaderMemoryManager,
                interruptionOptions.InterruptDescriptorKeyLength),
            multiReaderMemoryManager),
        nameTable,
        dataSliceDescriptors,
        interruptionOptions.IsInterruptible);
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessParallelMultiReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    std::optional<THintKeyPrefixes> hintKeyPrefixes,
    TNameTablePtr nameTable,
    const TClientChunkReadOptions& chunkReadOptions,
    TReaderInterruptionOptions interruptionOptions,
    const TColumnFilter& columnFilter,
    std::optional<int> partitionTag,
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

    return New<TSchemalessMultiChunkReader>(
        CreateParallelMultiReaderManager(
            config,
            options,
            CreateReaderFactories(
                config,
                options,
                std::move(chunkReaderHost),
                dataSourceDirectory,
                dataSliceDescriptors,
                hintKeyPrefixes,
                nameTable,
                chunkReadOptions,
                columnFilter,
                partitionTag,
                multiReaderMemoryManager,
                interruptionOptions.InterruptDescriptorKeyLength),
            multiReaderMemoryManager),
        nameTable,
        dataSliceDescriptors,
        interruptionOptions.IsInterruptible);
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMergingMultiChunkReader
    : public ISchemalessMultiChunkReader
{
public:
    static ISchemalessMultiChunkReaderPtr Create(
        TTableReaderConfigPtr config,
        TTableReaderOptionsPtr options,
        TChunkReaderHostPtr chunkReaderHost,
        const TDataSourceDirectoryPtr& dataSourceDirectory,
        const TDataSliceDescriptor& dataSliceDescriptor,
        TNameTablePtr nameTable,
        const TClientChunkReadOptions& chunkReadOptions,
        TColumnFilter columnFilter,
        IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

    TFuture<void> GetReadyEvent() const override
    {
        return AnySet(
            std::vector{ErrorPromise_.ToFuture(), UnderlyingReader_->GetReadyEvent()},
            TFutureCombinerOptions{.CancelInputOnShortcut = false});
    }

    TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    TTimingStatistics GetTimingStatistics() const override
    {
        // TODO(max42): one should make IReaderBase inherit from ITimingReader in order for this to work.
        // return UnderlyingReader_->GetTimingStatistics();

        return {};
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        // TODO(psushin): every reader must implement this method eventually.
        return {};
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        MemoryPool_.Clear();

        if (Interrupting_) {
            return nullptr;
        }

        if (ErrorPromise_.IsSet()) {
            return CreateEmptyUnversionedRowBatch();
        }

        SchemafulBatch_ = UnderlyingReader_->Read(options);
        if (SchemafulBatch_) {
            SchemafulRows_ = SchemafulBatch_->MaterializeRows();
        }

        if (!SchemafulBatch_) {
            HasMore_ = false;
            return nullptr;
        }

        if (SchemafulBatch_->IsEmpty()) {
            return CreateEmptyUnversionedRowBatch();
        }

        LastKey_ = GetKeyPrefix(SchemafulRows_.Back(), Schema_->GetKeyColumnCount());

        YT_VERIFY(HasMore_);

        std::vector<TUnversionedRow> schemalessRows;
        schemalessRows.reserve(SchemafulRows_.Size());

        try {
            for (auto schemafulRow : SchemafulRows_) {
                auto schemalessRow = TMutableUnversionedRow::Allocate(&MemoryPool_, SchemaColumnCount_ + SystemColumnCount_);

                // TODO(lukyan): Do not remap here. Use proper mapping in schemaful reader.
                int schemalessValueIndex = 0;
                for (int valueIndex = 0; valueIndex < static_cast<int>(schemafulRow.GetCount()); ++valueIndex) {
                    const auto& value = schemafulRow[valueIndex];
                    auto id = IdMapping_[value.Id];

                    if (id >= 0) {
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

                schemalessRows.push_back(schemalessRow);
            }

            RowIndex_ += schemalessRows.size();
        } catch (const std::exception& ex) {
            SchemafulBatch_.Reset();
            SchemafulRows_ = {};
            ErrorPromise_.Set(ex);
            return CreateEmptyUnversionedRowBatch();
        }

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(schemalessRows), MakeStrong(this)));
    }

    TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override
    {
        std::vector<TDataSliceDescriptor> unreadDescriptors;
        std::vector<TDataSliceDescriptor> readDescriptors;

        TLegacyOwningKey firstUnreadKey;
        if (!unreadRows.Empty()) {
            auto firstSchemafulUnreadRow = SchemafulRows_[SchemafulRows_.size() - unreadRows.Size()];
            firstUnreadKey = GetKeyPrefix(firstSchemafulUnreadRow, Schema_->GetKeyColumnCount());
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
                    ToProto(chunk.mutable_lower_limit()->mutable_legacy_key(), firstUnreadKey);
                }
            }
            for (auto& descriptor : readDescriptors) {
                for (auto& chunk : descriptor.ChunkSpecs) {
                    ToProto(chunk.mutable_upper_limit()->mutable_legacy_key(), firstUnreadKey);
                }
            }
        }

        return {std::move(unreadDescriptors), std::move(readDescriptors)};
    }

    void Interrupt() override
    {
        Interrupting_.store(true);
        ErrorPromise_.TrySet();
    }

    void SkipCurrentReader() override
    {
        // Merging reader doesn't support sub-reader skipping.
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    i64 GetSessionRowIndex() const override
    {
        return RowIndex_;
    }

    i64 GetTotalRowCount() const override
    {
        return RowCount_;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    i64 GetTableRowIndex() const override
    {
        // Not supported for versioned data.
        return -1;
    }

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_ABORT();
    }

    ~TSchemalessMergingMultiChunkReader()
    {
        YT_UNUSED_FUTURE(ParallelReaderMemoryManager_->Finalize());
        YT_LOG_DEBUG("Schemaless merging multi chunk reader data statistics (DataStatistics: %v)", TSchemalessMergingMultiChunkReader::GetDataStatistics());
    }

private:
    const TTableReaderOptionsPtr Options_;
    const ISchemafulUnversionedReaderPtr UnderlyingReader_;
    const TDataSliceDescriptor DataSliceDescriptor_;
    const TTableSchemaPtr Schema_;
    const std::vector<int> IdMapping_;
    const TNameTablePtr NameTable_;
    const i64 RowCount_;
    const IMultiReaderMemoryManagerPtr ParallelReaderMemoryManager_;

    // We keep rows received from underlying schemaful reader
    // to define proper lower limit during interrupt.
    IUnversionedRowBatchPtr SchemafulBatch_;
    TRange<TUnversionedRow> SchemafulRows_;

    std::atomic<bool> Interrupting_ = false;

    // We must assume that there is more data if we read nothing to the moment.
    std::atomic<bool> HasMore_ = true;
    TLegacyOwningKey LastKey_;

    i64 RowIndex_ = 0;

    TChunkedMemoryPool MemoryPool_;

    int TableIndexId_ = -1;
    int RangeIndexId_ = -1;
    int TableIndex_ = -1;
    int RangeIndex_ = -1;
    int SystemColumnCount_ = 0;

    // Number of "active" columns in id mapping.
    int SchemaColumnCount_ = 0;

    const TPromise<void> ErrorPromise_ = NewPromise<void>();

    TLogger Logger;

    TSchemalessMergingMultiChunkReader(
        TTableReaderOptionsPtr options,
        ISchemafulUnversionedReaderPtr underlyingReader,
        const TDataSliceDescriptor& dataSliceDescriptor,
        TTableSchemaPtr schema,
        std::vector<int> idMapping,
        TNameTablePtr nameTable,
        i64 rowCount,
        IMultiReaderMemoryManagerPtr parallelReaderMemoryManager,
        TLogger logger)
        : Options_(options)
        , UnderlyingReader_(std::move(underlyingReader))
        , DataSliceDescriptor_(dataSliceDescriptor)
        , Schema_(std::move(schema))
        , IdMapping_(idMapping)
        , NameTable_(nameTable)
        , RowCount_(rowCount)
        , ParallelReaderMemoryManager_(std::move(parallelReaderMemoryManager))
        , Logger(std::move(logger))
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

        for (auto id : IdMapping_) {
            if (id >= 0) {
                ++SchemaColumnCount_;
            }
        }
    }

    DECLARE_NEW_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

std::tuple<TTableSchemaPtr, TColumnFilter> CreateVersionedReadParameters(
    const TTableSchemaPtr& schema,
    const TColumnFilter& columnFilter)
{
    if (columnFilter.IsUniversal()) {
        return std::pair(schema, columnFilter);
    }

    std::vector<NTableClient::TColumnSchema> columns;
    for (int index = 0; index < schema->GetKeyColumnCount(); ++index) {
        columns.push_back(schema->Columns()[index]);
    }

    TColumnFilter::TIndexes columnFilterIndexes;
    for (int index : columnFilter.GetIndexes()) {
        if (index >= schema->GetKeyColumnCount()) {
            columnFilterIndexes.push_back(columns.size());
            columns.push_back(schema->Columns()[index]);
        } else {
            columnFilterIndexes.push_back(index);
        }
    }

    return std::tuple(
        New<TTableSchema>(std::move(columns)),
        TColumnFilter(std::move(columnFilterIndexes)));
}

ISchemalessMultiChunkReaderPtr TSchemalessMergingMultiChunkReader::Create(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const TDataSliceDescriptor& dataSliceDescriptor,
    TNameTablePtr nameTable,
    const TClientChunkReadOptions& chunkReadOptions,
    TColumnFilter columnFilter,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    if (config->SamplingRate && config->SamplingMode == ESamplingMode::Block) {
        THROW_ERROR_EXCEPTION("Block sampling is not yet supported for sorted dynamic tables");
    }

    auto Logger = TableClientLogger;
    if (chunkReadOptions.ReadSessionId) {
        Logger.AddTag("ReadSessionId: %v", chunkReadOptions.ReadSessionId);
    }

    const auto& dataSource = dataSourceDirectory->DataSources()[dataSliceDescriptor.GetDataSourceIndex()];
    const auto& chunkSpecs = dataSliceDescriptor.ChunkSpecs;

    auto tableSchema = dataSource.Schema();
    YT_VERIFY(tableSchema);
    auto timestamp = dataSource.GetTimestamp();
    auto retentionTimestamp = dataSource.GetRetentionTimestamp();
    const auto& renameDescriptors = dataSource.ColumnRenameDescriptors();

    THashSet<TStringBuf> omittedInaccessibleColumnSet(
        dataSource.OmittedInaccessibleColumns().begin(),
        dataSource.OmittedInaccessibleColumns().end());

    if (!columnFilter.IsUniversal()) {
        TColumnFilter::TIndexes transformedIndexes;
        for (auto index : columnFilter.GetIndexes()) {
            if (const auto* column = tableSchema->FindColumn(nameTable->GetName(index))) {
                auto columnIndex = tableSchema->GetColumnIndex(*column);
                if (std::find(transformedIndexes.begin(), transformedIndexes.end(), columnIndex) ==
                    transformedIndexes.end())
                {
                    transformedIndexes.push_back(columnIndex);
                }
            }
        }
        columnFilter = TColumnFilter(std::move(transformedIndexes));
    }

    ValidateColumnFilter(columnFilter, tableSchema->GetColumnCount());

    auto [versionedReadSchema, versionedColumnFilter] = CreateVersionedReadParameters(
        tableSchema,
        columnFilter);

    std::vector<int> idMapping(versionedReadSchema->GetColumnCount());

    try {
        for (int columnIndex = 0; columnIndex < std::ssize(versionedReadSchema->Columns()); ++columnIndex) {
            const auto& column = versionedReadSchema->Columns()[columnIndex];
            if (versionedColumnFilter.ContainsIndex(columnIndex) && !omittedInaccessibleColumnSet.contains(column.Name())) {
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

    std::vector<TLegacyOwningKey> boundaries;
    boundaries.reserve(chunkSpecs.size());

    auto extractMinKey = [] (const TChunkSpec& chunkSpec) {
        auto type = TypeFromId(FromProto<TChunkId>(chunkSpec.chunk_id()));

        if (chunkSpec.has_lower_limit()) {
            auto limit = FromProto<TLegacyReadLimit>(chunkSpec.lower_limit());
            if (limit.HasLegacyKey()) {
                return limit.GetLegacyKey();
            }
        } else if (IsChunkTabletStoreType(type)) {
            YT_VERIFY(chunkSpec.has_chunk_meta());
            if (FindProtoExtension<NProto::TBoundaryKeysExt>(chunkSpec.chunk_meta().extensions())) {
                auto boundaryKeysExt = GetProtoExtension<NProto::TBoundaryKeysExt>(chunkSpec.chunk_meta().extensions());
                return FromProto<TLegacyOwningKey>(boundaryKeysExt.min());
            }
        }
        return TLegacyOwningKey();
    };

    for (const auto& chunkSpec : chunkSpecs) {
        TLegacyOwningKey minKey = extractMinKey(chunkSpec);
        boundaries.push_back(minKey);
    }

    YT_LOG_DEBUG("Create overlapping range reader (Boundaries: %v, Stores: %v, ColumnFilter: %v)",
        boundaries,
        MakeFormattableView(chunkSpecs, [] (TStringBuilderBase* builder, const TChunkSpec& chunkSpec) {
            FormatValue(builder, FromProto<TChunkId>(chunkSpec.chunk_id()), TStringBuf());
        }),
        columnFilter);

    if (!multiReaderMemoryManager) {
        multiReaderMemoryManager = CreateParallelReaderMemoryManager(
            TParallelReaderMemoryManagerOptions{
                .TotalReservedMemorySize = config->MaxBufferSize,
                .MaxInitialReaderReservedMemory = config->WindowSize
            },
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }

    auto createVersionedChunkReader = [
        config,
        options,
        chunkReaderHost,
        chunkReadOptions,
        chunkSpecs,
        tableSchema,
        versionedReadSchema = versionedReadSchema,
        timestamp,
        renameDescriptors,
        multiReaderMemoryManager,
        dataSource,
        Logger
    ] (
        const TChunkSpec& chunkSpec,
        const TChunkReaderMemoryManagerHolderPtr& chunkReaderMemoryManagerHolder) -> IVersionedReaderPtr
    {
        auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
        auto replicas = GetReplicasFromChunkSpec(chunkSpec);

        TLegacyReadLimit lowerLimit;
        TLegacyReadLimit upperLimit;

        if (chunkSpec.has_lower_limit()) {
            lowerLimit = NYT::FromProto<TLegacyReadLimit>(chunkSpec.lower_limit());
        }
        if (!lowerLimit.HasLegacyKey() || !lowerLimit.GetLegacyKey()) {
            lowerLimit.SetLegacyKey(MinKey());
        }

        if (chunkSpec.has_upper_limit()) {
            upperLimit = NYT::FromProto<TLegacyReadLimit>(chunkSpec.upper_limit());
        }
        if (!upperLimit.HasLegacyKey() || !upperLimit.GetLegacyKey()) {
            upperLimit.SetLegacyKey(MaxKey());
        }

        if (lowerLimit.HasRowIndex() || upperLimit.HasRowIndex()) {
            THROW_ERROR_EXCEPTION("Row index limit is not supported");
        }

        YT_LOG_DEBUG("Creating versioned chunk reader (ChunkId: %v, Range: <%v : %v>)",
            chunkId,
            lowerLimit,
            upperLimit);

        auto remoteReader = CreateRemoteReader(
            chunkSpec,
            config,
            options,
            chunkReaderHost);

        auto asyncVersionedChunkMeta = remoteReader->GetMeta(chunkReadOptions)
            .Apply(BIND(
                &TCachedVersionedChunkMeta::Create,
                /*prepareColumnarMeta*/ false,
                /*memoryTracker*/ nullptr));

        auto versionedChunkMeta = WaitFor(asyncVersionedChunkMeta)
            .ValueOrThrow();

        auto chunkState = New<TChunkState>(TChunkState{
            .BlockCache = chunkReaderHost->BlockCache,
            .ChunkSpec = chunkSpec,
            .ChunkMeta = versionedChunkMeta,
            .OverrideTimestamp = chunkSpec.has_override_timestamp() ? chunkSpec.override_timestamp() : NullTimestamp,
            .TableSchema = versionedReadSchema,
            .DataSource = dataSource,
        });

        auto effectiveTimestamp = timestamp;
        if (chunkSpec.has_max_clip_timestamp()) {
            YT_ASSERT(chunkSpec.max_clip_timestamp() != NullTimestamp);
            effectiveTimestamp = std::min(effectiveTimestamp, chunkSpec.max_clip_timestamp());
        }

        return CreateVersionedChunkReader(
            config,
            std::move(remoteReader),
            std::move(chunkState),
            std::move(versionedChunkMeta),
            chunkReadOptions,
            lowerLimit.GetLegacyKey(),
            upperLimit.GetLegacyKey(),
            TColumnFilter(),
            effectiveTimestamp,
            false,
            chunkReaderMemoryManagerHolder
                ? chunkReaderMemoryManagerHolder
                : multiReaderMemoryManager->CreateChunkReaderMemoryManager(
                    versionedChunkMeta->Misc().uncompressed_data_size()));
    };

    auto createVersionedReader = [
        config,
        options,
        chunkReaderHost,
        chunkReadOptions,
        chunkSpecs,
        tableSchema,
        columnFilter,
        timestamp,
        multiReaderMemoryManager,
        createVersionedChunkReader,
        Logger
    ] (int index) -> IVersionedReaderPtr {
        const auto& chunkSpec = chunkSpecs[index];
        auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
        auto type = TypeFromId(chunkId);

        if (type == EObjectType::SortedDynamicTabletStore) {
            return CreateRetryingRemoteSortedDynamicStoreReader(
                chunkSpec,
                tableSchema,
                config->DynamicStoreReader,
                chunkReaderHost,
                chunkReadOptions,
                columnFilter,
                timestamp,
                multiReaderMemoryManager->CreateChunkReaderMemoryManager(
                    DefaultRemoteDynamicStoreReaderMemoryEstimate),
                BIND(createVersionedChunkReader));
        } else {
            return createVersionedChunkReader(chunkSpec, nullptr);
        }
    };

    struct TSchemalessMergingMultiChunkReaderBufferTag
    { };

    const auto& connection = chunkReaderHost->Client->GetNativeConnection();
    auto rowMerger = std::make_unique<TSchemafulRowMerger>(
        New<TRowBuffer>(TSchemalessMergingMultiChunkReaderBufferTag()),
        versionedReadSchema->GetColumnCount(),
        versionedReadSchema->GetKeyColumnCount(),
        TColumnFilter(),
        connection->GetColumnEvaluatorCache()->Find(versionedReadSchema),
        retentionTimestamp);

    auto schemafulReader = CreateSchemafulOverlappingRangeReader(
        std::move(boundaries),
        std::move(rowMerger),
        createVersionedReader,
        CompareValueRanges);

    i64 rowCount = NChunkClient::GetCumulativeRowCount(chunkSpecs);

    return New<TSchemalessMergingMultiChunkReader>(
        std::move(options),
        std::move(schemafulReader),
        dataSliceDescriptor,
        versionedReadSchema,
        std::move(idMapping),
        std::move(nameTable),
        rowCount,
        std::move(multiReaderMemoryManager),
        Logger);
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessMergingMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const TDataSliceDescriptor& dataSliceDescriptor,
    TNameTablePtr nameTable,
    const TClientChunkReadOptions& chunkReadOptions,
    const TColumnFilter& columnFilter,
    IMultiReaderMemoryManagerPtr readerMemoryManager)
{
    return TSchemalessMergingMultiChunkReader::Create(
        config,
        options,
        std::move(chunkReaderHost),
        dataSourceDirectory,
        dataSliceDescriptor,
        nameTable,
        chunkReadOptions,
        columnFilter,
        std::move(readerMemoryManager));
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateAppropriateSchemalessMultiChunkReader(
    const TTableReaderOptionsPtr& options,
    const TTableReaderConfigPtr& config,
    TChunkReaderHostPtr chunkReaderHost,
    const TTableReadSpec& tableReadSpec,
    const TClientChunkReadOptions& chunkReadOptions,
    bool unordered,
    const TNameTablePtr& nameTable,
    const TColumnFilter& columnFilter)
{
    const auto& dataSourceDirectory = tableReadSpec.DataSourceDirectory;
    auto& dataSliceDescriptors = tableReadSpec.DataSliceDescriptors;

    // TODO(max42): think about mixing different data sources here.
    // TODO(max42): what about reading several tables together?
    YT_VERIFY(dataSourceDirectory->DataSources().size() == 1);
    const auto& dataSource = dataSourceDirectory->DataSources().front();

    switch (dataSourceDirectory->GetCommonTypeOrThrow()) {
        case EDataSourceType::VersionedTable: {
            YT_VERIFY(dataSliceDescriptors.size() == 1);
            const auto& dataSliceDescriptor = dataSliceDescriptors.front();

            auto adjustedColumnFilter = columnFilter.IsUniversal()
                ? CreateColumnFilter(dataSource.Columns(), nameTable)
                : columnFilter;

            auto reader = CreateSchemalessMergingMultiChunkReader(
                config,
                options,
                chunkReaderHost,
                dataSourceDirectory,
                dataSliceDescriptor,
                nameTable,
                chunkReadOptions,
                adjustedColumnFilter);

            auto chunkFragmentReader = CreateChunkFragmentReader(
                config,
                chunkReaderHost->Client,
                CreateTrivialNodeStatusDirectory(),
                /*profiler*/ {},
                /*mediumThrottler*/ GetUnlimitedThrottler(),
                /*throttlerProvider*/ {});
            auto dictionaryCompressionFactory = CreateSimpleDictionaryCompressionFactory(
                chunkFragmentReader,
                config,
                nameTable,
                chunkReaderHost);

            return CreateHunkDecodingSchemalessMultiChunkReader(
                config,
                std::move(reader),
                std::move(chunkFragmentReader),
                std::move(dictionaryCompressionFactory),
                dataSource.Schema(),
                chunkReadOptions);
        }

        case EDataSourceType::UnversionedTable: {
            auto factory = unordered
                ? CreateSchemalessParallelMultiReader
                : CreateSchemalessSequentialMultiReader;
            return factory(
                config,
                options,
                chunkReaderHost,
                dataSourceDirectory,
                std::move(dataSliceDescriptors),
                /*hintKeyPrefixes*/ std::nullopt,
                nameTable,
                chunkReadOptions,
                TReaderInterruptionOptions::InterruptibleWithEmptyKey(),
                columnFilter,
                /*partitionTag*/ std::nullopt,
                /*multiReaderMemoryManager*/ nullptr);
        }
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
