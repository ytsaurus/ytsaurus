#include "remote_dynamic_store_reader.h"
#include "schemaless_chunk_reader.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/query_client/query_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/chunk_client/read_limit.h>
#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/tablet_client/config.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NApi::NNative;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NQueryClient;
using namespace NTabletClient;
using namespace NApi;
using namespace NObjectClient;
using namespace NTabletClient;

using NChunkClient::TLegacyReadLimit;

using NYT::FromProto;
using NYT::ToProto;

using TIdMapping = TCompactVector<int, TypicalColumnCount>;

template <class IReaderPtr>
using TChunkReaderFactory = TCallback<IReaderPtr(
    TChunkSpec,
    TChunkReaderMemoryManagerPtr)>;

template <class IReaderPtr>
using TAsyncChunkReaderFactory = TCallback<TFuture<IReaderPtr>(
    TChunkSpec,
    TChunkReaderMemoryManagerPtr)>;

////////////////////////////////////////////////////////////////////////////////

struct TRemoteDynamicStoreReaderPoolTag
{ };

////////////////////////////////////////////////////////////////////////////////

std::tuple<TTableSchemaPtr, TColumnFilter, TIdMapping> CreateSortedReadParameters(
    const TTableSchemaPtr& schema,
    const TColumnFilter& columnFilter)
{
    TIdMapping idMapping(static_cast<size_t>(schema->GetColumnCount()), -1);

    if (columnFilter.IsUniversal()) {
        for (int index = 0; index < std::ssize(idMapping); ++index) {
             idMapping[index] = index;
        }
        return {
            schema,
            TColumnFilter(),
            idMapping
        };
    } else {
        int nextId = 0;
        TColumnFilter::TIndexes columnFilterIndexes;
        for (int index = 0; index < schema->GetKeyColumnCount(); ++index) {
            idMapping[index] = nextId++;
            columnFilterIndexes.push_back(index);
        }
        for (int index : columnFilter.GetIndexes()) {
            if (index >= schema->GetKeyColumnCount()) {
                idMapping[index] = nextId++;
                columnFilterIndexes.push_back(index);
            }
        }

        TColumnFilter readColumnFilter(std::move(columnFilterIndexes));

        return {
            schema->Filter(readColumnFilter),
            readColumnFilter,
            idMapping
        };
    }
}

std::tuple<TColumnFilter, TIdMapping> CreateOrderedReadParameters(
    const TTableSchemaPtr& schema,
    const std::optional<std::vector<TString>>& columns,
    const TNameTablePtr& nameTable)
{
    // We don't request system columns (tablet_index and row_index) from the node.
    // Instead all system columns are added in PostprocessRow().
    TIdMapping idMapping(
        static_cast<size_t>(schema->GetColumnCount() + OrderedTabletSystemColumnCount),
        -1);

    if (!columns) {
        TColumnFilter::TIndexes columnFilterIndexes;

        for (int index = 0; index < schema->GetColumnCount(); ++index) {
            columnFilterIndexes.push_back(index + OrderedTabletSystemColumnCount);
            const auto& columnName = schema->Columns()[index].Name();
            idMapping[index + OrderedTabletSystemColumnCount] = nameTable->GetIdOrRegisterName(columnName);
        }

        TColumnFilter readColumnFilter(std::move(columnFilterIndexes));
        return {readColumnFilter, idMapping};
    } else {
        TColumnFilter::TIndexes columnFilterIndexes;

        for (const auto& columnName : *columns) {
            auto* column = schema->FindColumn(columnName);
            if (column) {
                int columnIndex = schema->GetColumnIndex(*column);
                columnFilterIndexes.push_back(columnIndex + OrderedTabletSystemColumnCount);
                idMapping[columnIndex + OrderedTabletSystemColumnCount] = nameTable->GetIdOrRegisterName(columnName);
            }
        }

        TColumnFilter readColumnFilter(std::move(columnFilterIndexes));
        return {readColumnFilter, idMapping};
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
class TRemoteDynamicStoreReaderBase
    : public virtual IReaderBase
    , public TTimingReaderBase
{
public:
    TRemoteDynamicStoreReaderBase(
        TChunkSpec chunkSpec,
        TRemoteDynamicStoreReaderConfigPtr config,
        TChunkReaderHostPtr chunkReaderHost,
        const TClientChunkReadOptions& chunkReadOptions)
        : ChunkSpec_(std::move(chunkSpec))
        , Config_(std::move(config))
        , ChunkReaderHost_(std::move(chunkReaderHost))
        , Client_(ChunkReaderHost_->Client)
        , NodeDirectory_(Client_->GetNativeConnection()->GetNodeDirectory())
        , Networks_(Client_->GetNativeConnection()->GetNetworks())
        , Logger(TableClientLogger.WithTag("ReaderId: %v", TGuid::Create()))
    {
        if (chunkReadOptions.ReadSessionId) {
            ReadSessionId_ = chunkReadOptions.ReadSessionId;
        } else {
            ReadSessionId_ = TReadSessionId::Create();
        }

        Logger.AddTag("StoreId: %v", GetObjectIdFromChunkSpec(ChunkSpec_));
        Logger.AddTag("ReadSessionId: %v", ReadSessionId_);

        if (ChunkSpec_.has_row_index_is_absolute() && !ChunkSpec_.row_index_is_absolute()) {
            THROW_ERROR_EXCEPTION("Remote dynamic store reader expects absolute row indices in chunk spec");
        }

        YT_LOG_DEBUG("Created remote dynamic store reader");
    }

    TDataStatistics GetDataStatistics() const override
    {
        TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(1);
        dataStatistics.set_uncompressed_data_size(UncompressedDataSize_);
        dataStatistics.set_compressed_data_size(UncompressedDataSize_);
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        // TODO(ifsmirnov): compression is done at the level of rpc streaming protocol, is it possible
        // to extract statistics from there?
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return FailedChunkIds_;
    }

protected:
    using TRows = TSharedRange<TRow>;
    using IRowBatchPtr = typename TRowBatchTrait<TRow>::IRowBatchPtr;

    const TChunkSpec ChunkSpec_;
    const TRemoteDynamicStoreReaderConfigPtr Config_;
    const TChunkReaderHostPtr ChunkReaderHost_;
    const NNative::IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TNetworkPreferenceList Networks_;

    TTableSchemaPtr Schema_;
    TColumnFilter ColumnFilter_;
    TIdMapping IdMapping_;

    NConcurrency::IAsyncZeroCopyInputStreamPtr InputStream_;
    TFuture<TRows> RowsFuture_;
    int RowIndex_;

    TRows StoredRowset_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;
    std::atomic<i64> UncompressedDataSize_ = 0;
    std::vector<TChunkId> FailedChunkIds_;

    TReadSessionId ReadSessionId_;
    NLogging::TLogger Logger;

    virtual void FillRequestAndLog(TQueryServiceProxy::TReqReadDynamicStorePtr req) = 0;

    virtual TRows DeserializeRows(const TSharedRef& data) = 0;

    virtual TRow PostprocessRow(TRow row)
    {
        return row;
    }

    IRowBatchPtr DoRead(const TRowBatchReadOptions& options)
    {
        YT_VERIFY(options.MaxRowsPerRead > 0);
        std::vector<TRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        auto readyEvent = ReadyEvent();
        if (!readyEvent.IsSet() || !readyEvent.Get().IsOK()) {
            return CreateEmptyRowBatch<TRow>();
        }

        YT_VERIFY(RowsFuture_);

        if (RowsFuture_.IsSet() && RowsFuture_.Get().IsOK()) {
            const auto& loadedRows = RowsFuture_.Get().Value();
            if (loadedRows.Empty()) {
                YT_LOG_DEBUG("Got empty streaming response, closing reader");
                return nullptr;
            }

            StoredRowset_ = loadedRows;

            int readCount = std::min<int>(
                rows.capacity() - rows.size(),
                loadedRows.size() - RowIndex_);

            for (int localRowIndex = 0; localRowIndex < readCount; ++localRowIndex) {
                auto postprocessedRow = PostprocessRow(loadedRows[RowIndex_++]);
                rows.push_back(postprocessedRow);
                ++RowCount_;
                DataWeight_ += GetDataWeight(rows.back());
            }

            if (RowIndex_ == std::ssize(loadedRows)) {
                RequestRows();
            }
        }

        SetReadyEvent(RowsFuture_.AsVoid());
        if (rows.empty()) {
            return CreateEmptyRowBatch<TRow>();
        } else {
            return CreateBatchFromRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
        }
    }

    TFuture<void> DoOpen()
    {
        YT_LOG_DEBUG("Opening remote dynamic store reader");

        auto storeId = GetObjectIdFromChunkSpec(ChunkSpec_);
        auto tabletId = GetTabletIdFromChunkSpec(ChunkSpec_);
        auto cellId = GetCellIdFromChunkSpec(ChunkSpec_);

        try {
            auto replicas = GetReplicasFromChunkSpec(ChunkSpec_);
            if (replicas.empty()) {
                THROW_ERROR_EXCEPTION("No replicas for a dynamic store");
            }

            auto replica = replicas.front();

            const auto* descriptor = NodeDirectory_->FindDescriptor(replica.GetNodeId());
            if (!descriptor) {
                THROW_ERROR_EXCEPTION("No such node %v", replica.GetNodeId());
            }

            auto address = descriptor->FindAddress(Networks_);
            if (!address) {
                THROW_ERROR_EXCEPTION("No such address %v", descriptor->GetDefaultAddress());
            }

            const auto& channelFactory = Client_->GetChannelFactory();
            auto channel = channelFactory->CreateChannel(*address);
            TQueryServiceProxy proxy(channel);

            auto req = proxy.ReadDynamicStore();
            ToProto(req->mutable_store_id(), storeId);
            ToProto(req->mutable_tablet_id(), tabletId);
            ToProto(req->mutable_cell_id(), cellId);
            if (!ColumnFilter_.IsUniversal()) {
                ToProto(req->mutable_column_filter()->mutable_indexes(), ColumnFilter_.GetIndexes());
            }

            ToProto(req->mutable_read_session_id(), ReadSessionId_);
            req->set_max_rows_per_read(Config_->MaxRowsPerServerRead);
            if (Config_->StreamingSubrequestFailureProbability > 0) {
                req->set_failure_probability(Config_->StreamingSubrequestFailureProbability);
            }

            FillRequestAndLog(req);

            req->ClientAttachmentsStreamingParameters().ReadTimeout = Config_->ClientReadTimeout;
            req->ServerAttachmentsStreamingParameters().ReadTimeout = Config_->ServerReadTimeout;
            req->ClientAttachmentsStreamingParameters().WriteTimeout = Config_->ClientWriteTimeout;
            req->ServerAttachmentsStreamingParameters().WriteTimeout = Config_->ServerWriteTimeout;
            req->ServerAttachmentsStreamingParameters().WindowSize = Config_->WindowSize;

            SetReadyEvent(CreateRpcClientInputStream(std::move(req))
                .Apply(BIND([&, this_ = MakeStrong(this)] (const TErrorOr<IAsyncZeroCopyInputStreamPtr>& errorOrStream) {
                    if (errorOrStream.IsOK()) {
                        YT_LOG_DEBUG("Input stream initialized");
                        InputStream_ = errorOrStream.Value();
                        RequestRows();
                        return RowsFuture_.AsVoid();
                    } else {
                        YT_LOG_DEBUG("Failed to initialize input stream");
                        return MakeFuture<void>(errorOrStream);
                    }
                })));
        } catch (const std::exception& ex) {
            FailedChunkIds_.push_back(storeId);
            SetReadyEvent(MakeFuture(TError("Failed to open remote dynamic store reader")
                << ex
                << TErrorAttribute("dynamic_store_id", storeId)
                << TErrorAttribute("tablet_id", tabletId)));
        }

        return ReadyEvent();
    }

private:
    void RequestRows()
    {
        RowIndex_ = 0;
        RowsFuture_ = InputStream_->Read()
            .Apply(BIND([this, weakThis = MakeWeak(this)] (const TSharedRef& data) {
                auto strongThis = weakThis.Lock();
                if (!strongThis) {
                    THROW_ERROR_EXCEPTION(NYT::EErrorCode::Canceled, "Reader abandoned");
                }

                return data
                    ? DeserializeRows(data)
                    : TRows{};
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRemoteSortedDynamicStoreReader
    : public TRemoteDynamicStoreReaderBase<TVersionedRow>
    , public IVersionedReader
{
public:
    TRemoteSortedDynamicStoreReader(
        TChunkSpec chunkSpec,
        TTableSchemaPtr schema,
        TRemoteDynamicStoreReaderConfigPtr config,
        TChunkReaderHostPtr chunkReaderHost,
        const TClientChunkReadOptions& chunkReadOptions,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp)
        : TBase(
            std::move(chunkSpec),
            std::move(config),
            std::move(chunkReaderHost),
            chunkReadOptions)
        , Timestamp_(timestamp)
    {
        std::tie(Schema_, ColumnFilter_, IdMapping_) = CreateSortedReadParameters(schema, columnFilter);
    }

    TFuture<void> Open() override
    {
        return DoOpen();
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return DoRead(options);
    }

private:
    using TBase = TRemoteDynamicStoreReaderBase<TVersionedRow>;

    const TTimestamp Timestamp_;

    void FillRequestAndLog(TQueryServiceProxy::TReqReadDynamicStorePtr req) override
    {
        auto lowerLimit = FromProto<NChunkClient::TLegacyReadLimit>(ChunkSpec_.lower_limit());
        auto upperLimit = FromProto<NChunkClient::TLegacyReadLimit>(ChunkSpec_.upper_limit());
        if (lowerLimit.HasLegacyKey()) {
            ToProto(req->mutable_lower_bound(), lowerLimit.GetLegacyKey());
        }
        if (upperLimit.HasLegacyKey()) {
            ToProto(req->mutable_upper_bound(), upperLimit.GetLegacyKey());
        }

        req->set_timestamp(Timestamp_);

        YT_LOG_DEBUG("Collected remote dynamic store reader parameters (Range: <%v .. %v>, Timestamp: %v, ColumnFilter: %v)",
            lowerLimit,
            upperLimit,
            Timestamp_,
            ColumnFilter_);
    }

    TRows DeserializeRows(const TSharedRef& data) override
    {
        UncompressedDataSize_ += data.Size();
        // Default row buffer.
        auto reader = CreateWireProtocolReader(data);
        auto schemaData = IWireProtocolReader::GetSchemaData(*Schema_);
        return reader->ReadVersionedRowset(schemaData, /*captureValues*/ true, &IdMapping_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRemoteSortedDynamicStoreReader(
    TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    TRemoteDynamicStoreReaderConfigPtr config,
    TChunkReaderHostPtr chunkReaderHost,
    const TClientChunkReadOptions& chunkReadOptions,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp)
{
    return New<TRemoteSortedDynamicStoreReader>(
        std::move(chunkSpec),
        std::move(schema),
        std::move(config),
        std::move(chunkReaderHost),
        chunkReadOptions,
        columnFilter,
        timestamp);
}

////////////////////////////////////////////////////////////////////////////////

class TRemoteOrderedDynamicStoreReader
    : public TRemoteDynamicStoreReaderBase<TUnversionedRow>
    , public ISchemalessChunkReader
{
public:
    TRemoteOrderedDynamicStoreReader(
        TChunkSpec chunkSpec,
        TTableSchemaPtr schema,
        TRemoteDynamicStoreReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        TChunkReaderHostPtr chunkReaderHost,
        const TClientChunkReadOptions& chunkReadOptions,
        TNameTablePtr nameTable,
        const std::optional<std::vector<TString>>& columns)
        : TBase(
            std::move(chunkSpec),
            std::move(config),
            std::move(chunkReaderHost),
            chunkReadOptions)
        , Options_(std::move(options))
        , NameTable_(std::move(nameTable))
    {
        std::tie(ColumnFilter_, IdMapping_) = CreateOrderedReadParameters(
            schema,
            columns,
            NameTable_);

        InitializeSystemColumnIds();

        YT_UNUSED_FUTURE(DoOpen());
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        RowBuffer_->Clear();
        return DoRead(options);
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    i64 GetTableRowIndex() const override
    {
        // NB: Table row index is requested only for the first reader in the sequential
        // group and only after ready event is set. Thus either this value is correctly
        // initialized or the first dynamic store of the table is empty, so zero is correct.
        return CurrentRowIndex_ == -1 ? 0 : CurrentRowIndex_;
    }

    NChunkClient::TInterruptDescriptor GetInterruptDescriptor(
        TRange<NTableClient::TUnversionedRow> /*unreadRows*/) const override
    {
        // TODO(ifsmirnov): interrupts for ordered remote dynamic stores are disabled.
        return {};
    }

    const NChunkClient::TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        // TODO(ifsmirnov, max42): used only in CHYT which does not yet support ordered dynamic tables.
        YT_UNIMPLEMENTED();
    }

private:
    using TBase = TRemoteDynamicStoreReaderBase<TUnversionedRow>;

    std::optional<i64> StartRowIndex_;
    i64 CurrentRowIndex_ = -1;
    const TChunkReaderOptionsPtr Options_;
    const TNameTablePtr NameTable_;

    int TableIndexId_ = -1;
    int RangeIndexId_ = -1;
    int RowIndexId_ = -1;
    int TabletIndexId_ = -1;
    int SystemColumnCount_ = 0;

    int TableIndex_ = -1;
    int RangeIndex_ = -1;
    int TabletIndex_ = -1;

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TRemoteDynamicStoreReaderPoolTag());

    void FillRequestAndLog(TQueryServiceProxy::TReqReadDynamicStorePtr req) override
    {
        auto lowerLimit = FromProto<NChunkClient::TLegacyReadLimit>(ChunkSpec_.lower_limit());
        auto upperLimit = FromProto<NChunkClient::TLegacyReadLimit>(ChunkSpec_.upper_limit());
        if (lowerLimit.HasRowIndex()) {
            req->set_start_row_index(lowerLimit.GetRowIndex());
        }
        if (upperLimit.HasRowIndex()) {
            req->set_end_row_index(upperLimit.GetRowIndex());
        }

        YT_LOG_DEBUG("Collected remote dynamic store reader parameters (Range: <%v .. %v>, ColumnFilter: %v)",
            lowerLimit,
            upperLimit,
            ColumnFilter_);
    }

    TRows DeserializeRows(const TSharedRef& data) override
    {
        UncompressedDataSize_ += data.Size();

        // Default row buffer.
        auto reader = CreateWireProtocolReader(data);

        if (!StartRowIndex_.has_value()) {
            StartRowIndex_ = reader->ReadInt64();
            CurrentRowIndex_ = *StartRowIndex_;
            YT_LOG_DEBUG("Received start row index (StartRowIndex: %v)", StartRowIndex_);
        }

        return reader->ReadUnversionedRowset(/*captureValues*/ true, &IdMapping_);
    }

    TUnversionedRow PostprocessRow(TUnversionedRow row) override
    {
        if (SystemColumnCount_ == 0) {
            ++CurrentRowIndex_;
            return row;
        }

        auto clonedRow = RowBuffer_->AllocateUnversioned(row.GetCount() + SystemColumnCount_);
        // NB: Shallow copy is done here. Actual values are stored in StoredRowset_.
        ::memcpy(clonedRow.Begin(), row.Begin(), sizeof(TUnversionedValue) * row.GetCount());

        auto* end = clonedRow.Begin() + row.GetCount();

        if (Options_->EnableTableIndex) {
            *end++ = MakeUnversionedInt64Value(TableIndex_, TableIndexId_);
        }
        if (Options_->EnableRangeIndex) {
            *end++ = MakeUnversionedInt64Value(RangeIndex_, RangeIndexId_);
        }
        if (Options_->EnableRowIndex) {
            *end++ = MakeUnversionedInt64Value(CurrentRowIndex_, RowIndexId_);
        }
        if (Options_->EnableTabletIndex) {
            *end++ = MakeUnversionedInt64Value(TabletIndex_, TabletIndexId_);
        }

        ++CurrentRowIndex_;

        return clonedRow;
    }

    void InitializeSystemColumnIds()
    {
        if (Options_->EnableTableIndex) {
            TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
            ++SystemColumnCount_;
            TableIndex_ = ChunkSpec_.table_index();
        }
        if (Options_->EnableRangeIndex) {
            RangeIndexId_ = NameTable_->GetIdOrRegisterName(RangeIndexColumnName);
            ++SystemColumnCount_;
            RangeIndex_ = ChunkSpec_.range_index();
        }
        if (Options_->EnableRowIndex) {
            RowIndexId_ = NameTable_->GetIdOrRegisterName(RowIndexColumnName);
            ++SystemColumnCount_;
        }
        if (Options_->EnableTabletIndex) {
            TabletIndexId_ = NameTable_->GetIdOrRegisterName(TabletIndexColumnName);
            ++SystemColumnCount_;
            TabletIndex_ = ChunkSpec_.tablet_index();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateRemoteOrderedDynamicStoreReader(
    TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    TRemoteDynamicStoreReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    TNameTablePtr nameTable,
    TChunkReaderHostPtr chunkReaderHost,
    const TClientChunkReadOptions& chunkReadOptions,
    const std::optional<std::vector<TString>>& columns)
{
    return New<TRemoteOrderedDynamicStoreReader>(
        std::move(chunkSpec),
        std::move(schema),
        std::move(config),
        std::move(options),
        std::move(chunkReaderHost),
        chunkReadOptions,
        std::move(nameTable),
        columns);
}

////////////////////////////////////////////////////////////////////////////////

template <class IReader, class TRow>
class TRetryingRemoteDynamicStoreReaderBase
    : public virtual IReaderBase
    , public TTimingReaderBase
{
public:
    using IReaderPtr = TIntrusivePtr<IReader>;
    using TRows = TSharedRange<TRow>;
    using IRowBatchPtr = typename TRowBatchTrait<TRow>::IRowBatchPtr;

    TRetryingRemoteDynamicStoreReaderBase(
        TChunkSpec chunkSpec,
        TTableSchemaPtr schema,
        TRetryingRemoteDynamicStoreReaderConfigPtr config,
        TChunkReaderHostPtr chunkReaderHost,
        const TClientChunkReadOptions& chunkReadOptions,
        TChunkReaderMemoryManagerPtr readerMemoryManager,
        TAsyncChunkReaderFactory<IReaderPtr> chunkReaderFactory)
        : ChunkSpec_(std::move(chunkSpec))
        , Schema_(std::move(schema))
        , Config_(std::move(config))
        , ChunkReaderHost_(std::move(chunkReaderHost))
        , Client_(ChunkReaderHost_->Client)
        , NodeDirectory_(Client_->GetNativeConnection()->GetNodeDirectory())
        , Networks_(Client_->GetNativeConnection()->GetNetworks())
        , ChunkReadOptions_(chunkReadOptions)
        , ReaderMemoryManager_(std::move(readerMemoryManager))
        , ChunkReaderFactory_(chunkReaderFactory)
        , Logger(TableClientLogger.WithTag("ReaderId: %v", TGuid::Create()))
    {
        if (ChunkReadOptions_.ReadSessionId) {
            ReadSessionId_ = ChunkReadOptions_.ReadSessionId;
        } else {
            ReadSessionId_ = TReadSessionId::Create();
        }

        Logger.AddTag("StoreId: %v", GetObjectIdFromChunkSpec(ChunkSpec_));
        Logger.AddTag("ReadSessionId: %v", ReadSessionId_);

        YT_LOG_DEBUG("Retrying remote dynamic store reader created");
    }

    virtual ~TRetryingRemoteDynamicStoreReaderBase() override
    {
        YT_LOG_DEBUG("Retrying remote dynamic store reader destroyed");
    }

    TFuture<void> DoOpen()
    {
        auto readyEvent = OpenCurrentReader()
            .Apply(BIND(&TRetryingRemoteDynamicStoreReaderBase::DispatchUnderlyingReadyEvent, MakeStrong(this))
                .AsyncVia(GetCurrentInvoker()));
        SetReadyEvent(readyEvent);

        return readyEvent;
    }

    IRowBatchPtr DoRead(const TRowBatchReadOptions& options)
    {
        auto readyEvent = ReadyEvent();
        if (!readyEvent.IsSet() || !readyEvent.Get().IsOK()) {
            return CreateEmptyRowBatch<TRow>();
        }

        auto currentReader = CurrentReader_.Acquire();
        if (PreviousReader_ != currentReader) {
            PreviousReader_ = currentReader;
        }

        if (FlushedToEmptyChunk_) {
            return nullptr;
        }

        auto batch = currentReader->Read(options);

        LastLocateRequestTimestamp_ = {};
        RetryCount_ = 0;

        if (!batch) {
            // End of read.
            return batch;
        }

        if (batch->IsEmpty()) {
            // Rows are not ready or error occurred and sneakily awaits us hiding in underlying ready event.
            SetReadyEvent(currentReader->GetReadyEvent().Apply(
                BIND(&TRetryingRemoteDynamicStoreReaderBase::DispatchUnderlyingReadyEvent, MakeStrong(this))));
            return batch;
        }

        if (!ChunkReaderFallbackOccurred_) {
            UpdateContinuationToken(batch);
        }

        // Rows are definitely here.
        return batch;
    }

    TDataStatistics GetDataStatistics() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto currentReader = CurrentReader_.Acquire();
        auto dataStatistics = currentReader ? currentReader->GetDataStatistics() : TDataStatistics{};
        auto guard = Guard(DataStatisticsLock_);
        CombineDataStatistics(&dataStatistics, AccumulatedDataStatistics_);
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto currentReader = CurrentReader_.Acquire();
        if (ChunkReaderFallbackOccurred_ && currentReader) {
            return currentReader->GetDecompressionStatistics();
        }
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

protected:
    TChunkSpec ChunkSpec_;
    const TTableSchemaPtr Schema_;
    const TRetryingRemoteDynamicStoreReaderConfigPtr Config_;
    const TChunkReaderHostPtr ChunkReaderHost_;
    const NNative::IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TNetworkPreferenceList Networks_;
    const TClientChunkReadOptions ChunkReadOptions_;
    const TChunkReaderMemoryManagerPtr ReaderMemoryManager_;

    TAtomicIntrusivePtr<IReader> CurrentReader_;
    // NB: It is necessary to store failed reader until Read is called for the
    // new one since it may still own some rows that were read but not yet captured.
    IReaderPtr PreviousReader_;
    TAsyncChunkReaderFactory<IReaderPtr> ChunkReaderFactory_;

    // Data statistics of all previous dynamic store readers.
    TDataStatistics AccumulatedDataStatistics_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DataStatisticsLock_);

    bool ChunkReaderFallbackOccurred_ = false;
    bool FlushedToEmptyChunk_ = false;

    int RetryCount_ = 0;
    TInstant LastLocateRequestTimestamp_;

    TReadSessionId ReadSessionId_;
    NLogging::TLogger Logger;

    virtual void UpdateContinuationToken(const IRowBatchPtr& batch) = 0;

    virtual void PatchChunkSpecWithContinuationToken() = 0;

    virtual void DoCreateRemoteDynamicStoreReader() = 0;

    // Necessary since unversioned readers lack Open() method.
    virtual TFuture<void> OpenCurrentReader() = 0;

    bool IsDynamicStoreSpec(const TChunkSpec& chunkSpec)
    {
        auto storeId = GetObjectIdFromChunkSpec(chunkSpec);
        return IsDynamicTabletStoreType(TypeFromId(storeId));
    }

    TFuture<void> DispatchUnderlyingReadyEvent(const TError& error)
    {
        if (error.IsOK()) {
            // Everything is just fine, underlying reader is ready.
            return VoidFuture;
        }

        // Error in underlying ready event should be treated differently depending on
        // whether we are still reading a dynamic store or already reading from a chunk.
        // Former case should lead to one more retry (if retry limit is not exceeded yet),
        // while latter case is fatal.

        if (ChunkReaderFallbackOccurred_) {
            // There is no way we can recover from this.
            return MakeFuture(error);
        }

        // TODO(max42): do not retry if error is not retryable?

        YT_LOG_DEBUG(error, "Remote dynamic store reader failed, retrying "
            "(RetryCount: %v, MaxRetryCount: %v, LastLocateRequestTimestamp: %v)",
            RetryCount_,
            Config_->RetryCount,
            LastLocateRequestTimestamp_);

        return LocateDynamicStore();
    }

    TFuture<void> LocateDynamicStore()
    {
        if (RetryCount_ == Config_->RetryCount) {
            auto storeId = GetObjectIdFromChunkSpec(ChunkSpec_);
            auto tabletId = GetTabletIdFromChunkSpec(ChunkSpec_);
            auto cellId = GetCellIdFromChunkSpec(ChunkSpec_);

            auto error = TError("Too many dynamic store locate retries failed")
                << TErrorAttribute("dynamic_store_id", storeId)
                << TErrorAttribute("tablet_id", tabletId)
                << TErrorAttribute("cell_id", cellId)
                << TErrorAttribute("retry_count", RetryCount_);

            return MakeFuture(error);
        }

        auto locateDynamicStoreAsync = BIND(&TRetryingRemoteDynamicStoreReaderBase::DoLocateDynamicStore, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker());

        auto now = TInstant::Now();
        if (LastLocateRequestTimestamp_ + Config_->LocateRequestBackoffTime > now) {
            return TDelayedExecutor::MakeDelayed(LastLocateRequestTimestamp_ + Config_->LocateRequestBackoffTime - now).Apply(
                locateDynamicStoreAsync);
        } else {
            return locateDynamicStoreAsync.Run();
        }
    }

    TFuture<void> DoLocateDynamicStore()
    {
        LastLocateRequestTimestamp_ = TInstant::Now();
        ++RetryCount_;

        YT_LOG_DEBUG("Locating dynamic store (RetryCount: %v, MaxRetryCount: %v)",
            RetryCount_,
            Config_->RetryCount);

        auto storeId = GetObjectIdFromChunkSpec(ChunkSpec_);

        NRpc::IChannelPtr channel;
        try {
            channel = Client_->GetMasterChannelOrThrow(
                EMasterChannelKind::Follower,
                CellTagFromId(storeId));
        } catch (const std::exception& ex) {
            return MakeFuture(TError("Error communicating with master")
                << ex);
        }

        TChunkServiceProxy proxy(channel);

        auto req = proxy.LocateDynamicStores();
        ToProto(req->add_subrequests(), storeId);
        req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        // Redundant for ordered tables but not much enough to move it out of the base class.
        req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
        req->SetResponseHeavy(true);
        return req->Invoke().Apply(
            BIND(&TRetryingRemoteDynamicStoreReaderBase::OnLocateResponse, MakeStrong(this))
                .AsyncVia(GetCurrentInvoker()));
    }

    TFuture<void> OnLocateResponse(const TChunkServiceProxy::TErrorOrRspLocateDynamicStoresPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Failed to locate dynamic store");
            return LocateDynamicStore();
        }

        const auto& rsp = rspOrError.Value();
        YT_VERIFY(rsp->subresponses_size() == 1);
        auto& subresponse = *rsp->mutable_subresponses(0);

        // Dynamic store is missing.
        if (subresponse.missing()) {
            YT_LOG_DEBUG("Dynamic store located: store is missing");
            return MakeFuture(TError("Dynamic store is missing"));
        }

        {
            auto guard = Guard(DataStatisticsLock_);
            CombineDataStatistics(&AccumulatedDataStatistics_, CurrentReader_.Acquire()->GetDataStatistics());
        }

        // Dynamic store was empty and flushed to no chunk.
        if (!subresponse.has_chunk_spec()) {
            YT_LOG_DEBUG("Dynamic store located: store is flushed to no chunk");
            CurrentReader_.Store(nullptr);
            ChunkReaderFallbackOccurred_ = true;
            FlushedToEmptyChunk_ = true;
            return VoidFuture;
        }

        NodeDirectory_->MergeFrom(rsp->node_directory());

        auto& chunkSpec = *subresponse.mutable_chunk_spec();
        // Dynamic store is not flushed.
        if (IsDynamicStoreSpec(chunkSpec)) {
            auto replicas = GetReplicasFromChunkSpec(chunkSpec);
            if (replicas.empty()) {
                YT_LOG_DEBUG("Dynamic store located: store has no replicas");
                return LocateDynamicStore();
            }

            YT_LOG_DEBUG("Dynamic store located: got new replicas");
            ChunkSpec_.clear_legacy_replicas();
            ChunkSpec_.clear_replicas();
            for (auto replica : replicas) {
                ChunkSpec_.add_legacy_replicas(ToProto<ui32>(replica.ToChunkReplica()));
                ChunkSpec_.add_replicas(ToProto<ui64>(replica));
            }

            PatchChunkSpecWithContinuationToken();

            DoCreateRemoteDynamicStoreReader();

            return OpenCurrentReader().Apply(
                BIND(&TRetryingRemoteDynamicStoreReaderBase::DispatchUnderlyingReadyEvent, MakeStrong(this))
                    .AsyncVia(GetCurrentInvoker()));
        } else {
            if (ChunkSpec_.has_lower_limit()) {
                *chunkSpec.mutable_lower_limit() = ChunkSpec_.lower_limit();
            }
            if (ChunkSpec_.has_upper_limit()) {
                *chunkSpec.mutable_upper_limit() = ChunkSpec_.upper_limit();
            }
            if (ChunkSpec_.has_tablet_index()) {
                chunkSpec.set_tablet_index(ChunkSpec_.tablet_index());
            }
            if (ChunkSpec_.has_range_index()) {
                chunkSpec.set_range_index(ChunkSpec_.range_index());
            }
            if (ChunkSpec_.has_table_index()) {
                chunkSpec.set_table_index(ChunkSpec_.table_index());
            }
            std::swap(ChunkSpec_, chunkSpec);
            PatchChunkSpecWithContinuationToken();

            YT_LOG_DEBUG("Dynamic store located: falling back to chunk reader (ChunkId: %v)",
                GetObjectIdFromChunkSpec(ChunkSpec_));

            return ChunkReaderFactory_(ChunkSpec_, ReaderMemoryManager_)
                .Apply(BIND(&TRetryingRemoteDynamicStoreReaderBase::OnChunkReaderCreated, MakeStrong(this)));
        }
    }

    TFuture<void> OnChunkReaderCreated(const IReaderPtr& reader)
    {
        CurrentReader_.Store(reader);
        ChunkReaderFallbackOccurred_ = true;
        return OpenCurrentReader();
    }


    void CombineDataStatistics(TDataStatistics* statistics, const TDataStatistics& delta) const
    {
        statistics->set_uncompressed_data_size(statistics->uncompressed_data_size() + delta.uncompressed_data_size());
        statistics->set_row_count(statistics->row_count() + delta.row_count());
        statistics->set_data_weight(statistics->data_weight() + delta.data_weight());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRetryingRemoteSortedDynamicStoreReader
    : public TRetryingRemoteDynamicStoreReaderBase<IVersionedReader, TVersionedRow>
    , public IVersionedReader
{
public:
    TRetryingRemoteSortedDynamicStoreReader(
        TChunkSpec chunkSpec,
        TTableSchemaPtr schema,
        TRetryingRemoteDynamicStoreReaderConfigPtr config,
        TChunkReaderHostPtr chunkReaderHost,
        const TColumnFilter& columnFilter,
        const TClientChunkReadOptions& chunkReadOptions,
        TTimestamp timestamp,
        NChunkClient::TChunkReaderMemoryManagerPtr readerMemoryManager,
        TAsyncChunkReaderFactory<IVersionedReaderPtr> chunkReaderFactory)
        : TRetryingRemoteDynamicStoreReaderBase(
            std::move(chunkSpec),
            std::move(schema),
            std::move(config),
            std::move(chunkReaderHost),
            chunkReadOptions,
            std::move(readerMemoryManager),
            std::move(chunkReaderFactory))
        , ColumnFilter_(columnFilter)
        , Timestamp_(timestamp)
    {
        DoCreateRemoteDynamicStoreReader();
    }

    TFuture<void> Open() override
    {
        return DoOpen();
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return DoRead(options);
    }

private:
    TColumnFilter ColumnFilter_;
    TTimestamp Timestamp_;

    TLegacyOwningKey LastKey_;

    void UpdateContinuationToken(const IVersionedRowBatchPtr& batch) override
    {
        if (batch->IsEmpty()) {
            return;
        }

        auto lastKey = batch->MaterializeRows().Back();
        YT_VERIFY(lastKey);
        LastKey_ = TLegacyOwningKey(lastKey.Keys());
    }

    void PatchChunkSpecWithContinuationToken() override
    {
        if (!LastKey_) {
            return;
        }

        TLegacyReadLimit lowerLimit;
        auto lastKeySuccessor = GetKeySuccessor(LastKey_);
        if (ChunkSpec_.has_lower_limit()) {
            FromProto(&lowerLimit, ChunkSpec_.lower_limit());
            if (lowerLimit.HasLegacyKey()) {
                YT_VERIFY(lowerLimit.GetLegacyKey() <= lastKeySuccessor);
            }
        }
        lowerLimit.SetLegacyKey(lastKeySuccessor);
        ToProto(ChunkSpec_.mutable_lower_limit(), lowerLimit);
    }

    void DoCreateRemoteDynamicStoreReader() override
    {
        TClientChunkReadOptions chunkReadOptions{
            .ReadSessionId = ReadSessionId_
        };

        try {
            CurrentReader_.Store(CreateRemoteSortedDynamicStoreReader(
                ChunkSpec_,
                Schema_,
                Config_,
                ChunkReaderHost_,
                chunkReadOptions,
                ColumnFilter_,
                Timestamp_));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error creating remote dynamic store reader")
                << ex;
        }
    }

    TFuture<void> OpenCurrentReader() override
    {
        return CurrentReader_.Acquire()->Open();
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRetryingRemoteSortedDynamicStoreReader(
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    TRetryingRemoteDynamicStoreReaderConfigPtr config,
    TChunkReaderHostPtr chunkReaderHost,
    const TClientChunkReadOptions& chunkReadOptions,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    TChunkReaderMemoryManagerPtr readerMemoryManager,
    TChunkReaderFactory<IVersionedReaderPtr> chunkReaderFactory)
{
    auto asyncChunkReaderFactory = BIND([chunkReaderFactory = std::move(chunkReaderFactory)] (
        TChunkSpec chunkSpec,
        TChunkReaderMemoryManagerPtr readerMemoryManager)
    {
        return MakeFuture(chunkReaderFactory(
            std::move(chunkSpec),
            std::move(readerMemoryManager)));
    });

    return New<TRetryingRemoteSortedDynamicStoreReader>(
        std::move(chunkSpec),
        schema,
        config,
        chunkReaderHost,
        columnFilter,
        chunkReadOptions,
        timestamp,
        std::move(readerMemoryManager),
        asyncChunkReaderFactory);
}

////////////////////////////////////////////////////////////////////////////////

class TRetryingRemoteOrderedDynamicStoreReader
    : public TRetryingRemoteDynamicStoreReaderBase<ISchemalessChunkReader, TUnversionedRow>
    , public ISchemalessChunkReader
{
public:
    TRetryingRemoteOrderedDynamicStoreReader(
        TChunkSpec chunkSpec,
        TTableSchemaPtr schema,
        TRetryingRemoteDynamicStoreReaderConfigPtr config,
        TChunkReaderOptionsPtr chunkReaderOptions,
        TChunkReaderHostPtr chunkReaderHost,
        const TClientChunkReadOptions& chunkReadOptions,
        TNameTablePtr nameTable,
        const std::optional<std::vector<TString>>& columns,
        TChunkReaderMemoryManagerPtr readerMemoryManager,
        TAsyncChunkReaderFactory<ISchemalessChunkReaderPtr> chunkReaderFactory)
        : TRetryingRemoteDynamicStoreReaderBase(
            std::move(chunkSpec),
            std::move(schema),
            std::move(config),
            std::move(chunkReaderHost),
            chunkReadOptions,
            std::move(readerMemoryManager),
            std::move(chunkReaderFactory))
        , ChunkReaderOptions_(std::move(chunkReaderOptions))
        , NameTable_(std::move(nameTable))
        , Columns_(columns)
    {
        if (ChunkSpec_.has_lower_limit()) {
            const auto& lowerLimit = ChunkSpec_.lower_limit();
            if (lowerLimit.has_row_index()) {
                // COMPAT(ifsmirnov)
                if (ChunkSpec_.has_row_index_is_absolute()) {
                    YT_VERIFY(ChunkSpec_.row_index_is_absolute());
                }
                TabletRowIndex_ = lowerLimit.row_index();
            }
        }

        DoCreateRemoteDynamicStoreReader();
        YT_UNUSED_FUTURE(DoOpen());
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return DoRead(options);
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    i64 GetTableRowIndex() const override
    {
        if (auto currentReader = CurrentReader_.Acquire()) {
            return currentReader->GetTableRowIndex();
        }
        YT_VERIFY(FlushedToEmptyChunk_);
        return 0;
    }

    NChunkClient::TInterruptDescriptor GetInterruptDescriptor(
        TRange<NTableClient::TUnversionedRow> /*unreadRows*/) const override
    {
        // XXX(ifsmirnov)
        return {};
    }

    const NChunkClient::TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    TChunkReaderOptionsPtr ChunkReaderOptions_;
    TNameTablePtr NameTable_;
    std::optional<std::vector<TString>> Columns_;

    // Used as continuation token.
    // If null, then limit was not provided in dynamic store spec and dynamic store
    // reader was not opened, so chunk should be read from the beginning.
    // If not null, then either of the above happened and lower limit for the
    // chunk should be patched (converting absolute index to relative).
    std::optional<i64> TabletRowIndex_;

    void UpdateContinuationToken(const IUnversionedRowBatchPtr& /*batch*/) override
    {
        YT_VERIFY(!ChunkReaderFallbackOccurred_);
        TabletRowIndex_ = CurrentReader_.Acquire()->GetTableRowIndex();
    }

    void PatchChunkSpecWithContinuationToken() override
    {
        if (!TabletRowIndex_) {
            return;
        }

        auto lowerLimit = ChunkSpec_.has_lower_limit()
            ? FromProto<TLegacyReadLimit>(ChunkSpec_.lower_limit())
            : TLegacyReadLimit{};

        if (IsDynamicStoreSpec(ChunkSpec_)) {
            lowerLimit.SetRowIndex(*TabletRowIndex_);
            ToProto(ChunkSpec_.mutable_lower_limit(), lowerLimit);
        } else {
            // COMPAT(ifsmirnov)
            if (ChunkSpec_.has_row_index_is_absolute()) {
                YT_VERIFY(ChunkSpec_.row_index_is_absolute());
            }

            auto firstChunkRowIndex = ChunkSpec_.table_row_index();
            lowerLimit.SetRowIndex(std::max<i64>(
                *TabletRowIndex_ - firstChunkRowIndex,
                0));
            ToProto(ChunkSpec_.mutable_lower_limit(), lowerLimit);

            if (ChunkSpec_.has_upper_limit()) {
                auto upperLimit = FromProto<TLegacyReadLimit>(ChunkSpec_.upper_limit());
                YT_VERIFY(upperLimit.HasRowIndex());
                i64 relativeUpperRowIndex = upperLimit.GetRowIndex() - firstChunkRowIndex;
                upperLimit.SetRowIndex(std::min(relativeUpperRowIndex, ChunkSpec_.row_count_override()));
                ToProto(ChunkSpec_.mutable_upper_limit(), upperLimit);
            }
        }
    }

    void DoCreateRemoteDynamicStoreReader() override
    {
        TClientChunkReadOptions chunkReadOptions{
            .ReadSessionId = ReadSessionId_
        };

        CurrentReader_.Store(CreateRemoteOrderedDynamicStoreReader(
            ChunkSpec_,
            Schema_,
            Config_,
            ChunkReaderOptions_,
            NameTable_,
            ChunkReaderHost_,
            chunkReadOptions,
            Columns_));
    }

    TFuture<void> OpenCurrentReader() override
    {
        return CurrentReader_.Acquire()->GetReadyEvent();
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateRetryingRemoteOrderedDynamicStoreReader(
    TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    TRetryingRemoteDynamicStoreReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    TNameTablePtr nameTable,
    TChunkReaderHostPtr chunkReaderHost,
    const TClientChunkReadOptions& chunkReadOptions,
    const std::optional<std::vector<TString>>& columns,
    TChunkReaderMemoryManagerPtr readerMemoryManager,
    TAsyncChunkReaderFactory<ISchemalessChunkReaderPtr> chunkReaderFactory)
{
    return New<TRetryingRemoteOrderedDynamicStoreReader>(
        std::move(chunkSpec),
        std::move(schema),
        std::move(config),
        std::move(options),
        chunkReaderHost,
        chunkReadOptions,
        std::move(nameTable),
        columns,
        std::move(readerMemoryManager),
        std::move(chunkReaderFactory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
