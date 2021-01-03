#include "remote_dynamic_store_reader.h"
#include "schemaless_chunk_reader.h"
#include "private.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/query_client/query_service_proxy.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/unversioned_row_batch.h>
#include <yt/client/table_client/versioned_reader.h>
#include <yt/client/table_client/wire_protocol.h>

#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/tablet_client/config.h>

#include <yt/core/misc/protobuf_helpers.h>

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

using NChunkClient::TLegacyReadLimit;

using NYT::FromProto;
using TIdMapping = SmallVector<int, TypicalColumnCount>;

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
        for (int index = 0; index < idMapping.size(); ++index) {
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

template<typename TRow>
class TRemoteDynamicStoreReaderBase
    : public virtual IReaderBase
{
public:
    TRemoteDynamicStoreReaderBase(
        TChunkSpec chunkSpec,
        TRemoteDynamicStoreReaderConfigPtr config,
        NNative::IClientPtr client,
        TNodeDirectoryPtr nodeDirectory,
        const TClientBlockReadOptions& blockReadOptions)
        : ChunkSpec_(std::move(chunkSpec))
        , Config_(std::move(config))
        , Client_(std::move(client))
        , NodeDirectory_(std::move(nodeDirectory))
        , Networks_(Client_->GetNativeConnection()->GetNetworks())
        , Logger(TableClientLogger.WithTag("ReaderId: %v", TGuid::Create()))
    {
        if (blockReadOptions.ReadSessionId) {
            ReadSessionId_ = blockReadOptions.ReadSessionId;
        } else {
            ReadSessionId_ = TReadSessionId::Create();
        }

        Logger.AddTag("StoreId: %v", FromProto<TDynamicStoreId>(ChunkSpec_.chunk_id()));
        Logger.AddTag("ReadSessionId: %v", ReadSessionId_);

        if (ChunkSpec_.has_row_index_is_absolute() && !ChunkSpec_.row_index_is_absolute()) {
            THROW_ERROR_EXCEPTION("Remote dynamic store reader expects absolute row indices in chunk spec");
        }

        YT_LOG_DEBUG("Created remote dynamic store reader");
    }

    virtual TFuture<void> GetReadyEvent() const
    {
        return ReadyEvent_;
    }

    virtual TDataStatistics GetDataStatistics() const
    {
        TDataStatistics dataStatistics;

        dataStatistics.set_chunk_count(1);
        dataStatistics.set_uncompressed_data_size(UncompressedDataSize_);
        dataStatistics.set_compressed_data_size(UncompressedDataSize_);

        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    virtual TCodecStatistics GetDecompressionStatistics() const
    {
        // TODO(ifsmirnov): compression is done at the level of rpc streaming protocol, is it possible
        // to extract statistics from there?
        return {};
    }

    virtual bool IsFetchingCompleted() const
    {
        return false;
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const
    {
        return FailedChunkIds_;
    }

protected:
    using TRows = TSharedRange<TRow>;

    const TChunkSpec ChunkSpec_;
    const TRemoteDynamicStoreReaderConfigPtr Config_;
    const NNative::IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TNetworkPreferenceList Networks_;

    TTableSchemaPtr Schema_;
    TColumnFilter ColumnFilter_;
    TIdMapping IdMapping_;

    TPromise<void> ReadyEvent_ = NewPromise<void>();
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

    bool DoRead(std::vector<TRow>* rows)
    {
        rows->clear();
        YT_VERIFY(rows->capacity() > 0);

        YT_VERIFY(ReadyEvent_);

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        ReadyEvent_ = NewPromise<void>();

        YT_VERIFY(RowsFuture_);

        if (RowsFuture_.IsSet() && RowsFuture_.Get().IsOK()) {
            const auto& loadedRows = RowsFuture_.Get().Value();
            if (loadedRows.Empty()) {
                YT_LOG_DEBUG("Got empty streaming response, closing reader");
                return false;
            }

            StoredRowset_ = loadedRows;

            int readCount = std::min<int>(
                rows->capacity() - rows->size(),
                loadedRows.size() - RowIndex_);

            for (int localRowIndex = 0; localRowIndex < readCount; ++localRowIndex) {
                auto postprocessedRow = PostprocessRow(loadedRows[RowIndex_++]);
                rows->push_back(postprocessedRow);
                ++RowCount_;
                DataWeight_ += GetDataWeight(rows->back());
            }

            if (RowIndex_ == loadedRows.size()) {
                RequestRows();
            }
        }

        ReadyEvent_.SetFrom(RowsFuture_);
        return true;
    }

    void DoOpen()
    {
        YT_LOG_DEBUG("Opening remote dynamic store reader");

        auto storeId = FromProto<TStoreId>(ChunkSpec_.chunk_id());
        auto tabletId = FromProto<TTabletId>(ChunkSpec_.tablet_id());

        try {
            if (ChunkSpec_.replicas_size() == 0) {
                THROW_ERROR_EXCEPTION("No replicas for a dynamic store");
            }

            auto replica = FromProto<TChunkReplicaWithMedium>(ChunkSpec_.replicas(0));

            const auto* descriptor = NodeDirectory_->FindDescriptor(replica.GetNodeId());
            if (!descriptor) {
                THROW_ERROR_EXCEPTION("No such node %v", replica.GetNodeId());
            }

            auto address = descriptor->FindAddress(Networks_);
            if (!address) {
                THROW_ERROR_EXCEPTION("No such address %v", descriptor->GetDefaultAddress());
            }

            auto addressWithNetwork = descriptor->GetAddressWithNetworkOrThrow(Networks_);
            const auto& channelFactory = Client_->GetChannelFactory();
            auto channel = channelFactory->CreateChannel(addressWithNetwork);
            TQueryServiceProxy proxy(channel);

            auto req = proxy.ReadDynamicStore();
            ToProto(req->mutable_tablet_id(), tabletId);
            ToProto(req->mutable_store_id(), storeId);
            if (!ColumnFilter_.IsUniversal()) {
                ToProto(req->mutable_column_filter()->mutable_indexes(), ColumnFilter_.GetIndexes());
            }

            ToProto(req->mutable_read_session_id(), ReadSessionId_);

            FillRequestAndLog(req);

            req->ClientAttachmentsStreamingParameters().ReadTimeout = Config_->ClientReadTimeout;
            req->ServerAttachmentsStreamingParameters().ReadTimeout = Config_->ServerReadTimeout;
            req->ClientAttachmentsStreamingParameters().WriteTimeout = Config_->ClientWriteTimeout;
            req->ServerAttachmentsStreamingParameters().WriteTimeout = Config_->ServerWriteTimeout;
            req->ServerAttachmentsStreamingParameters().WindowSize = Config_->WindowSize;

            ReadyEvent_ = NewPromise<void>();
            CreateRpcClientInputStream(std::move(req))
                .Apply(BIND([&, this_ = MakeStrong(this)] (const TErrorOr<IAsyncZeroCopyInputStreamPtr>& errorOrStream) {
                    if (errorOrStream.IsOK()) {
                        YT_LOG_DEBUG("Input stream initialized");
                        InputStream_ = errorOrStream.Value();
                        RequestRows();
                        ReadyEvent_.SetFrom(RowsFuture_);
                    } else {
                        YT_LOG_DEBUG("Failed to initialize input stream");
                        ReadyEvent_.Set(errorOrStream);
                    }
                }));
        } catch (const std::exception& ex) {
            FailedChunkIds_.push_back(storeId);
            THROW_ERROR_EXCEPTION("Failed to open remote dynamic store reader")
                << ex
                << TErrorAttribute("dynamic_store_id", storeId)
                << TErrorAttribute("tablet_id", tabletId);
        }
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

class TRemoteSortedDynamicStoreReader
    : public TRemoteDynamicStoreReaderBase<TVersionedRow>
    , public IVersionedReader
{
public:
    TRemoteSortedDynamicStoreReader(
        TChunkSpec chunkSpec,
        TTableSchemaPtr schema,
        TRemoteDynamicStoreReaderConfigPtr config,
        NNative::IClientPtr client,
        TNodeDirectoryPtr nodeDirectory,
        const TClientBlockReadOptions& blockReadOptions,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp)
        : TBase(
            std::move(chunkSpec),
            std::move(config),
            std::move(client),
            std::move(nodeDirectory),
            blockReadOptions)
        , Timestamp_(timestamp)
    {
        std::tie(Schema_, ColumnFilter_, IdMapping_) = CreateSortedReadParameters(schema, columnFilter);
    }

    virtual TFuture<void> Open()
    {
        DoOpen();
        return ReadyEvent_;
    }

    virtual bool Read(std::vector<TVersionedRow>* rows)
    {
        return DoRead(rows);
    }

private:
    using TBase = TRemoteDynamicStoreReaderBase<TVersionedRow>;

    const TTimestamp Timestamp_;

    virtual void FillRequestAndLog(TQueryServiceProxy::TReqReadDynamicStorePtr req) override
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

    virtual TRows DeserializeRows(const TSharedRef& data) override
    {
        UncompressedDataSize_ += data.Size();
        // Default row buffer.
        TWireProtocolReader reader(data);
        auto schemaData = TWireProtocolReader::GetSchemaData(*Schema_);
        return reader.ReadVersionedRowset(schemaData, true /*deep*/, &IdMapping_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRemoteSortedDynamicStoreReader(
    TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    TRemoteDynamicStoreReaderConfigPtr config,
    NNative::IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    TTrafficMeterPtr /*trafficMeter*/,
    IThroughputThrottlerPtr /*bandwidthThrottler*/,
    IThroughputThrottlerPtr /*rpsThrottler*/,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp)
{
    return New<TRemoteSortedDynamicStoreReader>(
        std::move(chunkSpec),
        std::move(schema),
        std::move(config),
        std::move(client),
        std::move(nodeDirectory),
        blockReadOptions,
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
        NNative::IClientPtr client,
        TNodeDirectoryPtr nodeDirectory,
        const TClientBlockReadOptions& blockReadOptions,
        TNameTablePtr nameTable,
        const std::optional<std::vector<TString>>& columns)
        : TBase(
            std::move(chunkSpec),
            std::move(config),
            std::move(client),
            std::move(nodeDirectory),
            blockReadOptions)
        , Options_(std::move(options))
        , NameTable_(std::move(nameTable))
        , ColumnPresenseBuffer_(schema->GetColumnCount())
    {
        std::tie(ColumnFilter_, IdMapping_) = CreateOrderedReadParameters(
            schema,
            columns,
            NameTable_);

        InitializeSystemColumnIds();

        DoOpen();
    }

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        RowBuffer_->Clear();
        std::vector<TUnversionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);
        if (DoRead(&rows)) {
            return rows.empty()
                ? CreateEmptyUnversionedRowBatch()
                : CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows)));
        } else {
            return nullptr;
        }
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual i64 GetTableRowIndex() const override
    {
        // NB: Table row index is requested only for the first reader in the sequential
        // group and only after ready event is set. Thus either this value is correctly
        // initialized or the first dynamic store of the table is empty, so zero is correct.
        return CurrentRowIndex_ == -1 ? 0 : CurrentRowIndex_;
    }

    //! Returns #unreadRows to reader and builds data slice descriptors for read and unread data.
    virtual NChunkClient::TInterruptDescriptor GetInterruptDescriptor(
        TRange<NTableClient::TUnversionedRow> /*unreadRows*/) const override
    {
        // XXX(ifsmirnov)
        return {};
    }

    virtual const NChunkClient::TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        // XXX(ifsmirnov)
        YT_ABORT();
    }

    virtual TTimingStatistics GetTimingStatistics() const override
    {
        // YT_ABORT();
        return {};
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

    std::vector<bool> ColumnPresenseBuffer_;

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TRemoteDynamicStoreReaderPoolTag());

    virtual void FillRequestAndLog(TQueryServiceProxy::TReqReadDynamicStorePtr req) override
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

    virtual TRows DeserializeRows(const TSharedRef& data) override
    {
        UncompressedDataSize_ += data.Size();

        // Default row buffer.
        TWireProtocolReader reader(data);

        if (!StartRowIndex_.has_value()) {
            StartRowIndex_ = reader.ReadInt64();
            CurrentRowIndex_ = *StartRowIndex_;
            YT_LOG_DEBUG("Received start row index (StartRowIndex: %v)", StartRowIndex_);
        }

        return reader.ReadUnversionedRowset(true /*deep*/, &IdMapping_);
    }

    virtual TUnversionedRow PostprocessRow(TUnversionedRow row) override
    {
        if (SystemColumnCount_ == 0) {
            return row;
        }

        auto clonedRow = RowBuffer_->AllocateUnversioned(row.GetCount() + SystemColumnCount_);
        ::memcpy(clonedRow.Begin(), row.Begin(), sizeof(TUnversionedValue) * row.GetCount());

        auto* end = clonedRow.Begin() + row.GetCount();

        if (Options_->EnableTableIndex) {
            *end++ = MakeUnversionedInt64Value(TableIndex_, TableIndexId_);
        }
        if (Options_->EnableRangeIndex) {
            *end++ = MakeUnversionedInt64Value(RangeIndex_, RangeIndexId_);
        }
        if (Options_->EnableRowIndex) {
            *end++ = MakeUnversionedInt64Value(CurrentRowIndex_++, RowIndexId_);
        }
        if (Options_->EnableTabletIndex) {
            *end++ = MakeUnversionedInt64Value(TabletIndex_, TabletIndexId_);
        }

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
    NNative::IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    TTrafficMeterPtr /*trafficMeter*/,
    IThroughputThrottlerPtr /*bandwidthThrottler*/,
    IThroughputThrottlerPtr /*rpsThrottler*/,
    const TClientBlockReadOptions& blockReadOptions,
    const std::optional<std::vector<TString>>& columns)
{
    return New<TRemoteOrderedDynamicStoreReader>(
        std::move(chunkSpec),
        std::move(schema),
        std::move(config),
        std::move(options),
        std::move(client),
        std::move(nodeDirectory),
        blockReadOptions,
        std::move(nameTable),
        columns);
}

////////////////////////////////////////////////////////////////////////////////

class TRetryingRemoteDynamicStoreReader
    : public IVersionedReader
{
public:
    TRetryingRemoteDynamicStoreReader(
        TChunkSpec chunkSpec,
        TTableSchemaPtr schema,
        TRetryingRemoteDynamicStoreReaderConfigPtr config,
        NNative::IClientPtr client,
        TNodeDirectoryPtr nodeDirectory,
        const TColumnFilter& columnFilter,
        const TClientBlockReadOptions& blockReadOptions,
        TTimestamp timestamp,
        TCallback<IVersionedReaderPtr(NChunkClient::NProto::TChunkSpec)> chunkReaderFactory)
        : ChunkSpec_(std::move(chunkSpec))
        , Schema_(std::move(schema))
        , Config_(std::move(config))
        , Client_(std::move(client))
        , NodeDirectory_(std::move(nodeDirectory))
        , ColumnFilter_(columnFilter)
        , Timestamp_(timestamp)
        , Networks_(Client_->GetNativeConnection()->GetNetworks())
        , BlockReadOptions_(blockReadOptions)
        , ChunkReaderFactory_(chunkReaderFactory)
        , Logger(TableClientLogger.WithTag("ReaderId: %v", TGuid::Create()))
    {
        if (BlockReadOptions_.ReadSessionId) {
            ReadSessionId_ = BlockReadOptions_.ReadSessionId;
        } else {
            ReadSessionId_ = TReadSessionId::Create();
        }

        Logger.AddTag("StoreId: %v", FromProto<TGuid>(ChunkSpec_.chunk_id()));
        Logger.AddTag("ReadSessionId: %v", ReadSessionId_);

        YT_LOG_DEBUG("Retrying remote dynamic store reader created");

        DoCreateRemoteDynamicStoreReader();
    }

    virtual ~TRetryingRemoteDynamicStoreReader() override
    {
        YT_LOG_DEBUG("Retrying remote dynamic store reader destroyed");
    }

    virtual TFuture<void> Open()
    {
        ReadyEvent_ = NewPromise<void>();
        CurrentReader_->Open().Subscribe(
            BIND(&TRetryingRemoteDynamicStoreReader::OnUnderlyingReaderReadyEvent, MakeStrong(this))
                .Via(GetCurrentInvoker()));
        return ReadyEvent_;
    }

    virtual bool Read(std::vector<TVersionedRow>* rows)
    {
        rows->clear();
        if (PreviousReader_ != CurrentReader_) {
            PreviousReader_ = CurrentReader_;
        }

        auto readyEvent = GetReadyEvent();
        if (!readyEvent.IsSet() || !readyEvent.Get().IsOK()) {
            return true;
        }

        ReadyEvent_ = NewPromise<void>();

        try {
            bool result = CurrentReader_->Read(rows);

            if (!ChunkReaderFallbackOccured_) {
                if (!rows->empty()) {
                    auto lastKey = rows->back();
                    YT_VERIFY(lastKey);
                    LastKey_ = TLegacyOwningKey(lastKey.BeginKeys(), lastKey.EndKeys());
                }
                if (result) {
                    CurrentReader_->GetReadyEvent().Subscribe(
                        BIND(&TRetryingRemoteDynamicStoreReader::OnUnderlyingReaderReadyEvent, MakeStrong(this))
                            .Via(GetCurrentInvoker()));
                }
            }

            return result;
        } catch (const std::exception& ex) {
            OnReaderFailed(ex);
            rows->clear();
            return true;
        }
    }

    virtual TFuture<void> GetReadyEvent() const
    {
        return ChunkReaderFallbackOccured_
            ? CurrentReader_->GetReadyEvent()
            : ReadyEvent_;
    }

    virtual TDataStatistics GetDataStatistics() const
    {
        auto dataStatistics = CurrentReader_ ? CurrentReader_->GetDataStatistics() : TDataStatistics{};
        CombineDataStatistics(&dataStatistics, AccumulatedDataStatistics_);
        return dataStatistics;
    }

    virtual TCodecStatistics GetDecompressionStatistics() const
    {
        if (ChunkReaderFallbackOccured_ && CurrentReader_) {
            return CurrentReader_->GetDecompressionStatistics();
        }
        return {};
    }

    virtual bool IsFetchingCompleted() const
    {
        return false;
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const
    {
        return {};
    }

private:
    TChunkSpec ChunkSpec_;
    const TTableSchemaPtr Schema_;
    const TRetryingRemoteDynamicStoreReaderConfigPtr Config_;
    const NNative::IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TColumnFilter ColumnFilter_;
    const TTimestamp Timestamp_;
    const TNetworkPreferenceList Networks_;
    const TClientBlockReadOptions BlockReadOptions_;

    IVersionedReaderPtr CurrentReader_;
    // NB: It is necessary to store failed reader until Read is called for the
    // new one since it may still own some rows that were read but not yet captured.
    IVersionedReaderPtr PreviousReader_;
    TCallback<IVersionedReaderPtr(TChunkSpec)> ChunkReaderFactory_;

    // Data statistics of all previous dynamic store readers.
    TDataStatistics AccumulatedDataStatistics_;

    bool ChunkReaderFallbackOccured_ = false;
    TPromise<void> ReadyEvent_ = NewPromise<void>();

    int RetryCount_ = 0;
    TInstant LastLocateRequestTimestamp_;

    TLegacyOwningKey LastKey_;

    TReadSessionId ReadSessionId_;
    NLogging::TLogger Logger;

    void OnReaderFailed(TError error)
    {
        if (ChunkReaderFallbackOccured_) {
            error.ThrowOnError();
            return;
        }

        YT_LOG_DEBUG(error, "Remote dynamic store reader failed, falling back "
            "(RetryCount: %v, MaxRetryCount: %v, LastLocateRequestTimestamp: %v)",
            RetryCount_,
            Config_->RetryCount,
            LastLocateRequestTimestamp_);

        LocateDynamicStore();
    }

    void OnUnderlyingReaderReadyEvent(TError error)
    {
        if (error.IsOK()) {
            ReadyEvent_.Set();
        } else {
            LocateDynamicStore();
        }
    }

    void LocateDynamicStore()
    {
        if (RetryCount_ == Config_->RetryCount) {
            ReadyEvent_.Set(TError("Too many locate retries failed, backing off"));
            return;
        }

        if (LastLocateRequestTimestamp_ + Config_->LocateRequestBackoffTime > TInstant::Now()) {
            TDelayedExecutor::Submit(
                BIND(&TRetryingRemoteDynamicStoreReader::DoLocateDynamicStore, MakeStrong(this))
                    .Via(GetCurrentInvoker()),
                LastLocateRequestTimestamp_ + Config_->LocateRequestBackoffTime);
        } else {
            DoLocateDynamicStore();
        }
    }

    void DoLocateDynamicStore()
    {
        LastLocateRequestTimestamp_ = TInstant::Now();
        ++RetryCount_;

        YT_LOG_DEBUG("Locating dynamic store (RetryCount: %v, MaxRetryCount: %v)",
            RetryCount_,
            Config_->RetryCount);

        try {
            auto storeId = FromProto<TDynamicStoreId>(ChunkSpec_.chunk_id());
            auto channel = Client_->GetMasterChannelOrThrow(
                EMasterChannelKind::Follower,
                CellTagFromId(storeId));

            TChunkServiceProxy proxy(channel);

            auto req = proxy.LocateDynamicStores();
            ToProto(req->add_subrequests(), storeId);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
            req->SetHeavy(true);
            req->Invoke().Subscribe(
                BIND(&TRetryingRemoteDynamicStoreReader::OnLocateResponse, MakeStrong(this))
                    .Via(GetCurrentInvoker()));
        } catch (const std::exception& ex) {
            ReadyEvent_.Set(TError("Error communicating with master") << ex);
        }
    }

    void OnLocateResponse(const TChunkServiceProxy::TErrorOrRspLocateDynamicStoresPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Failed to locate dynamic store");
            LocateDynamicStore();
            return;
        }

        const auto& rsp = rspOrError.Value();
        YT_VERIFY(rsp->subresponses_size() == 1);
        auto& subresponse = *rsp->mutable_subresponses(0);


        // Dynamic store is missing.
        if (subresponse.missing()) {
            YT_LOG_DEBUG("Dynamic store located: store is missing");
            ReadyEvent_.Set(TError("Dynamic store is missing"));
            return;
        }

        CombineDataStatistics(&AccumulatedDataStatistics_, CurrentReader_->GetDataStatistics());

        // Dynamic store was empty and flushed to no chunk.
        if (!subresponse.has_chunk_spec()) {
            YT_LOG_DEBUG("Dynamic store located: store is flushed to no chunk");
            CurrentReader_ = CreateEmptyVersionedReader();
            YT_VERIFY(CurrentReader_->Open().IsSet());

            ChunkReaderFallbackOccured_ = true;
            ReadyEvent_.Set();
            return;
        }

        NodeDirectory_->MergeFrom(rsp->node_directory());

        auto& chunkSpec = *subresponse.mutable_chunk_spec();
        // Dynamic store is not flushed.
        if (TypeFromId(FromProto<TDynamicStoreId>(chunkSpec.chunk_id())) == EObjectType::SortedDynamicTabletStore) {
            if (chunkSpec.replicas_size() == 0) {
                YT_LOG_DEBUG("Dynamic store located: store has no replicas");
                LocateDynamicStore();
                return;
            }

            YT_LOG_DEBUG("Dynamic store located: got new replicas (LastKey: %v)",
                LastKey_);
            ChunkSpec_.clear_replicas();
            for (auto replica : chunkSpec.replicas()) {
                ChunkSpec_.add_replicas(replica);
            }

            SetLowerBoundInChunkSpec();

            DoCreateRemoteDynamicStoreReader();

            if (ReadyEvent_.IsCanceled()) {
                YT_LOG_DEBUG("Reader canceled");
                return;
            }
            YT_VERIFY(!ReadyEvent_.IsSet());

            CurrentReader_->Open().Subscribe(
                BIND(&TRetryingRemoteDynamicStoreReader::OnUnderlyingReaderReadyEvent, MakeStrong(this))
                    .Via(GetCurrentInvoker()));
        } else {
            if (ChunkSpec_.has_lower_limit()) {
                *chunkSpec.mutable_lower_limit() = ChunkSpec_.lower_limit();
            }
            if (ChunkSpec_.has_upper_limit()) {
                *chunkSpec.mutable_upper_limit() = ChunkSpec_.upper_limit();
            }
            std::swap(ChunkSpec_, chunkSpec);
            SetLowerBoundInChunkSpec();

            YT_LOG_DEBUG("Dynamic store located: falling back to chunk reader (ChunkId: %v, LastKey: %v)",
                FromProto<TChunkId>(ChunkSpec_.chunk_id()),
                LastKey_);

            try {
                CurrentReader_ = ChunkReaderFactory_(ChunkSpec_);
            } catch (const std::exception& ex) {
                ReadyEvent_.Set(TError("Failed to create chunk reader") << ex);
                return;
            }

            CurrentReader_->Open().Subscribe(
                BIND(&TRetryingRemoteDynamicStoreReader::OnChunkReaderOpened, MakeStrong(this))
                    .Via(GetCurrentInvoker()));
        }
    }

    void OnChunkReaderOpened(TError error)
    {
        if (error.IsOK()) {
            ChunkReaderFallbackOccured_ = true;
            ReadyEvent_.Set();
        } else {
            ReadyEvent_.Set(error);
        }
    }

    void SetLowerBoundInChunkSpec()
    {
        if (LastKey_) {
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
    }

    void CombineDataStatistics(TDataStatistics* statistics, const TDataStatistics& delta) const
    {
        statistics->set_uncompressed_data_size(statistics->uncompressed_data_size() + delta.uncompressed_data_size());
        statistics->set_row_count(statistics->row_count() + delta.row_count());
        statistics->set_data_weight(statistics->data_weight() + delta.data_weight());
    }

    void DoCreateRemoteDynamicStoreReader()
    {
        TClientBlockReadOptions blockReadOptions;
        blockReadOptions.ReadSessionId = ReadSessionId_;
        CurrentReader_ = CreateRemoteSortedDynamicStoreReader(
            ChunkSpec_,
            Schema_,
            Config_,
            Client_,
            NodeDirectory_,
            {} /*trafficMeter*/,
            {} /*bandwidthThrottler*/,
            {} /*rpsThrottler*/,
            blockReadOptions,
            ColumnFilter_,
            Timestamp_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRetryingRemoteSortedDynamicStoreReader(
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    TRetryingRemoteDynamicStoreReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::TTrafficMeterPtr /*trafficMeter*/,
    NConcurrency::IThroughputThrottlerPtr /*bandwidthThrottler*/,
    NConcurrency::IThroughputThrottlerPtr /*rpsThrottler*/,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    TCallback<IVersionedReaderPtr(NChunkClient::NProto::TChunkSpec)> chunkReaderFactory)
{
    return New<TRetryingRemoteDynamicStoreReader>(
        std::move(chunkSpec),
        schema,
        config,
        client,
        nodeDirectory,
        columnFilter,
        blockReadOptions,
        timestamp,
        chunkReaderFactory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
