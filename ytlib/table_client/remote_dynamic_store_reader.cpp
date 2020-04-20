#include "remote_dynamic_store_reader.h"
#include "private.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/query_client/query_service_proxy.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/client/table_client/versioned_reader.h>
#include <yt/client/table_client/wire_protocol.h>
#include <yt/client/table_client/row_buffer.h>

#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/tablet_client/config.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NApi::NNative;
using namespace NChunkClient;
using namespace NQueryClient;
using namespace NChunkClient::NProto;
using namespace NTabletClient;
using namespace NApi;
using namespace NObjectClient;

using NYT::FromProto;
using TIdMapping = SmallVector<int, TypicalColumnCount>;

////////////////////////////////////////////////////////////////////////////////

std::tuple<TTableSchema, TColumnFilter, TIdMapping> CreateReadParameters(
    const TTableSchema& schema,
    const TColumnFilter& columnFilter)
{
    TIdMapping idMapping(static_cast<size_t>(schema.GetColumnCount()), -1);

    if (columnFilter.IsUniversal()) {
        for (int index = 0; index < idMapping.size(); ++index) {
             idMapping[index] = index;
        }
        return {schema, TColumnFilter{}, idMapping};
    } else {
        int nextId = 0;
        TColumnFilter::TIndexes columnFilterIndexes;
        for (int index = 0; index < schema.GetKeyColumnCount(); ++index) {
            idMapping[index] = nextId++;
            columnFilterIndexes.push_back(index);
        }
        for (int index : columnFilter.GetIndexes()) {
            if (index >= schema.GetKeyColumnCount()) {
                idMapping[index] = nextId++;
                columnFilterIndexes.push_back(index);
            }
        }

        TColumnFilter readColumnFilter(std::move(columnFilterIndexes));

        return {schema.Filter(readColumnFilter), readColumnFilter, idMapping};
    }
}

////////////////////////////////////////////////////////////////////////////////

class TRemoteDynamicStoreReader
    : public IVersionedReader
{
public:
    TRemoteDynamicStoreReader(
        const TChunkSpec& chunkSpec,
        TTableSchema schema,
        TRemoteDynamicStoreReaderConfigPtr config,
        NNative::IClientPtr client,
        TNodeDirectoryPtr nodeDirectory,
        const TClientBlockReadOptions& blockReadOptions,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp)
        : ChunkSpec_(chunkSpec)
        , Config_(config)
        , Client_(client)
        , NodeDirectory_(nodeDirectory)
        , Timestamp_(timestamp)
        , Networks_(Client_->GetNativeConnection()->GetNetworks())
        , Logger(NLogging::TLogger(TableClientLogger)
            .AddTag("ReaderId: %v", TGuid::Create()))
    {
        if (blockReadOptions.ReadSessionId) {
            ReadSessionId_ = blockReadOptions.ReadSessionId;
        } else {
            ReadSessionId_ = TReadSessionId::Create();
        }

        Logger.AddTag("StoreId: %v", FromProto<TDynamicStoreId>(chunkSpec.chunk_id()));
        Logger.AddTag("ReadSessionId: %v", ReadSessionId_);

        std::tie(Schema_, ColumnFilter_, IdMapping_) = CreateReadParameters(schema, columnFilter);

        YT_LOG_DEBUG("Remote dynamic store reader created");
    }

    virtual ~TRemoteDynamicStoreReader() override
    {
        YT_LOG_DEBUG("Remote dynamic store reader destroyed");
    }

    virtual TFuture<void> Open()
    {
        DoOpen();
        return ReadyEvent_;
    }

    virtual bool Read(std::vector<TVersionedRow>* rows)
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
                rows->push_back(loadedRows[RowIndex_++]);
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

    virtual TFuture<void> GetReadyEvent()
    {
        return ReadyEvent_;
    }

    virtual TDataStatistics GetDataStatistics() const
    {
        TDataStatistics dataStatistics;

        dataStatistics.set_chunk_count(0);
        dataStatistics.set_uncompressed_data_size(UncompressedDataSize_);
        dataStatistics.set_compressed_data_size(0);

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

private:
    using TRows = TSharedRange<TVersionedRow>;

    const TChunkSpec ChunkSpec_;
    const TRemoteDynamicStoreReaderConfigPtr Config_;
    const NNative::IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TTimestamp Timestamp_;
    const TNetworkPreferenceList Networks_;

    TTableSchema Schema_;
    TColumnFilter ColumnFilter_;
    TIdMapping IdMapping_;

    TPromise<void> ReadyEvent_ = NewPromise<void>();
    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    NConcurrency::IAsyncZeroCopyInputStreamPtr InputStream_;
    TFuture<TRows> RowsFuture_;
    int RowIndex_ = -1;
    TRows StoredRowset_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;
    std::atomic<i64> UncompressedDataSize_ = 0;
    std::vector<TChunkId> FailedChunkIds_;

    TReadSessionId ReadSessionId_;
    NLogging::TLogger Logger;

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
            if (!ColumnFilter_.IsUniversal())  {
                ToProto(req->mutable_column_filter()->mutable_indexes(), ColumnFilter_.GetIndexes());
            }

            ToProto(req->mutable_read_session_id(), ReadSessionId_);

            auto lowerLimit = FromProto<NChunkClient::TReadLimit>(ChunkSpec_.lower_limit());
            auto upperLimit = FromProto<NChunkClient::TReadLimit>(ChunkSpec_.upper_limit());
            if (lowerLimit.HasKey()) {
                ToProto(req->mutable_lower_bound(), lowerLimit.GetKey());
            }
            if (upperLimit.HasKey()) {
                ToProto(req->mutable_upper_bound(), upperLimit.GetKey());
            }

            req->set_timestamp(Timestamp_);

            YT_LOG_DEBUG("Collected remote dynamic store reader parameters (Range: <%v .. %v>, Timestamp: %v, ColumnFilter: %v)",
                lowerLimit,
                upperLimit,
                Timestamp_,
                ColumnFilter_);

            req->ClientAttachmentsStreamingParameters().ReadTimeout = Config_->ClientReadTimeout;
            req->ServerAttachmentsStreamingParameters().ReadTimeout = Config_->ServerReadTimeout;
            req->ClientAttachmentsStreamingParameters().WriteTimeout = Config_->ClientWriteTimeout;
            req->ServerAttachmentsStreamingParameters().WriteTimeout = Config_->ServerWriteTimeout;

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

    TRows DeserializeRows(const TSharedRef& data)
    {
        UncompressedDataSize_ += data.Size();
        // Default row buffer.
        TWireProtocolReader reader(data);
        auto schemaData = TWireProtocolReader::GetSchemaData(Schema_);
        return reader.ReadVersionedRowset(schemaData, true /*deep*/, &IdMapping_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRemoteDynamicStoreReader(
    const TChunkSpec& chunkSpec,
    TTableSchema schema,
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
    return New<TRemoteDynamicStoreReader>(
        chunkSpec,
        std::move(schema),
        std::move(config),
        std::move(client),
        std::move(nodeDirectory),
        blockReadOptions,
        columnFilter,
        timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
