#include "chunks_samples.h"

#include "chunk_manager.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto& Logger = ChunkServerLogger;

} // namespace

////////////////////////////////////////////////////////////////////////////////

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NYTree;

using NCypressClient::TCypressYPathProxy;
using NObjectClient::TCellTag;
using NObjectClient::TObjectId;
using NObjectClient::TObjectServiceProxy;
using NObjectClient::TObjectYPathProxy;

////////////////////////////////////////////////////////////////////////////////

TChunksSamples::TChunksSamples(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

const TChunksSampleMap& TChunksSamples::GetCellLostVitalChunks() const
{
    return LostVitalChunksSample_;
}

const TChunksSampleMap& TChunksSamples::GetCellDataMissingChunks() const
{
    return DataMissingChunksSample_;
}

const TChunksSampleMap& TChunksSamples::GetCellParityMissingChunks() const
{
    return ParityMissingChunksSample_;
}

void TChunksSamples::Clear()
{
    LostVitalChunksSample_.clear();
    DataMissingChunksSample_.clear();
    ParityMissingChunksSample_.clear();
}

void TChunksSamples::Load(TLoadContext& context)
{
    using NYT::Load;
    if (context.GetVersion() >= NCellMaster::EMasterReign::MulticellStatisticsCollector) {
        Load(context, LostVitalChunksSample_);
    }
    if (context.GetVersion() >= NCellMaster::EMasterReign::MulticellChunksSamples) {
        Load(context, DataMissingChunksSample_);
        Load(context, ParityMissingChunksSample_);
    }
}

void TChunksSamples::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, LostVitalChunksSample_);
    Save(context, DataMissingChunksSample_);
    Save(context, ParityMissingChunksSample_);
}

void TChunksSamples::HydraApplyMulticellStatisticsUpdate(NProto::TReqChunksSamples* request)
{
    YT_VERIFY(NHydra::HasMutationContext());

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsPrimaryMaster());

    using NYT::FromProto;
    auto cellTag = FromProto<TCellTag>(request->cell_tag());
    LostVitalChunksSample_[cellTag] = FromProto<std::vector<TObjectId>>(request->lost_vital_chunk_ids());
    DataMissingChunksSample_[cellTag] = FromProto<std::vector<TObjectId>>(request->data_missing_chunk_ids());
    ParityMissingChunksSample_[cellTag] = FromProto<std::vector<TObjectId>>(request->parity_missing_chunk_ids());

    if (!request->lost_vital_chunk_ids().empty()) {
        YT_LOG_DEBUG("Received lost vital chunks sample (LostVitalChunksSample: %v)", request->lost_vital_chunk_ids());
    }
    if (!request->data_missing_chunk_ids().empty()) {
        YT_LOG_DEBUG("Received data missing chunks sample (DataMissingChunksSample: %v)", request->data_missing_chunk_ids());
    }
    if (!request->parity_missing_chunk_ids().empty()) {
        YT_LOG_DEBUG("Received parity missing chunks sample (ParityMissingChunksSample: %v)", request->parity_missing_chunk_ids());
    }
}

void TChunksSamples::FinishUpdate()
{ }

TFuture<TChunksSamples::TLocalSampleVector> TChunksSamples::GetLocalSample(NYPath::TYPath localChunksPath)
{
    const auto& dynamicConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;
    auto limit = dynamicConfig->MaxChunksSampleSizePerCell;

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto channels = chunkManager->GetChunkReplicatorChannels();

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> responseFutures;
    responseFutures.reserve(channels.size());
    for (const auto& channel : channels) {
        auto proxy = TObjectServiceProxy::FromDirectMasterChannel(channel);
        // TODO(nadya02): Set the correct timeout here.
        proxy.SetDefaultTimeout(NRpc::DefaultRpcRequestTimeout);
        auto batchReq = proxy.ExecuteBatch();
        auto req = TCypressYPathProxy::Enumerate(localChunksPath);
        req->set_limit(limit);
        batchReq->AddRequest(req, "enumerate");
        responseFutures.push_back(batchReq->Invoke());
    }

    using TRspExecuteBatchPtr = TObjectServiceProxy::TRspExecuteBatchPtr;

    return AllSet(std::move(responseFutures))
        .ApplyUnique(BIND([limit] (TErrorOr<std::vector<TErrorOr<TRspExecuteBatchPtr>>>&& response) {
        using TRspEnumeratePtr = TCypressYPathProxy::TRspEnumeratePtr;
        auto extractResponse = [] (const TErrorOr<TRspExecuteBatchPtr>& batchRsp) -> TRspEnumeratePtr {
            if (!batchRsp.IsOK()) {
                return {};
            }

            auto rspOrError = batchRsp.Value()->GetResponse<TCypressYPathProxy::TRspEnumerate>("enumerate");
            if (!rspOrError.IsOK()) {
                return {};
            }

            return std::move(rspOrError).Value();
        };

        auto batchRsps = std::move(response).ValueOrDefault({});
        std::vector<TRspEnumeratePtr> batchResponses(batchRsps.size());
        std::ranges::transform(batchRsps, batchResponses.begin(), extractResponse);

        std::ranges::sort(batchResponses, [&] (const auto& lhs, const auto& rhs) {
            if (!lhs) {
                return false;
            }

            if (!rhs) {
                return true;
            }

            return lhs->items_size() > rhs->items_size();
        });

        int maxResponses = 0;
        if (!batchResponses.empty()) {
            if (auto rsp = batchResponses.front()) {
                maxResponses = std::ranges::min(rsp->items_size(), limit);
            }
        }

        TLocalSampleVector keys;
        // Here we make the sample fairly distributed among all responses.
        for (int i = 0; i < maxResponses; ++i) {
            for (const auto& batchResponse : batchResponses) {
                if (std::ssize(keys) >= limit) {
                    return keys;
                }

                if (!batchResponse || batchResponse->items_size() <= i) {
                    break;
                }

                auto chunkId = TChunkId::FromString(batchResponse->items()[i].key());
                keys.push_back(chunkId);
            }
        }

        return keys;
    }));
}

TFuture<NProto::TReqChunksSamples> TChunksSamples::GetLocalCellUpdate() {
    return AllSet(std::vector{
        GetLocalSample("//sys/local_lost_vital_chunks"),
        GetLocalSample("//sys/local_data_missing_chunks"),
        GetLocalSample("//sys/local_parity_missing_chunks")})
        .ApplyUnique(BIND([cellTag = Bootstrap_->GetMulticellManager()->GetCellTag()] (std::vector<TErrorOr<TLocalSampleVector>>&& samples) {
            NProto::TReqChunksSamples req;
            req.set_cell_tag(ToProto(cellTag));
            YT_VERIFY(samples.size() == 3);
            ToProto(req.mutable_lost_vital_chunk_ids(), samples[0].Value());
            ToProto(req.mutable_data_missing_chunk_ids(), samples[1].Value());
            ToProto(req.mutable_parity_missing_chunk_ids(), samples[2].Value());
            return req;
        }));
}

std::optional<TDuration> TChunksSamples::GetUpdatePeriod()
{
    const auto& dynamicConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;
    return dynamicConfig->ChunkSamplesUpdatePeriod;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
