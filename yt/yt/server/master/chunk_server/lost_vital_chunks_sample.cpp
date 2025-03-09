#include "lost_vital_chunks_sample.h"

#include "chunk_manager.h"
#include "chunk_replicator.h"
#include "config.h"

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

TLostVitalChunksSample::TLostVitalChunksSample(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

const TLostVitalChunksSampleMap& TLostVitalChunksSample::GetCellLostVitalChunks() const
{
    return LostVitalChunksSample_;
}

void TLostVitalChunksSample::Clear()
{
    LostVitalChunksSample_.clear();
}

void TLostVitalChunksSample::Load(TLoadContext& context)
{
    using NYT::Load;
    if (context.GetVersion() >= NCellMaster::EMasterReign::MulticellStatisticsCollector) {
        Load(context, LostVitalChunksSample_);
    }
}

void TLostVitalChunksSample::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, LostVitalChunksSample_);
}

void TLostVitalChunksSample::HydraApplyMulticellStatisticsUpdate(NProto::TReqLostVitalChunksSample* request)
{
    YT_VERIFY(NHydra::HasMutationContext());

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsPrimaryMaster());

    using NYT::FromProto;
    auto cellTag = FromProto<TCellTag>(request->cell_tag());
    LostVitalChunksSample_[cellTag] = FromProto<std::vector<TObjectId>>(request->chunk_ids());

    if (!request->chunk_ids().empty()) {
        YT_LOG_DEBUG("Received lost vital chunks sample (LostVitalChunksSample: %v)", request->chunk_ids());
    }
}

void TLostVitalChunksSample::FinishUpdate()
{ }

TFuture<NProto::TReqLostVitalChunksSample> TLostVitalChunksSample::GetLocalCellUpdate()
{
    const auto& dynamicConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;
    auto limit = dynamicConfig->MaxLostVitalChunksSampleSizePerCell;

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto channels = chunkManager->GetChunkReplicatorChannels();

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> responseFutures;
    responseFutures.reserve(channels.size());
    for (const auto& channel : channels) {
        auto proxy = TObjectServiceProxy::FromDirectMasterChannel(channel);
        auto batchReq = proxy.ExecuteBatch();
        auto req = TCypressYPathProxy::Enumerate("//sys/local_lost_vital_chunks");
        req->set_limit(limit);
        batchReq->AddRequest(req, "enumerate");
        responseFutures.push_back(batchReq->Invoke());
    }

    using TRspExecuteBatchPtr = TObjectServiceProxy::TRspExecuteBatchPtr;

    auto processCollected = [this, limit] (TErrorOr<std::vector<TErrorOr<TRspExecuteBatchPtr>>>&& response) {
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

        auto fairSampleKeys = [&] {
            constexpr auto defaultLimit = TDynamicChunkManagerConfig::DefaultMaxLostVitalChunksSampleSizePerCell;
            TCompactVector<TObjectId, defaultLimit> keys;
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
        };

        NProto::TReqLostVitalChunksSample req;
        req.set_cell_tag(ToProto(Bootstrap_->GetMulticellManager()->GetCellTag()));
        ToProto(req.mutable_chunk_ids(), fairSampleKeys());

        if (!req.chunk_ids().empty()) {
            YT_LOG_DEBUG("Sending lost vital chunks sample (LostVitalChunksSample: %v)", req.chunk_ids());
        }

        return req;
    };

    return AllSet(std::move(responseFutures)).ApplyUnique(BIND(std::move(processCollected)));
}

std::optional<TDuration> TLostVitalChunksSample::GetUpdatePeriod()
{
    const auto& dynamicConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;
    return dynamicConfig->LostVitalChunksSampleUpdatePeriod;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
