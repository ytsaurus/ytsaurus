#include "chunks_samples.h"

#include "chunk_manager.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/attributes.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NServer;
using namespace NYTree;
using namespace NYson;

using NCypressClient::TCypressYPathProxy;
using NObjectClient::TCellTag;
using NObjectClient::TObjectId;
using NObjectClient::TObjectServiceProxy;
using NObjectClient::TObjectYPathProxy;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto& Logger = ChunkServerLogger;

using TRspExecuteBatchPtr = TObjectServiceProxy::TRspExecuteBatchPtr;

using TRspEnumeratePtr = TCypressYPathProxy::TRspEnumeratePtr;

std::vector<TRspEnumeratePtr> ExtractResponses(TErrorOr<std::vector<TErrorOr<TRspExecuteBatchPtr>>>&& responses)
{
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

    auto batchRsps = std::move(responses).ValueOrDefault({});
    std::vector<TRspEnumeratePtr> batchResponses(batchRsps.size());
    std::ranges::transform(batchRsps, batchResponses.begin(), extractResponse);
    return batchResponses;
}

} // namespace

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

const TChunksSampleMap& TChunksSamples::GetCellOldestPartMissingChunks() const
{
    return OldestPartMissingChunksSample_;
}

const TChunksSampleMap& TChunksSamples::GetCellQuorumMissingChunks() const
{
    return QuorumMissingChunksSample_;
}

const TChunksSampleMap& TChunksSamples::GetCellInconsistentlyPlacedChunks() const
{
    return InconsistentlyPlacedChunksSample_;
}

void TChunksSamples::Clear()
{
    LostVitalChunksSample_.clear();
    DataMissingChunksSample_.clear();
    ParityMissingChunksSample_.clear();
    OldestPartMissingChunksSample_.clear();
    QuorumMissingChunksSample_.clear();
    InconsistentlyPlacedChunksSample_.clear();
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
    if (context.GetVersion() >= NCellMaster::EMasterReign::AdditionalMulticellChunksSamples) {
        Load(context, OldestPartMissingChunksSample_);
        Load(context, QuorumMissingChunksSample_);
        Load(context, InconsistentlyPlacedChunksSample_);
    }
}

void TChunksSamples::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, LostVitalChunksSample_);
    Save(context, DataMissingChunksSample_);
    Save(context, ParityMissingChunksSample_);
    Save(context, OldestPartMissingChunksSample_);
    Save(context, QuorumMissingChunksSample_);
    Save(context, InconsistentlyPlacedChunksSample_);
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
    OldestPartMissingChunksSample_[cellTag] = FromProto<std::vector<TObjectId>>(request->oldest_part_missing_chunk_ids());
    QuorumMissingChunksSample_[cellTag] = FromProto<std::vector<TObjectId>>(request->quorum_missing_chunk_ids());
    InconsistentlyPlacedChunksSample_[cellTag] = FromProto<std::vector<TObjectId>>(request->inconsistently_placed_chunk_ids());

    if (!request->lost_vital_chunk_ids().empty()) {
        YT_LOG_DEBUG("Received lost vital chunks sample (LostVitalChunksSampleSize: %v)", request->lost_vital_chunk_ids_size());
    }
    if (!request->data_missing_chunk_ids().empty()) {
        YT_LOG_DEBUG("Received data missing chunks sample (DataMissingChunksSampleSize: %v)", request->data_missing_chunk_ids_size());
    }
    if (!request->parity_missing_chunk_ids().empty()) {
        YT_LOG_DEBUG("Received parity missing chunks sample (ParityMissingChunksSampleSize: %v)", request->parity_missing_chunk_ids_size());
    }
    if (!request->oldest_part_missing_chunk_ids().empty()) {
        YT_LOG_DEBUG("Received oldest part missing chunks sample (OldestPartMissingChunksSampleSize: %v)", request->oldest_part_missing_chunk_ids_size());
    }
    if (!request->quorum_missing_chunk_ids().empty()) {
        YT_LOG_DEBUG("Received quorum missing chunks sample (QuorumMissingChunksSampleSize: %v)", request->quorum_missing_chunk_ids_size());
    }
    if (!request->inconsistently_placed_chunk_ids().empty()) {
        YT_LOG_DEBUG("Received inconsistently placed chunks sample (InconsistentlyPlacedChunksSampleSize: %v)", request->inconsistently_placed_chunk_ids_size());
    }
}

void TChunksSamples::FinishUpdate()
{ }

std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> TChunksSamples::SendLocalSampleRequests(
        NYPath::TYPath localChunksPath,
        TAttributeFilter attributeFilter,
        std::optional<int> limit)
{
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
        if (limit) {
            req->set_limit(limit.value());
        }

        if (attributeFilter) {
            ToProto(req->mutable_attributes(), attributeFilter);
        }

        batchReq->AddRequest(req, "enumerate");
        responseFutures.push_back(batchReq->Invoke());
    }
    return responseFutures;
}

TFuture<TChunksSamples::TLocalSampleVector> TChunksSamples::GetLocalSample(NYPath::TYPath localChunksPath)
{
    const auto& dynamicConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;
    auto limit = dynamicConfig->MaxChunksSampleSizePerCell;

    auto responseFutures = SendLocalSampleRequests(localChunksPath, TAttributeFilter(), limit);

    return AllSet(std::move(responseFutures))
        .ApplyUnique(BIND([limit] (TErrorOr<std::vector<TErrorOr<TRspExecuteBatchPtr>>>&& response) {
        auto batchResponses = ExtractResponses(std::move(response));

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

TFuture<TChunksSamples::TLocalSampleVector> TChunksSamples::GetLocalOldestPartMissingChunkSample()
{
    const auto& dynamicConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;

    auto responseFutures = SendLocalSampleRequests(
        "//sys/local_oldest_part_missing_chunks",
        TAttributeFilter({
            EInternedAttributeKey::Vital.Unintern(),
            EInternedAttributeKey::PartLossTime.Unintern()}),
        dynamicConfig->MaxOldestPartMissingChunks);

    auto limit = dynamicConfig->MaxChunksSampleSizePerCell;

    return AllSet(std::move(responseFutures))
        .ApplyUnique(BIND([limit] (TErrorOr<std::vector<TErrorOr<TRspExecuteBatchPtr>>>&& response) {
        auto batchResponses = ExtractResponses(std::move(response));

        struct TMissingPartChunkInfo {
            TChunkId ChunkId;
            TInstant PartLossTime;
        };

        std::vector<TMissingPartChunkInfo> partLostChunks;

        for (const auto& response : batchResponses) {
            if (!response) {
                continue;
            }
            for (const auto& chunk : response->items()) {
                if (!chunk.has_attributes()) {
                    continue;
                }
                auto attr = ConvertToAttributes(
                    TYsonString(chunk.attributes(), EYsonType::MapFragment));

                auto vitalChunk = attr->Find<bool>(EInternedAttributeKey::Vital.Unintern());
                if (!vitalChunk || !vitalChunk.value()) {
                    continue;
                }

                auto partLossTime = attr->Find<TInstant>(EInternedAttributeKey::PartLossTime.Unintern());
                if (partLossTime) {
                    partLostChunks.emplace_back(
                        TChunkId::FromString(chunk.key()),
                        partLossTime.value());
                }
            }
        }
        if (std::ssize(partLostChunks) > limit) {
            std::ranges::nth_element(
                partLostChunks,
                partLostChunks.begin() + limit,
                [](const auto& lhs, const auto& rhs) {
                    return lhs.PartLossTime < rhs.PartLossTime;
                });
            partLostChunks.resize(limit);
        }

        TLocalSampleVector keys(partLostChunks.size());
        std::ranges::transform(partLostChunks, keys.begin(), [](const auto& chunkInfo) {
            return chunkInfo.ChunkId;
        });

        return keys;
    }));
}

TFuture<NProto::TReqChunksSamples> TChunksSamples::GetLocalCellUpdate()
{
    return AllSet(std::vector{
        GetLocalSample("//sys/local_lost_vital_chunks"),
        GetLocalSample("//sys/local_data_missing_chunks"),
        GetLocalSample("//sys/local_parity_missing_chunks"),
        GetLocalOldestPartMissingChunkSample(),
        GetLocalSample("//sys/local_quorum_missing_chunks"),
        GetLocalSample("//sys/local_inconsistently_placed_chunks")})
        .Apply(BIND([cellTag = Bootstrap_->GetMulticellManager()->GetCellTag()] (const std::vector<TErrorOr<TLocalSampleVector>>& samples) {
            NProto::TReqChunksSamples req;
            req.set_cell_tag(ToProto(cellTag));
            YT_VERIFY(samples.size() == 6);
            ToProto(req.mutable_lost_vital_chunk_ids(), samples[0].ValueOrDefault({}));
            ToProto(req.mutable_data_missing_chunk_ids(), samples[1].ValueOrDefault({}));
            ToProto(req.mutable_parity_missing_chunk_ids(), samples[2].ValueOrDefault({}));
            ToProto(req.mutable_oldest_part_missing_chunk_ids(), samples[3].ValueOrDefault({}));
            ToProto(req.mutable_quorum_missing_chunk_ids(), samples[4].ValueOrDefault({}));
            ToProto(req.mutable_inconsistently_placed_chunk_ids(), samples[5].ValueOrDefault({}));
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
