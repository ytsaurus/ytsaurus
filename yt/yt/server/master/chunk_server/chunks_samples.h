#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/server/master/chunk_server/proto/chunks_samples.pb.h>

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cell_master/multicell_statistics_collector.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

using TChunksSampleMap = THashMap<NObjectClient::TCellTag, std::vector<NObjectClient::TObjectId>>;

////////////////////////////////////////////////////////////////////////////////

class TChunksSamples
{
public:
    explicit TChunksSamples(NCellMaster::TBootstrap* bootstrap);

    const TChunksSampleMap& GetCellLostVitalChunks() const;

    const TChunksSampleMap& GetCellDataMissingChunks() const;

    const TChunksSampleMap& GetCellParityMissingChunks() const;

    const TChunksSampleMap& GetCellOldestPartMissingChunks() const;

    const TChunksSampleMap& GetCellQuorumMissingChunks() const;

    const TChunksSampleMap& GetCellInconsistentlyPlacedChunks() const;

    // CMulticellStatisticsValue implementation
    using TMutationRequestTypes = std::tuple<NProto::TReqChunksSamples>;

    void Clear();

    void Load(NCellMaster::TLoadContext& context);

    void Save(NCellMaster::TSaveContext& context) const;

    void HydraApplyMulticellStatisticsUpdate(NProto::TReqChunksSamples* request);

    void FinishUpdate();

    TFuture<NProto::TReqChunksSamples> GetLocalCellUpdate();

    std::optional<TDuration> GetUpdatePeriod();

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    TChunksSampleMap LostVitalChunksSample_;
    TChunksSampleMap DataMissingChunksSample_;
    TChunksSampleMap ParityMissingChunksSample_;
    TChunksSampleMap OldestPartMissingChunksSample_;
    TChunksSampleMap QuorumMissingChunksSample_;
    TChunksSampleMap InconsistentlyPlacedChunksSample_;

    using TLocalSampleVector = TCompactVector<NCypressClient::TObjectId, TDynamicChunkManagerConfig::DefaultMaxChunksSampleSizePerCell>;

    TFuture<TLocalSampleVector> GetLocalSample(NYPath::TYPath localChunksPath);

    TFuture<TChunksSamples::TLocalSampleVector> GetLocalOldestPartMissingChunkSample();

    std::vector<TFuture<NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr>> SendLocalSampleRequests(
        NYPath::TYPath localChunksPath,
        NYTree::TAttributeFilter attributeFilter,
        std::optional<int> limit);
};

static_assert(NCellMaster::CMulticellStatisticsValue<TChunksSamples>);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
