#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/proto/lost_vital_chunks_sample.pb.h>

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cell_master/multicell_statistics_collector.h>

#include <yt/yt/ytlib/object_client/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

using TLostVitalChunksSampleMap = THashMap<NObjectClient::TCellTag, std::vector<NObjectClient::TObjectId>>;

////////////////////////////////////////////////////////////////////////////////

class TLostVitalChunksSample
{
public:
    explicit TLostVitalChunksSample(NCellMaster::TBootstrap* bootstrap);

    const TLostVitalChunksSampleMap& GetCellLostVitalChunks() const;

    // CMulticellStatisticsValue implementation
    using TMutationRequestTypes = std::tuple<NProto::TReqLostVitalChunksSample>;

    void Clear();

    void Load(NCellMaster::TLoadContext& context);

    void Save(NCellMaster::TSaveContext& context) const;

    void HydraApplyMulticellStatisticsUpdate(NProto::TReqLostVitalChunksSample* request);

    void FinishUpdate();

    TFuture<NProto::TReqLostVitalChunksSample> GetLocalCellUpdate();

    std::optional<TDuration> GetUpdatePeriod();

private:
    NCellMaster::TBootstrap* Bootstrap_;

    TLostVitalChunksSampleMap LostVitalChunksSample_;
};

static_assert(NCellMaster::CMulticellStatisticsValue<TLostVitalChunksSample>);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
