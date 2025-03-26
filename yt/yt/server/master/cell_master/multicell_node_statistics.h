#pragma once

#include "public.h"
#include "multicell_statistics_collector.h"

#include <yt/yt/server/master/cell_master/proto/multicell_node_statistics.pb.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TMulticellNodeStatistics
{
public:
    explicit TMulticellNodeStatistics(TBootstrap* bootstrap);

    const NProto::TCellStatistics& GetClusterStatistics() const;

    NProto::TCellStatistics GetCellStatistics(NObjectClient::TCellTag cellTag) const;
    i64 GetChunkCount(NObjectClient::TCellTag cellTag) const;

    // COMPAT(koloshmet)
    void PopulateLocalStatisticsAfterSnapshotLoaded(const NProto::TCellStatistics& statistics);

    // COMPAT(koloshmet)
    void PopulateClusterStatisticsAfterSnapshotLoaded(const NProto::TCellStatistics& statistics);

    // COMPAT(koloshmet)
    template <std::ranges::input_range TCellStatisticPairs>
        requires std::same_as<
            std::ranges::range_value_t<TCellStatisticPairs>,
            std::pair<NObjectClient::TCellTag, NProto::TCellStatistics>>
    void PopulateMasterCellStatisticsAfterSnapshotLoaded(TCellStatisticPairs&& statisticsPairs);

    // CMulticellStatisticsValue implementation.
    using TMutationRequestTypes = std::tuple<NProto::TReqSetCellStatistics, NProto::TReqSetMulticellStatistics>;

    void Clear();

    void Load(TLoadContext& context);

    void Save(TSaveContext& context) const;

    void HydraApplyMulticellStatisticsUpdate(NProto::TReqSetCellStatistics* request);

    void HydraApplyMulticellStatisticsUpdate(NProto::TReqSetMulticellStatistics* request);

    void FinishUpdate();

    TFuture<NProto::TReqSetCellStatistics> GetLocalCellUpdate();

    std::optional<TDuration> GetUpdatePeriod();

private:
    TBootstrap* const Bootstrap_;
    NProto::TCellStatistics LocalCellStatistics_;
    NProto::TCellStatistics ClusterCellStatisics_;
    std::map<NObjectClient::TCellTag, NProto::TCellStatistics> MasterCellStatistics_;

    NProto::TReqSetMulticellStatistics GetMulticellStatistics();

    NProto::TReqSetCellStatistics GetClusterCellStatistics();

    void RecomputeClusterCellStatistics();
};

static_assert(CMulticellStatisticsValue<TMulticellNodeStatistics>);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

#define MULTICELL_NODE_STATISTICS_INL_H_
#include "multicell_node_statistics-inl.h"
#undef MULTICELL_NODE_STATISTICS_INL_H_
