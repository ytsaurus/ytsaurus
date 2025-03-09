#include "multicell_node_statistics.h"

#include "cell_statistics.h"
#include "config.h"
#include "config_manager.h"
#include "private.h"

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto& Logger = CellMasterLogger;

}

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using NObjectClient::TCellTag;

////////////////////////////////////////////////////////////////////////////////

TMulticellNodeStatistics::TMulticellNodeStatistics(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

const NProto::TCellStatistics& TMulticellNodeStatistics::GetClusterStatistics() const
{
    Bootstrap_->VerifyPersistentStateRead();

    return ClusterCellStatisics_;
}

NProto::TCellStatistics TMulticellNodeStatistics::GetCellStatistics(NObjectClient::TCellTag cell) const
{
    return GetOrDefault(MasterCellStatistics_, cell);
}

// COMPAT(koloshmet)
void TMulticellNodeStatistics::PopulateLocalStatisticsAfterSnapshotLoaded(
    const NProto::TCellStatistics& statistics)
{
    LocalCellStatistics_ = statistics;
}

// COMPAT(koloshmet)
void TMulticellNodeStatistics::PopulateClusterStatisticsAfterSnapshotLoaded(
    const NProto::TCellStatistics& statistics)
{
    ClusterCellStatisics_ = statistics;
}

void TMulticellNodeStatistics::Clear()
{
    LocalCellStatistics_ = {};
    ClusterCellStatisics_ = {};
    MasterCellStatistics_.clear();
}

void TMulticellNodeStatistics::Load(TLoadContext& context)
{
    using NYT::Load;
    if (context.GetVersion() >= EMasterReign::MulticellStatisticsCollector) {
        Load(context, LocalCellStatistics_);
        Load(context, ClusterCellStatisics_);
        Load(context, MasterCellStatistics_);
    }
}

void TMulticellNodeStatistics::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, LocalCellStatistics_);
    Save(context, ClusterCellStatisics_);
    Save(context, MasterCellStatistics_);
}

void TMulticellNodeStatistics::HydraApplyMulticellStatisticsUpdate(NProto::TReqSetCellStatistics* request)
{
    YT_VERIFY(NHydra::HasMutationContext());

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (multicellManager->IsPrimaryMaster()) {
        auto cellTag = FromProto<TCellTag>(request->cell_tag());

        if (cellTag == multicellManager->GetPrimaryCellTag()) {
            YT_LOG_INFO("Persisted primary cell statistics (%v)",
                request->statistics());

            LocalCellStatistics_ = request->statistics();

            RecomputeClusterCellStatistics();
        } else {
            YT_LOG_INFO("Received cell statistics gossip message (CellTag: %v, %v)",
                cellTag,
                request->statistics());

            if (multicellManager->IsRegisteredMasterCell(cellTag)) {
                MasterCellStatistics_[cellTag] = request->statistics();
            }
        }
    } else {
        YT_LOG_INFO("Received cell statistics gossip message (%v)",
            request->statistics());
        ClusterCellStatisics_ = request->statistics();
    }
}

void TMulticellNodeStatistics::HydraApplyMulticellStatisticsUpdate(NProto::TReqSetMulticellStatistics* request)
{
    YT_VERIFY(NHydra::HasMutationContext());

    const auto& multicellManager = Bootstrap_->GetMulticellManager();

    YT_VERIFY(multicellManager->IsSecondaryMaster());

    auto cellRoles = multicellManager->GetMasterCellRoles(multicellManager->GetCellTag());
    if (None(cellRoles & EMasterCellRoles::CypressNodeHost)) {
        // NB: alerting this cell is not having the 'Cypress node host' role would
        // probably be a bit too fragile.
        YT_LOG_INFO(
            "Received node multicell statistics but cell doesn't have %Qlv role", EMasterCellRoles::CypressNodeHost);
    }

    YT_LOG_INFO("Received multicell statistics gossip message (%v)",
        request->statistics());

    for (const auto& cellStatistics : request->statistics()) {
        auto cellTag = FromProto<TCellTag>(cellStatistics.cell_tag());
        if (cellTag == multicellManager->GetCellTag()) {
            // No point in overwriting local statistics - they're persisted
            // periodically anyway.
            continue;
        }
        // Registering every secondary cell at every secondary cell may happen too late.
        if (multicellManager->IsRegisteredMasterCell(cellTag)) {
            MasterCellStatistics_[cellTag] = cellStatistics.statistics();
        }
    }

    RecomputeClusterCellStatistics();
}

void TMulticellNodeStatistics::FinishUpdate()
{
    // Send statistics to secondary cells.
    const auto& multicellManager = Bootstrap_->GetMulticellManager();

    auto allCellTags = multicellManager->GetRegisteredMasterCellTags();
    std::ranges::sort(allCellTags);

    auto portalCellTags = multicellManager->GetRoleMasterCells(EMasterCellRole::CypressNodeHost);
    YT_VERIFY(std::ranges::is_sorted(portalCellTags));

    NObjectClient::TCellTagList nonPortalCellTags;
    std::ranges::set_difference(allCellTags, portalCellTags, std::back_inserter(nonPortalCellTags));

    if (!portalCellTags.empty()) {
        auto multicellRequest = GetMulticellStatistics();
        multicellManager->PostToMasters(multicellRequest, portalCellTags, /*reliable*/ false);
    }

    if (!nonPortalCellTags.empty()) {
        auto clusterRequest = GetClusterCellStatistics();
        multicellManager->PostToMasters(clusterRequest, nonPortalCellTags, /*reliable*/ false);
    }
}

TFuture<NProto::TReqSetCellStatistics> TMulticellNodeStatistics::GetLocalCellUpdate()
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    auto makeReqSetCellStatistics = [this] (TErrorOr<i64>&& lostVitalChunkCountOrError) {
        // NB: Never an error
        auto lostVitalChunkCount = std::move(lostVitalChunkCountOrError).Value();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        NProto::TReqSetCellStatistics result;
        result.set_cell_tag(ToProto(multicellManager->GetCellTag()));
        auto* cellStatistics = result.mutable_statistics();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        cellStatistics->set_chunk_count(chunkManager->Chunks().GetSize());

        cellStatistics->set_lost_vital_chunk_count(lostVitalChunkCount);

        if (multicellManager->IsPrimaryMaster()) {
            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            cellStatistics->set_online_node_count(nodeTracker->GetOnlineNodeCount());
        }

        return result;
    };

    return chunkManager->GetCellLostVitalChunkCount().ApplyUnique(BIND(std::move(makeReqSetCellStatistics)));
}

std::optional<TDuration> TMulticellNodeStatistics::GetUpdatePeriod()
{
    const auto& dynamicConfig = Bootstrap_->GetConfigManager()->GetConfig()->MulticellManager;
    return dynamicConfig->CellStatisticsGossipPeriod;
}

NProto::TReqSetMulticellStatistics TMulticellNodeStatistics::GetMulticellStatistics()
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsPrimaryMaster());

    NProto::TReqSetMulticellStatistics result;

    auto addCellStatistics = [&] (TCellTag cellTag, const NProto::TCellStatistics& statistics) {
        auto* cellStatistics = result.add_statistics();
        cellStatistics->set_cell_tag(ToProto(cellTag));
        *cellStatistics->mutable_statistics() = statistics;
    };

    addCellStatistics(multicellManager->GetCellTag(), LocalCellStatistics_);
    for (auto& [cellTag, statistics] : MasterCellStatistics_) {
        addCellStatistics(cellTag, statistics);
    }

    return result;
}

NProto::TReqSetCellStatistics TMulticellNodeStatistics::GetClusterCellStatistics()
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsPrimaryMaster());

    NProto::TReqSetCellStatistics result;
    result.set_cell_tag(ToProto(multicellManager->GetCellTag()));
    *result.mutable_statistics() = ClusterCellStatisics_;

    return result;
}

void TMulticellNodeStatistics::RecomputeClusterCellStatistics()
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();

    ClusterCellStatisics_ = {};

    auto addCellStatistics = [&] (TCellTag cellTag, const NProto::TCellStatistics& statistics) {
        YT_ASSERT(statistics.online_node_count() == 0 || cellTag == multicellManager->GetPrimaryCellTag());

        ClusterCellStatisics_ += statistics;
        // TODO(shakurov): consider moving this into operator+.
        ClusterCellStatisics_.set_online_node_count(
            ClusterCellStatisics_.online_node_count() + statistics.online_node_count());
    };

    addCellStatistics(multicellManager->GetCellTag(), LocalCellStatistics_);
    for (const auto& [cellTag, statistics] : MasterCellStatistics_) {
        addCellStatistics(cellTag, statistics);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
