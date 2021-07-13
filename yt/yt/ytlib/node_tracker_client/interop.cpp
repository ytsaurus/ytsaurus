#include "interop.h"

#include <yt/yt/ytlib/cellar_client/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NNodeTrackerClient {

using namespace NCellarClient;

////////////////////////////////////////////////////////////////////////////////

void FillExecNodeStatistics(
    NProto::TNodeStatistics* nodeStatistics,
    const NProto::TExecNodeStatistics& execNodeStatistics)
{
    nodeStatistics->mutable_slot_locations()->CopyFrom(execNodeStatistics.slot_locations());
}

void FromNodeStatistics(
    NProto::TExecNodeStatistics* execNodeStatistics,
    const NProto::TNodeStatistics& nodeStatistics)
{
    execNodeStatistics->mutable_slot_locations()->CopyFrom(nodeStatistics.slot_locations());
}

void FromIncrementalHeartbeatRequest(
    NExecNodeTrackerClient::NProto::TReqHeartbeat* execNodeHeartbeat,
    const NProto::TReqIncrementalHeartbeat& incrementalHeartbeat)
{
    FromNodeStatistics(execNodeHeartbeat->mutable_statistics(), incrementalHeartbeat.statistics());
}

void FromFullHeartbeatRequest(
    NExecNodeTrackerClient::NProto::TReqHeartbeat* execNodeHeartbeat,
    const NProto::TReqFullHeartbeat& fullHeartbeat)
{
    FromNodeStatistics(execNodeHeartbeat->mutable_statistics(), fullHeartbeat.statistics());
}

void FillExecNodeHeartbeatPart(
    NProto::TReqIncrementalHeartbeat* incrementalHeartbeat,
    const NExecNodeTrackerClient::NProto::TReqHeartbeat& execNodeHeartbeat)
{
    FillExecNodeStatistics(incrementalHeartbeat->mutable_statistics(), execNodeHeartbeat.statistics());
}

void FillExecNodeHeartbeatPart(
    NProto::TReqFullHeartbeat* fullHeartbeat,
    const NExecNodeTrackerClient::NProto::TReqHeartbeat& execNodeHeartbeat)
{
    FillExecNodeStatistics(fullHeartbeat->mutable_statistics(), execNodeHeartbeat.statistics());
}

void FromIncrementalHeartbeatResponse(
    NExecNodeTrackerClient::NProto::TRspHeartbeat* execNodeHeartbeatResponse,
    const NProto::TRspIncrementalHeartbeat& incrementalHeartbeatResponse)
{
    execNodeHeartbeatResponse->set_disable_scheduler_jobs(incrementalHeartbeatResponse.disable_scheduler_jobs());
}

void FillIncrementalHeartbeatResponse(
    NProto::TRspIncrementalHeartbeat* incrementalHeartbeatResponse,
    const NExecNodeTrackerClient::NProto::TRspHeartbeat& execNodeHeartbeatResponse)
{
    incrementalHeartbeatResponse->set_disable_scheduler_jobs(execNodeHeartbeatResponse.disable_scheduler_jobs());
}

////////////////////////////////////////////////////////////////////////////////

void FillCellarNodeStatistics(
    NProto::TNodeStatistics* nodeStatistics,
    const NProto::TCellarNodeStatistics& cellarNodeStatistics)
{
    nodeStatistics->set_available_tablet_slots(cellarNodeStatistics.available_cell_slots());
    nodeStatistics->set_used_tablet_slots(cellarNodeStatistics.used_cell_slots());
}

void FromNodeStatistics(
    NProto::TCellarNodeStatistics* cellarNodeStatistics,
    const NProto::TNodeStatistics& nodeStatistics)
{
    cellarNodeStatistics->set_available_cell_slots(nodeStatistics.available_tablet_slots());
    cellarNodeStatistics->set_used_cell_slots(nodeStatistics.used_tablet_slots());
}

void FromIncrementalHeartbeatRequest(
    NCellarNodeTrackerClient::NProto::TReqHeartbeat* cellarNodeHeartbeat,
    const NProto::TReqIncrementalHeartbeat& incrementalHeartbeat)
{
    auto* cellarHeartbeat = cellarNodeHeartbeat->add_cellars();
    cellarHeartbeat->set_type(ToProto<int>(ECellarType::Tablet));

    FromNodeStatistics(cellarHeartbeat->mutable_statistics(), incrementalHeartbeat.statistics());

    cellarHeartbeat->mutable_cell_slots()->CopyFrom(incrementalHeartbeat.tablet_slots());
}

void FromFullHeartbeatRequest(
    NCellarNodeTrackerClient::NProto::TReqHeartbeat* cellarNodeHeartbeat,
    const NProto::TReqFullHeartbeat& fullHeartbeat)
{
    auto* cellarHeartbeat = cellarNodeHeartbeat->add_cellars();
    cellarHeartbeat->set_type(ToProto<int>(ECellarType::Tablet));

    FromNodeStatistics(cellarHeartbeat->mutable_statistics(), fullHeartbeat.statistics());
}

void FillCellarNodeHeartbeatPart(
    NProto::TReqIncrementalHeartbeat* incrementalHeartbeat,
    const NCellarNodeTrackerClient::NProto::TReqHeartbeat& cellarNodeHeartbeat)
{
    YT_VERIFY(cellarNodeHeartbeat.cellars_size() == 1);
    const auto& cellarHeartbeat = cellarNodeHeartbeat.cellars(0);

    FillCellarNodeStatistics(incrementalHeartbeat->mutable_statistics(), cellarHeartbeat.statistics());

    incrementalHeartbeat->mutable_tablet_slots()->CopyFrom(cellarHeartbeat.cell_slots());
}

void FillCellarNodeHeartbeatPart(
    NProto::TReqFullHeartbeat* fullHeartbeat,
    const NCellarNodeTrackerClient::NProto::TReqHeartbeat& cellarNodeHeartbeat)
{
    YT_VERIFY(cellarNodeHeartbeat.cellars_size() == 1);
    const auto& cellarHeartbeat = cellarNodeHeartbeat.cellars(0);

    FillCellarNodeStatistics(fullHeartbeat->mutable_statistics(), cellarHeartbeat.statistics());
}

void FromIncrementalHeartbeatResponse(
    NCellarNodeTrackerClient::NProto::TRspHeartbeat* cellarNodeHeartbeatResponse,
    const NProto::TRspIncrementalHeartbeat& incrementalHeartbeatResponse)
{
    auto* cellarHeartbeatResponse = cellarNodeHeartbeatResponse->add_cellars();
    cellarHeartbeatResponse->set_type(ToProto<int>(ECellarType::Tablet));

    cellarHeartbeatResponse->mutable_slots_to_create()->CopyFrom(incrementalHeartbeatResponse.tablet_slots_to_create());
    cellarHeartbeatResponse->mutable_slots_to_remove()->CopyFrom(incrementalHeartbeatResponse.tablet_slots_to_remove());
    cellarHeartbeatResponse->mutable_slots_to_configure()->CopyFrom(incrementalHeartbeatResponse.tablet_slots_configure());
    cellarHeartbeatResponse->mutable_slots_to_update()->CopyFrom(incrementalHeartbeatResponse.tablet_slots_update());
}

void FillIncrementalHeartbeatResponse(
    NProto::TRspIncrementalHeartbeat* incrementalHeartbeatResponse,
    const NCellarNodeTrackerClient::NProto::TRspHeartbeat& cellarNodeHeartbeatResponse)
{
    YT_VERIFY(cellarNodeHeartbeatResponse.cellars_size() == 1);
    const auto& cellarHeartbeatResponse = cellarNodeHeartbeatResponse.cellars(0);

    incrementalHeartbeatResponse->mutable_tablet_slots_to_create()->CopyFrom(cellarHeartbeatResponse.slots_to_create());
    incrementalHeartbeatResponse->mutable_tablet_slots_to_remove()->CopyFrom(cellarHeartbeatResponse.slots_to_remove());
    incrementalHeartbeatResponse->mutable_tablet_slots_configure()->CopyFrom(cellarHeartbeatResponse.slots_to_configure());
    incrementalHeartbeatResponse->mutable_tablet_slots_update()->CopyFrom(cellarHeartbeatResponse.slots_to_update());
}

////////////////////////////////////////////////////////////////////////////////

void FromIncrementalHeartbeatRequest(
    NTabletNodeTrackerClient::NProto::TReqHeartbeat* tabletNodeHeartbeat,
    const NProto::TReqIncrementalHeartbeat& incrementalHeartbeat)
{
    tabletNodeHeartbeat->mutable_tablets()->CopyFrom(incrementalHeartbeat.tablets());
}

void FillTabletNodeHeartbeatPart(
    NProto::TReqIncrementalHeartbeat* incrementalHeartbeat,
    const NTabletNodeTrackerClient::NProto::TReqHeartbeat& tabletNodeHeartbeat)
{
    incrementalHeartbeat->mutable_tablets()->CopyFrom(tabletNodeHeartbeat.tablets());
}

////////////////////////////////////////////////////////////////////////////////

void FillDataNodeStatistics(
    NProto::TNodeStatistics* nodeStatistics,
    const NProto::TDataNodeStatistics& dataNodeStatistics)
{
    nodeStatistics->set_total_available_space(dataNodeStatistics.total_available_space());
    nodeStatistics->set_total_used_space(dataNodeStatistics.total_used_space());
    nodeStatistics->set_total_stored_chunk_count(dataNodeStatistics.total_stored_chunk_count());
    nodeStatistics->set_total_cached_chunk_count(dataNodeStatistics.total_cached_chunk_count());
    nodeStatistics->set_total_user_session_count(dataNodeStatistics.total_user_session_count());
    nodeStatistics->set_total_replication_session_count(dataNodeStatistics.total_replication_session_count());
    nodeStatistics->set_total_repair_session_count(dataNodeStatistics.total_repair_session_count());
    nodeStatistics->set_total_low_watermark_space(dataNodeStatistics.total_low_watermark_space());
    nodeStatistics->set_full(dataNodeStatistics.full());
    nodeStatistics->mutable_storage_locations()->CopyFrom(dataNodeStatistics.storage_locations());
    nodeStatistics->mutable_media()->CopyFrom(dataNodeStatistics.media());
}

void FromNodeStatistics(
    NProto::TDataNodeStatistics* dataNodeStatistics,
    const NProto::TNodeStatistics& nodeStatistics)
{
    dataNodeStatistics->set_total_available_space(nodeStatistics.total_available_space());
    dataNodeStatistics->set_total_used_space(nodeStatistics.total_used_space());
    dataNodeStatistics->set_total_stored_chunk_count(nodeStatistics.total_stored_chunk_count());
    dataNodeStatistics->set_total_cached_chunk_count(nodeStatistics.total_cached_chunk_count());
    dataNodeStatistics->set_total_user_session_count(nodeStatistics.total_user_session_count());
    dataNodeStatistics->set_total_replication_session_count(nodeStatistics.total_replication_session_count());
    dataNodeStatistics->set_total_repair_session_count(nodeStatistics.total_repair_session_count());
    dataNodeStatistics->set_total_low_watermark_space(nodeStatistics.total_low_watermark_space());
    dataNodeStatistics->set_full(nodeStatistics.full());
    dataNodeStatistics->mutable_storage_locations()->CopyFrom(nodeStatistics.storage_locations());
    dataNodeStatistics->mutable_media()->CopyFrom(nodeStatistics.media());
}

void FromFullHeartbeatRequest(
    NDataNodeTrackerClient::NProto::TReqFullHeartbeat* fullDataNodeHeartbeat,
    const NProto::TReqFullHeartbeat& fullHeartbeat)
{
    fullDataNodeHeartbeat->set_node_id(fullHeartbeat.node_id());
    FromNodeStatistics(fullDataNodeHeartbeat->mutable_statistics(), fullHeartbeat.statistics());
    fullDataNodeHeartbeat->mutable_chunk_statistics()->CopyFrom(fullHeartbeat.chunk_statistics());
    fullDataNodeHeartbeat->mutable_chunks()->CopyFrom(fullHeartbeat.chunks());
    fullDataNodeHeartbeat->set_write_sessions_disabled(fullHeartbeat.write_sessions_disabled());
}

void FromIncrementalHeartbeatRequest(
    NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat* incrementalDataNodeHeartbeat,
    const NProto::TReqIncrementalHeartbeat& incrementalHeartbeat)
{
    incrementalDataNodeHeartbeat->set_node_id(incrementalHeartbeat.node_id());
    FromNodeStatistics(incrementalDataNodeHeartbeat->mutable_statistics(), incrementalHeartbeat.statistics());
    incrementalDataNodeHeartbeat->mutable_added_chunks()->CopyFrom(incrementalHeartbeat.added_chunks());
    incrementalDataNodeHeartbeat->mutable_removed_chunks()->CopyFrom(incrementalHeartbeat.removed_chunks());
    incrementalDataNodeHeartbeat->set_write_sessions_disabled(incrementalHeartbeat.write_sessions_disabled());
    incrementalDataNodeHeartbeat->mutable_confirmed_replica_announcement_requests()->CopyFrom(incrementalHeartbeat.confirmed_replica_announcement_requests());
}

void FillDataNodeHeartbeatPart(
    NProto::TReqIncrementalHeartbeat* incrementalHeartbeat,
    const NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat& incrementalDataNodeHeartbeat)
{
    FillDataNodeStatistics(incrementalHeartbeat->mutable_statistics(), incrementalDataNodeHeartbeat.statistics());
    incrementalHeartbeat->mutable_added_chunks()->CopyFrom(incrementalDataNodeHeartbeat.added_chunks());
    incrementalHeartbeat->mutable_removed_chunks()->CopyFrom(incrementalDataNodeHeartbeat.removed_chunks());
    incrementalHeartbeat->set_write_sessions_disabled(incrementalDataNodeHeartbeat.write_sessions_disabled());
    incrementalHeartbeat->mutable_confirmed_replica_announcement_requests()->CopyFrom(incrementalDataNodeHeartbeat.confirmed_replica_announcement_requests());
}

void FillDataNodeHeartbeatPart(
    NProto::TReqFullHeartbeat* fullHeartbeat,
    const NDataNodeTrackerClient::NProto::TReqFullHeartbeat& fullDataNodeHeartbeat)
{
    FillDataNodeStatistics(fullHeartbeat->mutable_statistics(), fullDataNodeHeartbeat.statistics());
    fullHeartbeat->mutable_chunk_statistics()->CopyFrom(fullDataNodeHeartbeat.chunk_statistics());
    fullHeartbeat->mutable_chunks()->CopyFrom(fullDataNodeHeartbeat.chunks());
    fullHeartbeat->set_write_sessions_disabled(fullDataNodeHeartbeat.write_sessions_disabled());
}

void FromFullHeartbeatResponse(
    NDataNodeTrackerClient::NProto::TRspFullHeartbeat* fullDataNodeHeartbeatResponse,
    const NProto::TRspFullHeartbeat& fullHeartbeatResponse)
{
    fullDataNodeHeartbeatResponse->set_revision(fullHeartbeatResponse.revision());
    if (fullHeartbeatResponse.has_enable_lazy_replica_announcements()) {
        fullDataNodeHeartbeatResponse->set_enable_lazy_replica_announcements(fullHeartbeatResponse.enable_lazy_replica_announcements());
    }
    fullDataNodeHeartbeatResponse->mutable_replica_announcement_requests()->CopyFrom(fullHeartbeatResponse.replica_announcement_requests());
}

void FillFullHeartbeatResponse(
    NProto::TRspFullHeartbeat* fullHeartbeatResponse,
    const NDataNodeTrackerClient::NProto::TRspFullHeartbeat& fullDataNodeHeartbeatResponse)
{
    fullHeartbeatResponse->set_revision(fullDataNodeHeartbeatResponse.revision());
    if (fullDataNodeHeartbeatResponse.has_enable_lazy_replica_announcements()) {
        fullHeartbeatResponse->set_enable_lazy_replica_announcements(fullDataNodeHeartbeatResponse.enable_lazy_replica_announcements());
    }
    fullHeartbeatResponse->mutable_replica_announcement_requests()->CopyFrom(fullDataNodeHeartbeatResponse.replica_announcement_requests());
}

void FromIncrementalHeartbeatResponse(
    NDataNodeTrackerClient::NProto::TRspIncrementalHeartbeat* incrementalDataNodeHeartbeatResponse,
    const NProto::TRspIncrementalHeartbeat& incrementalHeartbeatResponse)
{
    incrementalDataNodeHeartbeatResponse->set_disable_write_sessions(incrementalHeartbeatResponse.disable_write_sessions());
    incrementalDataNodeHeartbeatResponse->set_revision(incrementalHeartbeatResponse.revision());
    if (incrementalHeartbeatResponse.has_enable_lazy_replica_announcements()) {
        incrementalDataNodeHeartbeatResponse->set_enable_lazy_replica_announcements(incrementalHeartbeatResponse.enable_lazy_replica_announcements());
    }
    incrementalDataNodeHeartbeatResponse->mutable_replica_announcement_requests()->CopyFrom(incrementalHeartbeatResponse.replica_announcement_requests());
}

void FillIncrementalHeartbeatResponse(
    NProto::TRspIncrementalHeartbeat* incrementalHeartbeatResponse,
    const NDataNodeTrackerClient::NProto::TRspIncrementalHeartbeat& incrementalDataNodeHeartbeatResponse)
{
    incrementalHeartbeatResponse->set_disable_write_sessions(incrementalDataNodeHeartbeatResponse.disable_write_sessions());
    incrementalHeartbeatResponse->set_revision(incrementalDataNodeHeartbeatResponse.revision());
    if (incrementalDataNodeHeartbeatResponse.has_enable_lazy_replica_announcements()) {
        incrementalHeartbeatResponse->set_enable_lazy_replica_announcements(incrementalDataNodeHeartbeatResponse.enable_lazy_replica_announcements());
    }
    incrementalHeartbeatResponse->mutable_replica_announcement_requests()->CopyFrom(incrementalDataNodeHeartbeatResponse.replica_announcement_requests());
}

////////////////////////////////////////////////////////////////////////////////

void FillClusterNodeStatistics(
    NProto::TNodeStatistics* nodeStatistics,
    const NProto::TClusterNodeStatistics& clusterNodeStatistics)
{
    nodeStatistics->mutable_memory()->CopyFrom(clusterNodeStatistics.memory());
    nodeStatistics->mutable_network()->CopyFrom(clusterNodeStatistics.network());
}

void FromNodeStatistics(
    NProto::TClusterNodeStatistics* clusterNodeStatistics,
    const NProto::TNodeStatistics& nodeStatistics)
{
    clusterNodeStatistics->mutable_memory()->CopyFrom(nodeStatistics.memory());
    clusterNodeStatistics->mutable_network()->CopyFrom(nodeStatistics.network());
}

void FillClusterNodeHeartbeatPart(
    NProto::TReqIncrementalHeartbeat* incrementalHeartbeat,
    const NProto::TReqHeartbeat& clusterNodeHeartbeat)
{
    FillClusterNodeStatistics(incrementalHeartbeat->mutable_statistics(), clusterNodeHeartbeat.statistics());
    incrementalHeartbeat->mutable_alerts()->CopyFrom(clusterNodeHeartbeat.alerts());
}

void FillClusterNodeHeartbeatPart(
    NProto::TReqFullHeartbeat* fullHeartbeat,
    const NProto::TReqHeartbeat& clusterNodeHeartbeat)
{
    FillClusterNodeStatistics(fullHeartbeat->mutable_statistics(), clusterNodeHeartbeat.statistics());
}

void FromIncrementalHeartbeatRequest(
    NProto::TReqHeartbeat* clusterNodeHeartbeat,
    const NProto::TReqIncrementalHeartbeat& incrementalHeartbeat)
{
    FromNodeStatistics(clusterNodeHeartbeat->mutable_statistics(), incrementalHeartbeat.statistics());
    clusterNodeHeartbeat->mutable_alerts()->CopyFrom(incrementalHeartbeat.alerts());
}

void FromFullHeartbeatRequest(
    NProto::TReqHeartbeat* clusterNodeHeartbeat,
    const NProto::TReqFullHeartbeat& fullHeartbeat)
{
    FromNodeStatistics(clusterNodeHeartbeat->mutable_statistics(), fullHeartbeat.statistics());
}

void FromIncrementalHeartbeatResponse(
    NProto::TRspHeartbeat* clusterNodeHeartbeatResponse,
    const NProto::TRspIncrementalHeartbeat& incrementalHeartbeatResponse)
{
    clusterNodeHeartbeatResponse->set_rack(incrementalHeartbeatResponse.rack());
    clusterNodeHeartbeatResponse->set_data_center(incrementalHeartbeatResponse.data_center());
    clusterNodeHeartbeatResponse->mutable_tags()->CopyFrom(incrementalHeartbeatResponse.tags());
    clusterNodeHeartbeatResponse->mutable_resource_limits_overrides()->CopyFrom(incrementalHeartbeatResponse.resource_limits_overrides());
    clusterNodeHeartbeatResponse->set_decommissioned(incrementalHeartbeatResponse.decommissioned());
}

void FillIncrementalHeartbeatResponse(
    NProto::TRspIncrementalHeartbeat* incrementalHeartbeatResponse,
    const NProto::TRspHeartbeat& clusterNodeHeartbeatResponse)
{
    incrementalHeartbeatResponse->set_rack(clusterNodeHeartbeatResponse.rack());
    incrementalHeartbeatResponse->set_data_center(clusterNodeHeartbeatResponse.data_center());
    incrementalHeartbeatResponse->mutable_tags()->CopyFrom(clusterNodeHeartbeatResponse.tags());
    incrementalHeartbeatResponse->mutable_resource_limits_overrides()->CopyFrom(clusterNodeHeartbeatResponse.resource_limits_overrides());
    incrementalHeartbeatResponse->set_decommissioned(clusterNodeHeartbeatResponse.decommissioned());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
