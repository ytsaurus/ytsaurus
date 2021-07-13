#include "legacy_master_connector.h"
#include "private.h"
#include "artifact.h"
#include "chunk_block_manager.h"
#include "chunk.h"
#include "chunk_store.h"
#include "config.h"
#include "location.h"
#include "master_connector.h"
#include "session_manager.h"
#include "network_statistics.h"

#include <yt/yt/server/node/cellar_node/bootstrap.h>
#include <yt/yt/server/node/cellar_node/master_connector.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/journal_dispatcher.h>

#include <yt/yt/server/node/job_agent/job_controller.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/chunk_cache.h>
#include <yt/yt/server/node/exec_node/master_connector.h>
#include <yt/yt/server/node/exec_node/slot_location.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/server/node/tablet_node/bootstrap.h>
#include <yt/yt/server/node/tablet_node/master_connector.h>
#include <yt/yt/server/node/tablet_node/slot_manager.h>
#include <yt/yt/server/node/tablet_node/tablet.h>
#include <yt/yt/server/node/tablet_node/tablet_slot.h>
#include <yt/yt/server/node/tablet_node/sorted_chunk_store.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/interop.h>
#include <yt/yt/ytlib/node_tracker_client/helpers.h>
#include <yt/yt/ytlib/node_tracker_client/node_statistics.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/string.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/build/build.h>

namespace NYT::NDataNode {

using namespace NYTree;
using namespace NElection;
using namespace NRpc;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NJobTrackerClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NTabletNodeTrackerClient::NProto;
using namespace NDataNodeTrackerClient::NProto;
using namespace NExecNodeTrackerClient::NProto;
using namespace NTabletNode;
using namespace NHydra;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NApi;
using namespace NClusterNode;
using namespace NYTree;

using NNodeTrackerClient::TAddressMap;
using NNodeTrackerClient::TNodeDescriptor;
using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TLegacyMasterConnector::TLegacyMasterConnector(
    TDataNodeConfigPtr config,
    const std::vector<TString>& nodeTags,
    NClusterNode::IBootstrap* bootstrap)
    : Config_(config)
    , NodeTags_(nodeTags)
    , Bootstrap_(bootstrap)
    , ControlInvoker_(bootstrap->GetControlInvoker())
    , IncrementalHeartbeatPeriod_(Config_->IncrementalHeartbeatPeriod)
    , IncrementalHeartbeatPeriodSplay_(Config_->IncrementalHeartbeatPeriodSplay)
{
    VERIFY_INVOKER_THREAD_AFFINITY(ControlInvoker_, ControlThread);
    YT_VERIFY(Config_);
    YT_VERIFY(Bootstrap_);
}

void TLegacyMasterConnector::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(!Started_);

    const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
    dynamicConfigManager->SubscribeConfigChanged(BIND(&TLegacyMasterConnector::OnDynamicConfigChanged, MakeWeak(this)));

    Started_ = true;
}

void TLegacyMasterConnector::ScheduleNodeHeartbeat(TCellTag cellTag, bool immedately)
{
    VERIFY_THREAD_AFFINITY_ANY();

    BIND(&TLegacyMasterConnector::DoScheduleNodeHeartbeat, MakeStrong(this), cellTag, immedately)
        .AsyncVia(ControlInvoker_)
        .Run();
}

void TLegacyMasterConnector::ScheduleNodeHeartbeat(bool immedately)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto cellTags = Bootstrap_->GetMasterCellTags();
    for (auto cellTag : cellTags) {
        ScheduleNodeHeartbeat(cellTag, immedately);
    }
}

void TLegacyMasterConnector::DoScheduleNodeHeartbeat(TCellTag cellTag, bool immedately)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto period = immedately
        ? TDuration::Zero()
        : IncrementalHeartbeatPeriod_ + RandomDuration(IncrementalHeartbeatPeriodSplay_);
    ++HeartbeatsScheduled_[cellTag];
    const auto& heartbeatInvoker = Bootstrap_->GetMasterConnectionInvoker();
    TDelayedExecutor::Submit(
        BIND(&TLegacyMasterConnector::ReportNodeHeartbeat, MakeStrong(this), cellTag)
            .Via(heartbeatInvoker),
        period);
}

void TLegacyMasterConnector::OnMasterConnected()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    for (auto cellTag : Bootstrap_->GetMasterCellTags()) {
        DoScheduleNodeHeartbeat(cellTag, true);
    }
}

void TLegacyMasterConnector::ReportNodeHeartbeat(TCellTag cellTag)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    --HeartbeatsScheduled_[cellTag];
    const auto& dataNodeMasterConnector = Bootstrap_
        ->GetDataNodeBootstrap()
        ->GetMasterConnector();
    auto state = dataNodeMasterConnector->GetMasterConnectorState(cellTag);
    switch (state) {
        case EMasterConnectorState::Registered:
            if (dataNodeMasterConnector->CanSendFullNodeHeartbeat(cellTag)) {
                ReportFullNodeHeartbeat(cellTag);
            } else {
                DoScheduleNodeHeartbeat(cellTag);
            }
            break;

        case EMasterConnectorState::Online:
            ReportIncrementalNodeHeartbeat(cellTag);
            break;

        case EMasterConnectorState::Offline:
            // Out of order heartbeat can be requested when node is offline.
            YT_LOG_WARNING("Heartbeat can't be sent because node is offline, retrying (CellTag: %v)",
                cellTag);
            DoScheduleNodeHeartbeat(cellTag);
            break;

        default:
            YT_ABORT();
    }
}

void TLegacyMasterConnector::ReportFullNodeHeartbeat(TCellTag cellTag)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto Logger = DataNodeLogger.WithTag("CellTag: %v", cellTag);

    auto channel = Bootstrap_->GetMasterChannel(cellTag);
    TNodeTrackerServiceProxy proxy(channel);

    auto request = proxy.FullHeartbeat();
    request->SetRequestCodec(NCompression::ECodec::Lz4);
    request->SetTimeout(Config_->FullHeartbeatTimeout);

    YT_VERIFY(Bootstrap_->IsConnected());
    request->set_node_id(Bootstrap_->GetNodeId());

    auto fullDataNodeHeartbeat = Bootstrap_
        ->GetDataNodeBootstrap()
        ->GetMasterConnector()
        ->GetFullHeartbeatRequest(cellTag);
    FillDataNodeHeartbeatPart(request.Get(), fullDataNodeHeartbeat);

    auto cellarNodeHeartbeat = Bootstrap_
        ->GetCellarNodeBootstrap()
        ->GetMasterConnector()
        ->GetHeartbeatRequest(cellTag);
    FillCellarNodeHeartbeatPart(request.Get(), cellarNodeHeartbeat);

    auto execNodeHeartbeat = Bootstrap_
        ->GetExecNodeBootstrap()
        ->GetMasterConnector()
        ->GetHeartbeatRequest();
    FillExecNodeHeartbeatPart(request.Get(), execNodeHeartbeat);

    auto clusterNodeHeartbeat = Bootstrap_->GetMasterConnector()->GetHeartbeatRequest();
    FillClusterNodeHeartbeatPart(request.Get(), clusterNodeHeartbeat);

    YT_LOG_INFO("Full node heartbeat sent to master (StoredChunkCount: %v, CachedChunkCount: %v, %v)",
        request->statistics().total_stored_chunk_count(),
        request->statistics().total_cached_chunk_count(),
        request->statistics());

    auto rspOrError = WaitFor(request->Invoke());

    if (!rspOrError.IsOK()) {
        YT_LOG_WARNING(rspOrError, "Error reporting full node heartbeat to master",
            cellTag);

        if (NRpc::IsRetriableError(rspOrError)) {
            DoScheduleNodeHeartbeat(cellTag);
        } else {
            Bootstrap_->ResetAndRegisterAtMaster();
        }
        return;
    }

    YT_LOG_INFO("Successfully reported full node heartbeat to master");

    {
        NDataNodeTrackerClient::NProto::TRspFullHeartbeat fullDataNodeHeartbeatResponse;
        FromFullHeartbeatResponse(&fullDataNodeHeartbeatResponse, *rspOrError.Value());
        Bootstrap_
            ->GetDataNodeBootstrap()
            ->GetMasterConnector()
            ->OnFullHeartbeatResponse(
                cellTag,
                fullDataNodeHeartbeatResponse);
    }

    DoScheduleNodeHeartbeat(cellTag);
}

void TLegacyMasterConnector::ReportIncrementalNodeHeartbeat(TCellTag cellTag)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TAsyncReaderWriterLock* heartbeatsLock;
    if (auto it = HeartbeatLocks_.find(cellTag); it != HeartbeatLocks_.end()) {
        heartbeatsLock = it->second.get();
    } else {
        auto lock = std::make_unique<TAsyncReaderWriterLock>();
        heartbeatsLock = lock.get();
        YT_VERIFY(HeartbeatLocks_.emplace(cellTag, std::move(lock)).second);
    }

    auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(heartbeatsLock))
        .ValueOrThrow();

    if (IncrementalHeartbeatThrottler_.find(cellTag) == IncrementalHeartbeatThrottler_.end()) {
        YT_VERIFY(IncrementalHeartbeatThrottler_.emplace(
            cellTag,
            CreateReconfigurableThroughputThrottler(Config_->IncrementalHeartbeatThrottler)).second);
    }

    WaitFor(IncrementalHeartbeatThrottler_[cellTag]->Throttle(1))
        .ThrowOnError();

    auto Logger = DataNodeLogger.WithTag("CellTag: %v", cellTag);

    auto primaryCellTag = CellTagFromId(Bootstrap_->GetCellId());

    const auto& masterConnector = Bootstrap_->GetMasterConnector();
    auto channel = masterConnector->GetMasterChannel(cellTag);
    TNodeTrackerServiceProxy proxy(channel);

    auto request = proxy.IncrementalHeartbeat();
    request->SetRequestCodec(NCompression::ECodec::Lz4);
    request->SetTimeout(Config_->IncrementalHeartbeatTimeout);

    YT_VERIFY(masterConnector->IsConnected());
    request->set_node_id(masterConnector->GetNodeId());

    auto incrementalDataNodeHeartbeat = Bootstrap_
        ->GetDataNodeBootstrap()
        ->GetMasterConnector()
        ->GetIncrementalHeartbeatRequest(cellTag);
    FillDataNodeHeartbeatPart(request.Get(), incrementalDataNodeHeartbeat);

    auto clusterNodeHeartbeat = masterConnector->GetHeartbeatRequest();
    FillClusterNodeHeartbeatPart(request.Get(), clusterNodeHeartbeat);

    auto cellarNodeHeartbeat = Bootstrap_
        ->GetCellarNodeBootstrap()
        ->GetMasterConnector()
        ->GetHeartbeatRequest(cellTag);
    FillCellarNodeHeartbeatPart(request.Get(), cellarNodeHeartbeat);

    auto tabletNodeHeartbeat = Bootstrap_
        ->GetTabletNodeBootstrap()
        ->GetMasterConnector()
        ->GetHeartbeatRequest(cellTag);
    FillTabletNodeHeartbeatPart(request.Get(), tabletNodeHeartbeat);

    auto execNodeHeartbeat = Bootstrap_
        ->GetExecNodeBootstrap()
        ->GetMasterConnector()
        ->GetHeartbeatRequest();
    FillExecNodeHeartbeatPart(request.Get(), execNodeHeartbeat);

    YT_LOG_INFO("Incremental node heartbeat sent to master (%v, AddedChunks: %v, RemovedChunks: %v)",
        request->statistics(),
        request->added_chunks_size(),
        request->removed_chunks_size());

    auto rspOrError = WaitFor(request->Invoke());
    if (!rspOrError.IsOK()) {
        Bootstrap_
            ->GetDataNodeBootstrap()
            ->GetMasterConnector()
            ->OnIncrementalHeartbeatFailed(cellTag);

        YT_LOG_WARNING(rspOrError, "Error reporting incremental node heartbeat to master");
        if (NRpc::IsRetriableError(rspOrError)) {
            DoScheduleNodeHeartbeat(cellTag);
        } else {
            Bootstrap_->ResetAndRegisterAtMaster();
        }
        return;
    }

    YT_LOG_INFO("Successfully reported incremental node heartbeat to master");

    const auto& rsp = rspOrError.Value();

    {
        NDataNodeTrackerClient::NProto::TRspIncrementalHeartbeat incrementalDataNodeHeartbeatResponse;
        FromIncrementalHeartbeatResponse(&incrementalDataNodeHeartbeatResponse, *rsp);
        Bootstrap_
            ->GetDataNodeBootstrap()
            ->GetMasterConnector()
            ->OnIncrementalHeartbeatResponse(cellTag, incrementalDataNodeHeartbeatResponse);
    }

    if (cellTag == primaryCellTag) {
        NNodeTrackerClient::NProto::TRspHeartbeat clusterNodeHeartbeatResponse;
        FromIncrementalHeartbeatResponse(&clusterNodeHeartbeatResponse, *rsp);
        Bootstrap_->GetMasterConnector()->OnHeartbeatResponse(clusterNodeHeartbeatResponse);

        NCellarNodeTrackerClient::NProto::TRspHeartbeat cellarNodeHeartbeatResponse;
        FromIncrementalHeartbeatResponse(&cellarNodeHeartbeatResponse, *rsp);
        Bootstrap_
            ->GetCellarNodeBootstrap()
            ->GetMasterConnector()
            ->OnHeartbeatResponse(cellarNodeHeartbeatResponse);

        NExecNodeTrackerClient::NProto::TRspHeartbeat execNodeHeartbeatResponse;
        FromIncrementalHeartbeatResponse(&execNodeHeartbeatResponse, *rsp);
        Bootstrap_
            ->GetExecNodeBootstrap()
            ->GetMasterConnector()
            ->OnHeartbeatResponse(execNodeHeartbeatResponse);
    }

    if (HeartbeatsScheduled_[cellTag] == 0) {
        DoScheduleNodeHeartbeat(cellTag);
    }
}

void TLegacyMasterConnector::Reset()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    HeartbeatsScheduled_.clear();
}

void TLegacyMasterConnector::OnDynamicConfigChanged(
    const TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
    const TClusterNodeDynamicConfigPtr& newNodeConfig)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    IncrementalHeartbeatPeriod_ = newNodeConfig->MasterConnector->IncrementalHeartbeatPeriod.value_or(Config_->IncrementalHeartbeatPeriod);
    IncrementalHeartbeatPeriodSplay_ = newNodeConfig->MasterConnector->IncrementalHeartbeatPeriodSplay.value_or(Config_->IncrementalHeartbeatPeriodSplay);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
