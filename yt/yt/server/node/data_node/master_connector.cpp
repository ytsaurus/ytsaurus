#include "master_connector.h"

#include "bootstrap.h"
#include "private.h"
#include "chunk.h"
#include "chunk_meta_manager.h"
#include "chunk_store.h"
#include "config.h"
#include "location.h"
#include "network_statistics.h"
#include "session_manager.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>

#include <yt/yt/server/node/data_node/journal_dispatcher.h>
#include <yt/yt/server/node/data_node/chunk_meta_manager.h>

#include <yt/yt/server/node/exec_agent/bootstrap.h>
#include <yt/yt/server/node/exec_agent/chunk_cache.h>

#include <yt/yt/server/node/job_agent/job_controller.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/data_node_tracker_client/data_node_tracker_service_proxy.h>

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NDataNodeTrackerClient;
using namespace NDataNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NObjectClient;
using namespace NNodeTrackerClient;

// TODO: Use `using NNodeTrackerClient::NProto` after legacy heartbeats removal.
using NNodeTrackerClient::NProto::TDataNodeStatistics;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public IMasterConnector
{
public:
    explicit TMasterConnector(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(bootstrap->GetConfig()->DataNode->MasterConnector)
        , IncrementalHeartbeatPeriod_(*Config_->IncrementalHeartbeatPeriod)
        , IncrementalHeartbeatPeriodSplay_(Config_->IncrementalHeartbeatPeriodSplay)
        , JobHeartbeatPeriod_(*Config_->JobHeartbeatPeriod)
        , JobHeartbeatPeriodSplay_(Config_->JobHeartbeatPeriodSplay)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
    }

    virtual void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (auto cellTag : Bootstrap_->GetMasterCellTags()) {
            auto cellTagData = std::make_unique<TPerCellTagData>();
            YT_VERIFY(PerCellTagData_.emplace(cellTag, std::move(cellTagData)).second);
        }

        Bootstrap_->SubscribeMasterConnected(BIND(&TMasterConnector::OnMasterConnected, MakeWeak(this)));
        Bootstrap_->SubscribeMasterDisconnected(BIND(&TMasterConnector::OnMasterDisconnected, MakeWeak(this)));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TMasterConnector::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& controlInvoker = Bootstrap_->GetControlInvoker();
        const auto& chunkStore = Bootstrap_->GetChunkStore();
        chunkStore->SubscribeChunkAdded(
            BIND(&TMasterConnector::OnChunkAdded, MakeWeak(this))
                .Via(controlInvoker));
        chunkStore->SubscribeChunkRemoved(
            BIND(&TMasterConnector::OnChunkRemoved, MakeWeak(this))
                .Via(controlInvoker));
        chunkStore->SubscribeChunkMediumChanged(
            BIND(&TMasterConnector::OnChunkMediumChanged, MakeWeak(this))
                .Via(controlInvoker));

        if (Bootstrap_->IsExecNode()) {
            const auto& chunkCache = Bootstrap_->GetExecNodeBootstrap()->GetChunkCache();
            chunkCache->SubscribeChunkAdded(
                BIND(&TMasterConnector::OnChunkAdded, MakeWeak(this))
                    .Via(controlInvoker));
            chunkCache->SubscribeChunkRemoved(
                BIND(&TMasterConnector::OnChunkRemoved, MakeWeak(this))
                    .Via(controlInvoker));
        }
    }

    virtual TReqFullHeartbeat GetFullHeartbeatRequest(TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(Bootstrap_->IsConnected());

        TReqFullHeartbeat heartbeat;

        heartbeat.set_node_id(Bootstrap_->GetNodeId());

        ComputeStatistics(heartbeat.mutable_statistics());

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        heartbeat.set_write_sessions_disabled(sessionManager->GetDisableWriteSessions());

        TMediumIntMap chunkCounts;

        int storedChunkCount = 0;
        int cachedChunkCount = 0;

        auto addStoredChunkInfo = [&] (const IChunkPtr& chunk) {
            if (CellTagFromId(chunk->GetId()) == cellTag) {
                auto info = BuildAddChunkInfo(chunk);
                *heartbeat.add_chunks() = info;
                auto mediumIndex = chunk->GetLocation()->GetMediumDescriptor().Index;
                ++chunkCounts[mediumIndex];
                ++storedChunkCount;
            }
        };

        auto addCachedChunkInfo = [&] (const IChunkPtr& chunk) {
            if (!IsArtifactChunkId(chunk->GetId())) {
                auto info = BuildAddChunkInfo(chunk);
                *heartbeat.add_chunks() = info;
                ++chunkCounts[DefaultCacheMediumIndex];
                ++cachedChunkCount;
            }
        };

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        for (const auto& chunk : chunkStore->GetChunks()) {
            addStoredChunkInfo(chunk);
        }

        if (Bootstrap_->IsExecNode()) {
            const auto& chunkCache = Bootstrap_->GetExecNodeBootstrap()->GetChunkCache();
            for (const auto& chunk : chunkCache->GetChunks()) {
                addCachedChunkInfo(chunk);
            }
        }

        for (const auto& [mediumIndex, chunkCount] : chunkCounts) {
            if (chunkCount != 0) {
                auto* mediumChunkStatistics = heartbeat.add_chunk_statistics();
                mediumChunkStatistics->set_medium_index(mediumIndex);
                mediumChunkStatistics->set_chunk_count(chunkCount);
            }
        }

        return heartbeat;
    }

    virtual TReqIncrementalHeartbeat GetIncrementalHeartbeatRequest(TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(Bootstrap_->IsConnected());

        TReqIncrementalHeartbeat heartbeat;

        heartbeat.set_node_id(Bootstrap_->GetNodeId());

        ComputeStatistics(heartbeat.mutable_statistics());

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        heartbeat.set_write_sessions_disabled(sessionManager->GetDisableWriteSessions());

        auto* delta = GetChunksDelta(cellTag);

        int chunkEventCount = 0;
        delta->ReportedAdded.clear();
        for (const auto& chunk : delta->AddedSinceLastSuccess) {
            YT_VERIFY(delta->ReportedAdded.emplace(chunk, chunk->GetVersion()).second);
            *heartbeat.add_added_chunks() = BuildAddChunkInfo(chunk);
            ++chunkEventCount;
        }

        delta->ReportedRemoved.clear();
        for (const auto& chunk : delta->RemovedSinceLastSuccess) {
            YT_VERIFY(delta->ReportedRemoved.insert(chunk).second);
            *heartbeat.add_removed_chunks() = BuildRemoveChunkInfo(chunk);
            ++chunkEventCount;
        }

        delta->ReportedChangedMedium.clear();
        for (const auto& [chunk, oldMediumIndex] : delta->ChangedMediumSinceLastSuccess) {
            if (chunkEventCount >= MaxChunkEventsPerIncrementalHeartbeat_) {
                auto mediumChangedBacklogCount = delta->ChangedMediumSinceLastSuccess.size() - delta->ReportedChangedMedium.size();
                YT_LOG_INFO("Chunk event limit per heartbeat is reached, will report %v chunks with medium changed in next heartbeats",
                    mediumChangedBacklogCount);
                break;
            }
            YT_VERIFY(delta->ReportedChangedMedium.insert({chunk, oldMediumIndex}).second);
            auto removeChunkInfo = BuildRemoveChunkInfo(chunk);
            removeChunkInfo.set_medium_index(oldMediumIndex);
            *heartbeat.add_removed_chunks() = removeChunkInfo;
            ++chunkEventCount;
            *heartbeat.add_added_chunks() = BuildAddChunkInfo(chunk);
            ++chunkEventCount;
        }


        delta->CurrentHeartbeatBarrier = delta->NextHeartbeatBarrier.Exchange(NewPromise<void>());

        return heartbeat;
    }

    virtual void OnFullHeartbeatReported(TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* delta = GetChunksDelta(cellTag);
        delta->State = EMasterConnectorState::Online;
        YT_VERIFY(delta->AddedSinceLastSuccess.empty());
        YT_VERIFY(delta->RemovedSinceLastSuccess.empty());
        YT_VERIFY(delta->ChangedMediumSinceLastSuccess.empty());
    }

    virtual void OnIncrementalHeartbeatFailed(TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* delta = GetChunksDelta(cellTag);

        auto currentHeartbeatFuture = delta->CurrentHeartbeatBarrier.ToFuture();
        auto nextHeartbeatBarrier = delta->NextHeartbeatBarrier.Exchange(std::move(delta->CurrentHeartbeatBarrier));
        nextHeartbeatBarrier.SetFrom(currentHeartbeatFuture);
    }

    virtual void OnIncrementalHeartbeatResponse(
        TCellTag cellTag,
        const TRspIncrementalHeartbeat& response) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* delta = GetChunksDelta(cellTag);

        delta->CurrentHeartbeatBarrier.Set();

        {
            auto it = delta->AddedSinceLastSuccess.begin();
            while (it != delta->AddedSinceLastSuccess.end()) {
                auto jt = it++;
                auto chunk = *jt;
                auto kt = delta->ReportedAdded.find(chunk);
                if (kt != delta->ReportedAdded.end() && kt->second == chunk->GetVersion()) {
                    delta->AddedSinceLastSuccess.erase(jt);
                }
            }
            delta->ReportedAdded.clear();
        }

        {
            auto it = delta->RemovedSinceLastSuccess.begin();
            while (it != delta->RemovedSinceLastSuccess.end()) {
                auto jt = it++;
                auto chunk = *jt;
                auto kt = delta->ReportedRemoved.find(chunk);
                if (kt != delta->ReportedRemoved.end()) {
                    delta->RemovedSinceLastSuccess.erase(jt);
                }
            }
            delta->ReportedRemoved.clear();
        }

        {
            auto it = delta->ChangedMediumSinceLastSuccess.begin();
            while (it != delta->ChangedMediumSinceLastSuccess.end()) {
                auto jt = it++;
                auto chunkAndOldMediumIndex = *jt;
                auto kt = delta->ReportedChangedMedium.find(chunkAndOldMediumIndex);
                if (kt != delta->ReportedChangedMedium.end()) {
                    delta->ReportedChangedMedium.erase(chunkAndOldMediumIndex);
                }
            }
            delta->ReportedChangedMedium.clear();
        }


        if (cellTag == PrimaryMasterCellTag) {
            const auto& sessionManager = Bootstrap_->GetSessionManager();
            sessionManager->SetDisableWriteSessions(response.disable_write_sessions() || Bootstrap_->Decommissioned());
        }
    }

    virtual EMasterConnectorState GetMasterConnectorState(TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* delta = GetChunksDelta(cellTag);
        return delta->State;
    }

    virtual bool CanSendFullNodeHeartbeat(TCellTag cellTag) const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& connection = Bootstrap_->GetMasterClient()->GetNativeConnection();
        if (cellTag != connection->GetPrimaryMasterCellTag()) {
            return true;
        }

        for (const auto& [cellTag, cellTagData] : PerCellTagData_) {
            const auto& chunksDelta = cellTagData->ChunksDelta;
            if (cellTag != connection->GetPrimaryMasterCellTag() && chunksDelta->State != EMasterConnectorState::Online) {
                return false;
            }
        }
        return true;
    }

    virtual TFuture<void> GetHeartbeatBarrier(NObjectClient::TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetChunksDelta(cellTag)->NextHeartbeatBarrier.Load();
    }

    virtual void ScheduleHeartbeat(bool immediately) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& controlInvoker = Bootstrap_->GetControlInvoker();
        const auto& masterCellTags = Bootstrap_->GetMasterCellTags();
        for (auto cellTag : masterCellTags) {
            controlInvoker->Invoke(
                BIND(&TMasterConnector::DoScheduleHeartbeat, MakeWeak(this), cellTag, immediately));
        }
    }

private:
    struct TChunksDelta
    {
        //! Synchronization state.
        EMasterConnectorState State = EMasterConnectorState::Offline;

        //! Chunks that were added since the last successful heartbeat.
        THashSet<IChunkPtr> AddedSinceLastSuccess;

        //! Chunks that were removed since the last successful heartbeat.
        THashSet<IChunkPtr> RemovedSinceLastSuccess;

        //! Chunks that changed medium since the last successful heartbeat and their old medium.
        THashSet<std::pair<IChunkPtr, int>> ChangedMediumSinceLastSuccess;

        //! Maps chunks that were reported added at the last heartbeat (for which no reply is received yet) to their versions.
        THashMap<IChunkPtr, int> ReportedAdded;

        //! Chunks that were reported removed at the last heartbeat (for which no reply is received yet).
        THashSet<IChunkPtr> ReportedRemoved;

        //! Chunks that were reported changed medium at the last heartbeat (for which no reply is received yet) and their old medium.
        THashSet<std::pair<IChunkPtr, int>> ReportedChangedMedium;

        //! Set when another incremental heartbeat is successfully reported to the corresponding master.
        TAtomicObject<TPromise<void>> NextHeartbeatBarrier = NewPromise<void>();

        //! Set when current heartbeat is successfully reported.
        TPromise<void> CurrentHeartbeatBarrier;
    };

    struct TPerCellTagData
    {
        std::unique_ptr<TChunksDelta> ChunksDelta = std::make_unique<TChunksDelta>();

        TAsyncReaderWriterLock DataNodeHeartbeatLock;
        int ScheduledDataNodeHeartbeatCount = 0;
    };
    THashMap<TCellTag, std::unique_ptr<TPerCellTagData>> PerCellTagData_;

    IBootstrap* const Bootstrap_;

    const TMasterConnectorConfigPtr Config_;

    int JobHeartbeatCellIndex_ = 0;

    IInvokerPtr HeartbeatInvoker_;

    TDuration IncrementalHeartbeatPeriod_;
    TDuration IncrementalHeartbeatPeriodSplay_;

    TDuration JobHeartbeatPeriod_;
    TDuration JobHeartbeatPeriodSplay_;
    i64 MaxChunkEventsPerIncrementalHeartbeat_;


    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& masterCellTags = Bootstrap_->GetMasterCellTags();
        for (auto cellTag : masterCellTags) {
            auto* delta = GetChunksDelta(cellTag);
            delta->State = EMasterConnectorState::Offline;
            delta->ReportedAdded.clear();
            delta->ReportedRemoved.clear();
            delta->AddedSinceLastSuccess.clear();
            delta->RemovedSinceLastSuccess.clear();

            auto* cellTagData = GetCellTagData(cellTag);
            cellTagData->ScheduledDataNodeHeartbeatCount = 0;
        }

        JobHeartbeatCellIndex_ = 0;
    }

    void OnMasterConnected(TNodeId /*nodeId*/)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        HeartbeatInvoker_ = Bootstrap_->GetMasterConnectionInvoker();

        const auto& masterCellTags = Bootstrap_->GetMasterCellTags();
        for (auto cellTag : masterCellTags) {
            auto* delta = GetChunksDelta(cellTag);
            delta->State = EMasterConnectorState::Registered;
        }

        if (Bootstrap_->UseNewHeartbeats()) {
            StartHeartbeats();
        }
        // Job heartbeats are sent using data node master connector in both old and new protocols.
        // TODO(gritukan): Move it to StartHeartbeats.
        ScheduleJobHeartbeat(/* immediately */ true);
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& dynamicConfig = newNodeConfig->DataNode->MasterConnector;
        IncrementalHeartbeatPeriod_ = dynamicConfig->IncrementalHeartbeatPeriod.value_or(*Config_->IncrementalHeartbeatPeriod);
        IncrementalHeartbeatPeriodSplay_ = dynamicConfig->IncrementalHeartbeatPeriodSplay.value_or(Config_->IncrementalHeartbeatPeriodSplay);
        JobHeartbeatPeriod_ = dynamicConfig->JobHeartbeatPeriod.value_or(*Config_->JobHeartbeatPeriod);
        JobHeartbeatPeriodSplay_ = dynamicConfig->JobHeartbeatPeriodSplay.value_or(Config_->JobHeartbeatPeriodSplay);
        MaxChunkEventsPerIncrementalHeartbeat_ = dynamicConfig->MaxChunkEventsPerIncrementalHeartbeat;
    }

    void StartHeartbeats()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Starting data node and job heartbeats");

        const auto& masterCellTags = Bootstrap_->GetMasterCellTags();
        for (auto cellTag : masterCellTags) {
            DoScheduleHeartbeat(cellTag, /* immediately */ true);
        }
    }

    void DoScheduleHeartbeat(TCellTag cellTag, bool immediately)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* cellTagData = GetCellTagData(cellTag);
        ++cellTagData->ScheduledDataNodeHeartbeatCount;

        auto delay = immediately ? TDuration::Zero() : IncrementalHeartbeatPeriod_ + RandomDuration(IncrementalHeartbeatPeriodSplay_);
        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::ReportHeartbeat, MakeWeak(this), cellTag),
            delay,
            HeartbeatInvoker_);
    }

    void ScheduleJobHeartbeat(bool immediately)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto delay = immediately ? TDuration::Zero() : JobHeartbeatPeriod_ + RandomDuration(JobHeartbeatPeriodSplay_);
        delay /= (Bootstrap_->GetMasterClient()->GetNativeConnection()->GetSecondaryMasterCellTags().size() + 1);
        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::ReportJobHeartbeat, MakeWeak(this)),
            delay,
            HeartbeatInvoker_);
    }

    void ReportJobHeartbeat()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& masterCellTags = Bootstrap_->GetMasterCellTags();
        auto cellTag = masterCellTags[JobHeartbeatCellIndex_];

        auto state = GetMasterConnectorState(cellTag);
        if (state == EMasterConnectorState::Online) {
            auto channel = Bootstrap_->GetMasterChannel(cellTag);
            TJobTrackerServiceProxy proxy(channel);

            auto req = proxy.Heartbeat();
            req->SetTimeout(Config_->JobHeartbeatTimeout);

            const auto& jobController = Bootstrap_->GetJobController();
            YT_VERIFY(WaitFor(jobController->PrepareHeartbeatRequest(cellTag, EObjectType::MasterJob, req))
                .IsOK());

            YT_LOG_INFO("Job heartbeat sent to master (ResourceUsage: %v, CellTag: %v)",
                FormatResourceUsage(req->resource_usage(), req->resource_limits()),
                cellTag);

            auto rspOrError = WaitFor(req->Invoke());

            if (rspOrError.IsOK()) {
                YT_LOG_INFO("Successfully reported job heartbeat to master (CellTag: %v)",
                    cellTag);

                const auto& rsp = rspOrError.Value();
                YT_VERIFY(WaitFor(jobController->ProcessHeartbeatResponse(rsp, EObjectType::MasterJob))
                    .IsOK());
            } else {
                YT_LOG_WARNING(rspOrError, "Error reporting job heartbeat to master (CellTag: %v)",
                    cellTag);
                if (NRpc::IsRetriableError(rspOrError)) {
                    ScheduleJobHeartbeat(/* immediately */ false);
                } else {
                    Bootstrap_->ResetAndRegisterAtMaster();
                }
                return;
            }
        }

        JobHeartbeatCellIndex_ = (JobHeartbeatCellIndex_ + 1) % masterCellTags.size();

        ScheduleJobHeartbeat(/* immediately */ false);
    }

    void ReportHeartbeat(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* cellTagData = GetCellTagData(cellTag);

        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&cellTagData->DataNodeHeartbeatLock))
            .ValueOrThrow();

        --cellTagData->ScheduledDataNodeHeartbeatCount;

        auto state = GetMasterConnectorState(cellTag);
        switch (state) {
            case EMasterConnectorState::Registered: {
                if (CanSendFullNodeHeartbeat(cellTag)) {
                    ReportFullHeartbeat(cellTag);
                } else {
                    // Try later.
                    DoScheduleHeartbeat(cellTag, /*immediately*/ false);
                }
                break;
            }

            case EMasterConnectorState::Online: {
                ReportIncrementalHeartbeat(cellTag);
                break;
            }

            default:
                YT_ABORT();
        }
    }

    void ReportFullHeartbeat(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto masterChannel = Bootstrap_->GetMasterChannel(cellTag);
        TDataNodeTrackerServiceProxy proxy(masterChannel);

        auto req = proxy.FullHeartbeat();
        req->SetTimeout(*Config_->FullHeartbeatTimeout);

        static_cast<TReqFullHeartbeat&>(*req) = GetFullHeartbeatRequest(cellTag);

        YT_LOG_INFO("Sending full data node heartbeat to master (CellTag: %v, %v)",
            cellTag,
            req->statistics());

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            OnFullHeartbeatReported(cellTag);

            YT_LOG_INFO("Successfully reported full data node heartbeat to master (CellTag: %v)",
                cellTag);

            // Schedule next heartbeat.
            DoScheduleHeartbeat(cellTag, /*immediately*/ false);
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting full data node heartbeat to master (CellTag: %v)",
                cellTag);
            if (IsRetriableError(rspOrError)) {
                DoScheduleHeartbeat(cellTag, /* immediately*/ false);
            } else {
                Bootstrap_->ResetAndRegisterAtMaster();
            }
        }
    }

    void ReportIncrementalHeartbeat(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto masterChannel = Bootstrap_->GetMasterChannel(cellTag);
        TDataNodeTrackerServiceProxy proxy(masterChannel);

        auto req = proxy.IncrementalHeartbeat();
        req->SetTimeout(*Config_->IncrementalHeartbeatTimeout);

        static_cast<TReqIncrementalHeartbeat&>(*req) = GetIncrementalHeartbeatRequest(cellTag);

        YT_LOG_INFO("Sending incremental data node heartbeat to master (CellTag: %v, %v)",
            cellTag,
            req->statistics());

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            OnIncrementalHeartbeatResponse(cellTag, *rspOrError.Value());

            YT_LOG_INFO("Successfully reported incremental data node heartbeat to master (CellTag: %v)",
                cellTag);

            // Schedule next heartbeat if no more heartbeats are scheduled.
            auto* cellTagData = GetCellTagData(cellTag);
            if (cellTagData->ScheduledDataNodeHeartbeatCount == 0) {
                DoScheduleHeartbeat(cellTag, /*immediately*/ false);
            }
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting incremental data node heartbeat to master (CellTag: %v)",
                cellTag);
            if (IsRetriableError(rspOrError)) {
                DoScheduleHeartbeat(cellTag, /*immediately*/ false);
            } else {
                Bootstrap_->ResetAndRegisterAtMaster();
            }
        }
    }

    void ComputeStatistics(TDataNodeStatistics* statistics) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        i64 totalAvailableSpace = 0;
        i64 totalLowWatermarkSpace = 0;
        i64 totalUsedSpace = 0;
        int totalStoredChunkCount = 0;
        int totalSessionCount = 0;
        bool full = true;

        THashMap<int, int> mediumIndexToIOWeight;

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        for (const auto& location : chunkStore->Locations()) {
            if (location->IsEnabled()) {
                totalAvailableSpace += location->GetAvailableSpace();
                totalLowWatermarkSpace += location->GetLowWatermarkSpace();
                full &= location->IsFull();
            }

            totalUsedSpace += location->GetUsedSpace();
            totalStoredChunkCount += location->GetChunkCount();
            totalSessionCount += location->GetSessionCount();

            auto* locationStatistics = statistics->add_storage_locations();

            auto mediumIndex = location->GetMediumDescriptor().Index;
            locationStatistics->set_medium_index(mediumIndex);
            locationStatistics->set_available_space(location->GetAvailableSpace());
            locationStatistics->set_used_space(location->GetUsedSpace());
            locationStatistics->set_low_watermark_space(location->GetLowWatermarkSpace());
            locationStatistics->set_chunk_count(location->GetChunkCount());
            locationStatistics->set_session_count(location->GetSessionCount());
            locationStatistics->set_enabled(location->IsEnabled());
            locationStatistics->set_full(location->IsFull());
            locationStatistics->set_throttling_reads(location->IsReadThrottling());
            locationStatistics->set_throttling_writes(location->IsWriteThrottling());
            locationStatistics->set_sick(location->IsSick());
            ToProto(locationStatistics->mutable_location_uuid(), location->GetUuid());
            locationStatistics->set_disk_family(location->GetDiskFamily());

            if (IsLocationWriteable(location)) {
                ++mediumIndexToIOWeight[mediumIndex];
            }
        }

        for (const auto& [mediumIndex, ioWeight] : mediumIndexToIOWeight) {
            auto* protoStatistics = statistics->add_media();
            protoStatistics->set_medium_index(mediumIndex);
            protoStatistics->set_io_weight(ioWeight);
        }

        int totalCachedChunkCount = 0;
        if (Bootstrap_->IsExecNode()) {
            const auto& chunkCache = Bootstrap_->GetExecNodeBootstrap()->GetChunkCache();
            totalCachedChunkCount = chunkCache->GetChunkCount();
        }

        statistics->set_total_available_space(totalAvailableSpace);
        statistics->set_total_low_watermark_space(totalLowWatermarkSpace);
        statistics->set_total_used_space(totalUsedSpace);
        statistics->set_total_stored_chunk_count(totalStoredChunkCount);
        statistics->set_total_cached_chunk_count(totalCachedChunkCount);
        statistics->set_full(full);

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        statistics->set_total_user_session_count(sessionManager->GetSessionCount(ESessionType::User));
        statistics->set_total_replication_session_count(sessionManager->GetSessionCount(ESessionType::Replication));
        statistics->set_total_repair_session_count(sessionManager->GetSessionCount(ESessionType::Repair));
    }

    bool IsLocationWriteable(const TStoreLocationPtr& location) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& config = Bootstrap_->GetConfig()->DataNode;

        if (!location->IsEnabled()) {
            return false;
        }

        if (location->IsFull()) {
            return false;
        }

        if (location->IsSick()) {
            return false;
        }

        if (location->GetMaxPendingIOSize(EIODirection::Write) > config->DiskWriteThrottlingLimit) {
            return false;
        }

        if (Bootstrap_->IsReadOnly()) {
            return false;
        }

        return true;
    }

    TChunkAddInfo BuildAddChunkInfo(IChunkPtr chunk)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TChunkAddInfo chunkAddInfo;

        ToProto(chunkAddInfo.mutable_chunk_id(), chunk->GetId());
        chunkAddInfo.set_medium_index(chunk->GetLocation()->GetMediumDescriptor().Index);
        chunkAddInfo.set_active(chunk->IsActive());
        chunkAddInfo.set_sealed(chunk->GetInfo().sealed());

        return chunkAddInfo;
    }

    TChunkRemoveInfo BuildRemoveChunkInfo(IChunkPtr chunk)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TChunkRemoveInfo chunkRemoveInfo;

        ToProto(chunkRemoveInfo.mutable_chunk_id(), chunk->GetId());
        chunkRemoveInfo.set_medium_index(chunk->GetLocation()->GetMediumDescriptor().Index);

        return chunkRemoveInfo;
    }

    TPerCellTagData* GetCellTagData(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetOrCrash(PerCellTagData_, cellTag).get();
    }

    TChunksDelta* GetChunksDelta(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto* cellTagData = GetCellTagData(cellTag);
        return cellTagData->ChunksDelta.get();
    }

    TChunksDelta* GetChunksDelta(TObjectId id)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetChunksDelta(CellTagFromId(id));
    }

    void OnChunkAdded(const IChunkPtr& chunk)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (IsArtifactChunkId(chunk->GetId())) {
            return;
        }

        auto* delta = GetChunksDelta(chunk->GetId());
        if (delta->State != EMasterConnectorState::Online) {
            return;
        }

        delta->RemovedSinceLastSuccess.erase(chunk);
        delta->AddedSinceLastSuccess.insert(chunk);

        YT_LOG_DEBUG("Chunk addition registered (ChunkId: %v, LocationId: %v)",
            chunk->GetId(),
            chunk->GetLocation()->GetId());
    }

    void OnChunkRemoved(const IChunkPtr& chunk)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (IsArtifactChunkId(chunk->GetId())) {
            return;
        }

        auto* delta = GetChunksDelta(chunk->GetId());
        if (delta->State != EMasterConnectorState::Online) {
            return;
        }

        delta->AddedSinceLastSuccess.erase(chunk);
        delta->RemovedSinceLastSuccess.insert(chunk);

        Bootstrap_->GetChunkMetaManager()->GetBlockMetaCache()->TryRemove(chunk->GetId());

        YT_LOG_DEBUG("Chunk removal registered (ChunkId: %v, LocationId: %v)",
            chunk->GetId(),
            chunk->GetLocation()->GetId());
    }

    void OnChunkMediumChanged(const IChunkPtr& chunk, int mediumIndex)
    {
        auto* delta = GetChunksDelta(chunk->GetId());
        if (delta->State != EMasterConnectorState::Online) {
            return;
        }
        delta->ChangedMediumSinceLastSuccess.emplace(chunk, mediumIndex);
    }

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap)
{
    return New<TMasterConnector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
