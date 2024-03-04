#include "master_connector.h"

#include "bootstrap.h"
#include "private.h"
#include "ally_replica_manager.h"
#include "chunk.h"
#include "chunk_meta_manager.h"
#include "chunk_store.h"
#include "config.h"
#include "job_controller.h"
#include "location.h"
#include "network_statistics.h"
#include "session_manager.h"
#include "io_throughput_meter.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>

#include <yt/yt/server/node/data_node/journal_dispatcher.h>
#include <yt/yt/server/node/data_node/chunk_meta_manager.h>
#include <yt/yt/server/node/data_node/medium_directory_manager.h>
#include <yt/yt/server/node/data_node/medium_updater.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/chunk_cache.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/data_node_tracker_client/data_node_tracker_service_proxy.h>
#include <yt/yt/ytlib/data_node_tracker_client/location_directory.h>

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/server/lib/chunk_server/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <util/random/shuffle.h>

namespace NYT::NDataNode {

using namespace NCellMasterClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NChunkServer;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NDataNodeTrackerClient;
using namespace NDataNodeTrackerClient::NProto;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    TIOStatistics* protoStatistics,
    const TStoreLocation::TIOStatistics& statistics,
    const IIOThroughputMeter::TIOCapacity& capacity);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterConnectorState,
    // Not registered.
    (Offline)
    // Registered but did not report the full heartbeat yet.
    (Registered)
    // Registered and reported the full heartbeat.
    (Online)
);

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

        DataNodeProfiler.AddFuncGauge("/online", MakeStrong(this), [this] {
            return IsOnline() ? 1.0 : 0.0;
        });
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LocationUuidsRequired_ = true;

        for (auto cellTag : Bootstrap_->GetMasterCellTags()) {
            auto cellId = Bootstrap_->GetConnection()->GetMasterCellId(cellTag);

            auto cellTagData = std::make_unique<TPerCellTagData>();
            EmplaceOrCrash(PerCellTagData_, cellTag, std::move(cellTagData));

            for (const auto& jobTrackerAddress : Bootstrap_->GetMasterAddressesOrThrow(cellTag)) {
                auto jobTrackerData = std::make_unique<TPerJobTrackerData>();
                jobTrackerData->CellTag = cellTag;

                const auto& channelFactory = Bootstrap_->GetConnection()->GetChannelFactory();
                auto channel = channelFactory->CreateChannel(jobTrackerAddress);
                jobTrackerData->Channel = CreateRealmChannel(std::move(channel), cellId);

                EmplaceOrCrash(PerJobTrackerData_, jobTrackerAddress, std::move(jobTrackerData));

                JobTrackerAddresses_.push_back(jobTrackerAddress);
            }
        }

        Shuffle(JobTrackerAddresses_.begin(), JobTrackerAddresses_.end());

        Bootstrap_->SubscribeMasterConnected(BIND(&TMasterConnector::OnMasterConnected, MakeWeak(this)));
        Bootstrap_->SubscribeMasterDisconnected(BIND(&TMasterConnector::OnMasterDisconnected, MakeWeak(this)));

        const auto& connection = Bootstrap_->GetClient()->GetNativeConnection();
        connection->GetMasterCellDirectory()->SubscribeCellDirectoryChanged(
            BIND(&TMasterConnector::OnMasterCellDirectoryChanged, MakeStrong(this))
                .Via(Bootstrap_->GetControlInvoker()));

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

        Initialized_ = true;
    }

    TDataNodeTrackerServiceProxy::TReqFullHeartbeatPtr BuildFullHeartbeatRequest(
        TNodeId nodeId,
        TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto masterChannel = Bootstrap_->GetMasterChannel(cellTag);
        TDataNodeTrackerServiceProxy proxy(masterChannel);

        auto req = proxy.FullHeartbeat();
        req->SetRequestCodec(NCompression::ECodec::Lz4);
        req->SetTimeout(GetDynamicConfig()->FullHeartbeatTimeout);

        req->set_node_id(ToProto<ui32>(nodeId));

        ComputeStatistics(req->mutable_statistics());

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        req->set_write_sessions_disabled(sessionManager->GetDisableWriteSessions());

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        TChunkLocationDirectory locationDirectory(chunkStore->Locations().size());

        TMediumMap<int> perMediumChunkCounts;
        THashMap<TChunkLocationUuid, int> perLocationChunkCounts;

        int storedChunkCount = 0;

        auto addStoredChunkInfo = [&] (const IChunkPtr& chunk) {
            const auto& location = chunk->GetLocation();
            if (location->CanPublish() && CellTagFromId(chunk->GetId()) == cellTag) {
                *req->add_chunks() = BuildAddChunkInfo(chunk, &locationDirectory);
                auto mediumIndex = chunk->GetLocation()->GetMediumDescriptor().Index;
                ++perMediumChunkCounts[mediumIndex];
                ++perLocationChunkCounts[location->GetUuid()];
                ++storedChunkCount;
            }
        };

        for (const auto& chunk : chunkStore->GetChunks()) {
            addStoredChunkInfo(chunk);
        }

        for (const auto& [mediumIndex, chunkCount] : perMediumChunkCounts) {
            if (chunkCount != 0) {
                auto* mediumChunkCount = req->add_per_medium_chunk_counts();
                mediumChunkCount->set_medium_index(mediumIndex);
                mediumChunkCount->set_chunk_count(chunkCount);
            }
        }

        for (const auto& [locationUuid, chunkCount] : perLocationChunkCounts) {
            if (chunkCount != 0) {
                auto* locationChunkCount = req->add_per_location_chunk_counts();
                ToProto(locationChunkCount->mutable_location_uuid(), locationUuid);
                locationChunkCount->set_chunk_count(chunkCount);
            }
        }

        ToProto(req->mutable_location_directory(), locationDirectory);

        return req;
    }

    TDataNodeTrackerServiceProxy::TReqIncrementalHeartbeatPtr BuildIncrementalHeartbeatRequest(
        TNodeId nodeId,
        TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto masterChannel = Bootstrap_->GetMasterChannel(cellTag);
        TDataNodeTrackerServiceProxy proxy(masterChannel);

        auto req = proxy.IncrementalHeartbeat();
        req->SetRequestCodec(NCompression::ECodec::Lz4);
        req->SetTimeout(GetDynamicConfig()->IncrementalHeartbeatTimeout);

        req->set_node_id(ToProto<ui32>(nodeId));
        req->set_sequence_number(SequenceNumber_);
        ++SequenceNumber_;

        ComputeStatistics(req->mutable_statistics());

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        req->set_write_sessions_disabled(sessionManager->GetDisableWriteSessions());

        auto* delta = GetChunksDelta(cellTag);

        TChunkLocationDirectory locationDirectory(Bootstrap_->GetChunkStore()->Locations().size());

        int chunkEventCount = 0;
        delta->ReportedAdded.clear();
        for (const auto& chunk : delta->AddedSinceLastSuccess) {
            if (!chunk->GetLocation()->CanPublish()) {
                continue;
            }

            YT_VERIFY(delta->ReportedAdded.emplace(chunk, chunk->GetVersion()).second);
            *req->add_added_chunks() = BuildAddChunkInfo(chunk, &locationDirectory);
            ++chunkEventCount;
        }

        delta->ReportedRemoved.clear();
        for (const auto& chunk : delta->RemovedSinceLastSuccess) {
            if (!chunk->GetLocation()->CanPublish()) {
                continue;
            }

            YT_VERIFY(delta->ReportedRemoved.insert(chunk).second);
            *req->add_removed_chunks() = BuildRemoveChunkInfo(chunk, &locationDirectory);
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

            if (!chunk->GetLocation()->CanPublish()) {
                continue;
            }

            YT_VERIFY(delta->ReportedChangedMedium.insert({chunk, oldMediumIndex}).second);
            auto removeChunkInfo = BuildRemoveChunkInfo(chunk, &locationDirectory, /*onMediumChange*/ true);
            removeChunkInfo.set_medium_index(oldMediumIndex);
            *req->add_removed_chunks() = removeChunkInfo;
            ++chunkEventCount;
            *req->add_added_chunks() = BuildAddChunkInfo(chunk, &locationDirectory, /*onMediumChange*/ true);
            ++chunkEventCount;
        }

        delta->CurrentHeartbeatBarrier = delta->NextHeartbeatBarrier.Exchange(NewPromise<void>());

        const auto& allyReplicaManager = Bootstrap_->GetAllyReplicaManager();
        auto unconfirmedAnnouncementRequests = allyReplicaManager->TakeUnconfirmedAnnouncementRequests(cellTag);
        for (auto [chunkId, revision] : unconfirmedAnnouncementRequests) {
            auto* protoRequest = req->add_confirmed_replica_announcement_requests();
            ToProto(protoRequest->mutable_chunk_id(), chunkId);
            protoRequest->set_revision(revision);
        }

        if (EnableIncrementalHeartbeatProfiling_) {
            const auto& counters = GetIncrementalHeartbeatCounters(cellTag);

            counters.Reported.AddedChunks.Increment(delta->ReportedAdded.size());
            counters.Reported.RemovedChunks.Increment(delta->ReportedRemoved.size());
            counters.Reported.MediumChangedChunks.Increment(delta->ReportedChangedMedium.size());
            counters.ConfirmedAnnouncementRequests.Increment(req->confirmed_replica_announcement_requests().size());
        }

        ToProto(req->mutable_location_directory(), locationDirectory);

        return req;
    }

    void OnFullHeartbeatResponse(
        TCellTag cellTag,
        const TRspFullHeartbeat& response)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* delta = GetChunksDelta(cellTag);
        YT_VERIFY(delta->State == EMasterConnectorState::Registered);

        delta->State = EMasterConnectorState::Online;
        YT_VERIFY(delta->AddedSinceLastSuccess.empty());
        YT_VERIFY(delta->RemovedSinceLastSuccess.empty());
        YT_VERIFY(delta->ChangedMediumSinceLastSuccess.empty());

        const auto& allyReplicaManager = Bootstrap_->GetAllyReplicaManager();
        // COMPAT(aleksandra-zh)
        if (!response.replica_announcement_requests().empty()) {
            YT_VERIFY(response.has_revision());
            allyReplicaManager->ScheduleAnnouncements(
                MakeRange(response.replica_announcement_requests()),
                response.revision(),
                /*onFullHeartbeat*/ true);
        }
        if (response.has_enable_lazy_replica_announcements()) {
            allyReplicaManager->SetEnableLazyAnnouncements(response.enable_lazy_replica_announcements());
        }

        HandleReplicaAnnouncements(allyReplicaManager, response, /*onFullHeartbeat*/ true);

        OnlineCellCount_ += 1;

        const auto& connection = Bootstrap_->GetConnection();
        if (cellTag == connection->GetPrimaryMasterCellTag()) {
            ProcessHeartbeatResponseMediaInfo(response);
        }
    }

    void OnIncrementalHeartbeatFailed(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* delta = GetChunksDelta(cellTag);

        auto currentHeartbeatFuture = delta->CurrentHeartbeatBarrier.ToFuture();
        auto nextHeartbeatBarrier = delta->NextHeartbeatBarrier.Exchange(std::move(delta->CurrentHeartbeatBarrier));
        nextHeartbeatBarrier.SetFrom(currentHeartbeatFuture);

        if (EnableIncrementalHeartbeatProfiling_) {
            const auto& counters = GetIncrementalHeartbeatCounters(cellTag);

            counters.FailedToReport.AddedChunks.Increment(delta->ReportedAdded.size());
            counters.FailedToReport.RemovedChunks.Increment(delta->ReportedRemoved.size());
            counters.FailedToReport.MediumChangedChunks.Increment(delta->ReportedChangedMedium.size());
        }
    }

    void OnIncrementalHeartbeatResponse(
        TCellTag cellTag,
        const TRspIncrementalHeartbeat& response)
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
                    delta->ChangedMediumSinceLastSuccess.erase(chunkAndOldMediumIndex);
                }
            }
            delta->ReportedChangedMedium.clear();
        }

        const auto& allyReplicaManager = Bootstrap_->GetAllyReplicaManager();
        // COMPAT(aleksandra-zh)
        if (!response.replica_announcement_requests().empty()) {
            YT_VERIFY(response.has_revision());
            allyReplicaManager->ScheduleAnnouncements(
                MakeRange(response.replica_announcement_requests()),
                response.revision(),
                /*onFullHeartbeat*/ false);
        }
        if (response.has_enable_lazy_replica_announcements()) {
            allyReplicaManager->SetEnableLazyAnnouncements(response.enable_lazy_replica_announcements());
        }
        HandleReplicaAnnouncements(allyReplicaManager, response, /*onFullHeartbeat*/ false);

        const auto& connection = Bootstrap_->GetConnection();
        if (cellTag == connection->GetPrimaryMasterCellTag()) {
            ProcessHeartbeatResponseMediaInfo(response);

            const auto& sessionManager = Bootstrap_->GetSessionManager();
            sessionManager->SetDisableWriteSessions(response.disable_write_sessions() || Bootstrap_->IsDecommissioned());
        }
    }

    EMasterConnectorState GetMasterConnectorState(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* delta = GetChunksDelta(cellTag);
        return delta->State;
    }

    TFuture<void> GetHeartbeatBarrier(NObjectClient::TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetChunksDelta(cellTag)->NextHeartbeatBarrier.Load().ToFuture().ToUncancelable();
    }

    void ScheduleHeartbeat(bool immediately) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& controlInvoker = Bootstrap_->GetControlInvoker();
        const auto& masterCellTags = Bootstrap_->GetMasterCellTags();
        for (auto cellTag : masterCellTags) {
            controlInvoker->Invoke(BIND(
                &TMasterConnector::DoScheduleHeartbeat,
                MakeWeak(this),
                cellTag,
                immediately,
                /*outOfOrder*/ true));
        }
    }

    void ScheduleJobHeartbeat(const TString& jobTrackerAddress) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Scheduling out-of-order job heartbeat "
            "(JobTrackerAddress: %v)",
            jobTrackerAddress);

        const auto& controlInvoker = Bootstrap_->GetControlInvoker();
        controlInvoker->Invoke(BIND(
            &TMasterConnector::ReportJobHeartbeat,
            MakeWeak(this),
            jobTrackerAddress,
            /*outOfOrder*/ true));
    }

    bool IsOnline() const override
    {
        return OnlineCellCount_.load() == std::ssize(Bootstrap_->GetMasterCellTags());
    }

    void SetLocationUuidsRequired(bool value) override
    {
        LocationUuidsRequired_ = value;
        YT_LOG_INFO("Location uuids in data node heartbeats are %v",
            value ? "enabled" : "disabled");
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

    struct TPerJobTrackerData
    {
        //! Tag of the cell job tracker belongs to.
        TCellTag CellTag;

        //! Channel to job tracker.
        NRpc::IChannelPtr Channel;

        //! Prevents concurrent job heartbeats.
        TAsyncReaderWriterLock JobHeartbeatLock;
    };
    THashMap<TString, std::unique_ptr<TPerJobTrackerData>> PerJobTrackerData_;

    IBootstrap* const Bootstrap_;

    const TMasterConnectorConfigPtr Config_;

    std::vector<TString> JobTrackerAddresses_;
    int JobHeartbeatJobTrackerIndex_ = 0;

    IInvokerPtr HeartbeatInvoker_;

    using THeartbeatSequenceNumber = i64;
    THeartbeatSequenceNumber SequenceNumber_ = 0;

    TDuration IncrementalHeartbeatPeriod_;
    TDuration IncrementalHeartbeatPeriodSplay_;

    TDuration JobHeartbeatPeriod_;
    TDuration JobHeartbeatPeriodSplay_;
    i64 MaxChunkEventsPerIncrementalHeartbeat_;
    bool EnableIncrementalHeartbeatProfiling_ = false;

    std::atomic<int> OnlineCellCount_ = 0;

    bool Initialized_ = false;

    struct TIncrementalHeartbeatCounters
    {
        struct TChunkCounters
        {
            TChunkCounters(const TProfiler& profiler)
                : AddedChunks(profiler.Counter("/added_chunk_count"))
                , RemovedChunks(profiler.Counter("/removed_chunk_count"))
                , MediumChangedChunks(profiler.Counter("/medium_changed_chunk_count"))
            { }

            TCounter AddedChunks;
            TCounter RemovedChunks;
            TCounter MediumChangedChunks;
        };

        TChunkCounters Reported;
        TChunkCounters FailedToReport;

        TCounter ConfirmedAnnouncementRequests;

        TIncrementalHeartbeatCounters(const TProfiler& profiler)
            : Reported(profiler.WithPrefix("/reported"))
            , FailedToReport(profiler.WithPrefix("/failed_to_report"))
            , ConfirmedAnnouncementRequests(profiler.Counter("/confirmed_announcement_request_count"))
        { }
    };

    THashMap<TCellTag, TIncrementalHeartbeatCounters> IncrementalHeartbeatCounters_;

    // COMPAT(kvk1920)
    bool LocationUuidsRequired_ = true;

    template <typename TResponse>
    void HandleReplicaAnnouncements(
        const IAllyReplicaManagerPtr& allyReplicaManager,
        const TResponse& response,
        bool onFullHeartbeat)
    {
        if (!response.has_replica_announcements()) {
            return;
        }

        const auto& replicaAnnouncements = response.replica_announcements();
        if (replicaAnnouncements.has_non_sequoia_announcements()) {
            const auto& nonSequoiaAnnouncements = replicaAnnouncements.non_sequoia_announcements();
            allyReplicaManager->ScheduleAnnouncements(
                MakeRange(nonSequoiaAnnouncements.replica_announcement_requests()),
                nonSequoiaAnnouncements.revision(),
                onFullHeartbeat);
        }

        if (replicaAnnouncements.has_sequoia_announcements()) {
            const auto& sequoiaAnnouncements = replicaAnnouncements.sequoia_announcements();
            allyReplicaManager->ScheduleAnnouncements(
                MakeRange(sequoiaAnnouncements.replica_announcement_requests()),
                sequoiaAnnouncements.revision(),
                onFullHeartbeat);
        }

        if (replicaAnnouncements.has_enable_lazy_replica_announcements()) {
            allyReplicaManager->SetEnableLazyAnnouncements(replicaAnnouncements.enable_lazy_replica_announcements());
        }
    }

    const TIncrementalHeartbeatCounters& GetIncrementalHeartbeatCounters(TCellTag cellTag)
    {
        auto it = IncrementalHeartbeatCounters_.find(cellTag);
        if (it != IncrementalHeartbeatCounters_.end()) {
            return it->second;
        }

        TIncrementalHeartbeatCounters counters(
            DataNodeProfiler
                .WithPrefix("/incremental_heartbeat")
                .WithTag("cell_tag", ToString(cellTag)));

        return IncrementalHeartbeatCounters_.emplace(cellTag, std::move(counters)).first->second;
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& masterCellTags = Bootstrap_->GetMasterCellTags();
        for (auto cellTag : masterCellTags) {
            auto* delta = GetChunksDelta(cellTag);
            delta->State = EMasterConnectorState::Offline;
            delta->ReportedAdded.clear();
            delta->ReportedRemoved.clear();
            delta->ReportedChangedMedium.clear();
            delta->AddedSinceLastSuccess.clear();
            delta->RemovedSinceLastSuccess.clear();
            delta->ChangedMediumSinceLastSuccess.clear();

            auto* cellTagData = GetCellTagData(cellTag);
            cellTagData->ScheduledDataNodeHeartbeatCount = 0;
        }

        OnlineCellCount_ = 0;

        JobHeartbeatJobTrackerIndex_ = 0;
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

        StartHeartbeats();
    }

    void OnMasterCellDirectoryChanged(
        const THashSet<TCellTag>& addedSecondaryCellTags,
        const TSecondaryMasterConnectionConfigs& /*reconfiguredSecondaryMasterConfigs*/,
        const THashSet<TCellTag>& removedSecondaryTags)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG_UNLESS(
            addedSecondaryCellTags.empty() && removedSecondaryTags.empty(),
            "Unexpected master cell configuration detected "
            "(AddedCellTags: %v, RemovedCellTags: %v)",
            addedSecondaryCellTags,
            removedSecondaryTags);
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& dynamicConfig = newNodeConfig->DataNode->MasterConnector;
        IncrementalHeartbeatPeriod_ = dynamicConfig->IncrementalHeartbeatPeriod.value_or(*Config_->IncrementalHeartbeatPeriod);
        IncrementalHeartbeatPeriodSplay_ = dynamicConfig->IncrementalHeartbeatPeriodSplay.value_or(Config_->IncrementalHeartbeatPeriodSplay);
        JobHeartbeatPeriod_ = dynamicConfig->JobHeartbeatPeriod.value_or(*Config_->JobHeartbeatPeriod);
        JobHeartbeatPeriodSplay_ = dynamicConfig->JobHeartbeatPeriodSplay.value_or(Config_->JobHeartbeatPeriodSplay);
        MaxChunkEventsPerIncrementalHeartbeat_ = dynamicConfig->MaxChunkEventsPerIncrementalHeartbeat;
        EnableIncrementalHeartbeatProfiling_ = dynamicConfig->EnableProfiling;

        if (!EnableIncrementalHeartbeatProfiling_) {
            IncrementalHeartbeatCounters_.clear();
        }
    }

    void StartHeartbeats()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Starting data node and job heartbeats");

        const auto& masterCellTags = Bootstrap_->GetMasterCellTags();
        for (auto cellTag : masterCellTags) {
            DoScheduleHeartbeat(cellTag, /*immediately*/ true, /*outOfOrder*/ false);
        }

        DoScheduleJobHeartbeat(/*immediately*/ true);
    }

    void DoScheduleHeartbeat(TCellTag cellTag, bool immediately, bool outOfOrder)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!Initialized_) {
            YT_LOG_WARNING("Master connector is not initialized");
            return;
        }

        auto* cellTagData = GetCellTagData(cellTag);
        ++cellTagData->ScheduledDataNodeHeartbeatCount;

        // Out-of-order heartbeats are best effort, so we do not execute them if node is not online.
        if (outOfOrder && cellTagData->ChunksDelta->State != EMasterConnectorState::Online) {
            return;
        }

        auto delay = immediately ? TDuration::Zero() : IncrementalHeartbeatPeriod_ + RandomDuration(IncrementalHeartbeatPeriodSplay_);
        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::ReportHeartbeat, MakeWeak(this), cellTag),
            delay,
            HeartbeatInvoker_);
    }

    void DoScheduleJobHeartbeat(bool immediately)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto delay = immediately ? TDuration::Zero() : JobHeartbeatPeriod_ + RandomDuration(JobHeartbeatPeriodSplay_);
        delay /= JobTrackerAddresses_.size();

        const auto& jobTrackerAddress = JobTrackerAddresses_[JobHeartbeatJobTrackerIndex_];

        TDelayedExecutor::Submit(
            BIND(
                &TMasterConnector::ReportJobHeartbeat,
                MakeWeak(this),
                jobTrackerAddress,
                /*outOfOrder*/ false),
            delay,
            HeartbeatInvoker_);
    }

    void ReportJobHeartbeat(TString jobTrackerAddress, bool outOfOrder)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Reporting job heartbeat to master (JobTrackerAddress: %v, OutOfOrder: %v)",
            jobTrackerAddress,
            outOfOrder);

        auto* jobTrackerData = GetJobTrackerData(jobTrackerAddress);
        auto cellTag = jobTrackerData->CellTag;

        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&jobTrackerData->JobHeartbeatLock))
            .ValueOrThrow();

        auto state = GetMasterConnectorState(cellTag);
        if (state == EMasterConnectorState::Online) {
            TJobTrackerServiceProxy proxy(jobTrackerData->Channel);

            auto req = proxy.Heartbeat();
            req->SetTimeout(GetDynamicConfig()->JobHeartbeatTimeout);

            auto sequenceNumber = SequenceNumber_++;
            req->set_sequence_number(sequenceNumber);
            req->set_reports_heartbeats_to_all_peers(true);

            const auto& jobController = Bootstrap_->GetJobController();
            {
                auto error = WaitFor(
                    jobController->PrepareHeartbeatRequest(cellTag, jobTrackerAddress, req));
                YT_LOG_FATAL_UNLESS(
                    error.IsOK(),
                    error,
                    "Failed to prepare heartbeat request to master (JobTrackerAddress: %v)",
                    jobTrackerAddress);
            }

            YT_LOG_INFO("Job heartbeat sent to master (ResourceUsage: %v, JobTrackerAddress: %v, SequenceNumber: %v)",
                FormatResourceUsage(req->resource_usage(), req->resource_limits()),
                jobTrackerAddress,
                sequenceNumber);

            auto rspOrError = WaitFor(req->Invoke());

            if (rspOrError.IsOK()) {
                YT_LOG_INFO("Successfully reported job heartbeat to master (JobTrackerAddress: %v, SequenceNumber: %v)",
                    jobTrackerAddress,
                    sequenceNumber);

                const auto& rsp = rspOrError.Value();
                auto error = WaitFor(jobController->ProcessHeartbeatResponse(
                    jobTrackerAddress,
                    rsp));
                YT_LOG_FATAL_IF(
                    !error.IsOK(),
                    error,
                    "Fail to process heartbeat response (JobTrackerAddress: %v, SequenceNumber: %v)",
                    jobTrackerAddress,
                    sequenceNumber);
            } else {
                YT_LOG_WARNING(rspOrError, "Error reporting job heartbeat to master (JobTrackerAddress: %v, SequenceNumber: %v)",
                    jobTrackerAddress,
                    sequenceNumber);

                if (!outOfOrder) {
                    JobHeartbeatJobTrackerIndex_ = (JobHeartbeatJobTrackerIndex_ + 1) % JobTrackerAddresses_.size();
                }

                if (NRpc::IsRetriableError(rspOrError)) {
                    DoScheduleJobHeartbeat(/*immediately*/ false);
                } else {
                    Bootstrap_->ResetAndRegisterAtMaster();
                }

                return;
            }
        }

        if (!outOfOrder) {
            JobHeartbeatJobTrackerIndex_ = (JobHeartbeatJobTrackerIndex_ + 1) % JobTrackerAddresses_.size();

            DoScheduleJobHeartbeat(/*immediately*/ false);
        }
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
            case EMasterConnectorState::Registered:
                ReportFullHeartbeat(cellTag);
                break;

            case EMasterConnectorState::Online:
                ReportIncrementalHeartbeat(cellTag);
                break;

            default:
                YT_ABORT();
        }
    }

    void ReportFullHeartbeat(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(Bootstrap_->IsConnected());
        auto nodeId = Bootstrap_->GetNodeId();

        // Full heartbeat construction can take a while; offload it RPC Heavy thread pool.
        auto req = WaitFor(
            BIND(&TMasterConnector::BuildFullHeartbeatRequest, MakeStrong(this), nodeId, cellTag)
                .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
                .Run())
            .ValueOrThrow();

        YT_LOG_INFO("Sending full data node heartbeat to master (CellTag: %v)",
            cellTag);

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            OnFullHeartbeatResponse(cellTag, *rspOrError.Value());

            YT_LOG_INFO("Successfully reported full data node heartbeat to master (CellTag: %v)",
                cellTag);

            // Schedule next heartbeat.
            DoScheduleHeartbeat(cellTag, /*immediately*/ false, /*outOfOrder*/ false);
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting full data node heartbeat to master (CellTag: %v)",
                cellTag);

            if (IsRetriableError(rspOrError)) {
                DoScheduleHeartbeat(cellTag, /*immediately*/ false, /*outOfOrder*/ false);
            } else {
                Bootstrap_->ResetAndRegisterAtMaster();
            }
        }
    }

    void ReportIncrementalHeartbeat(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(Bootstrap_->IsConnected());
        auto nodeId = Bootstrap_->GetNodeId();

        auto req = BuildIncrementalHeartbeatRequest(nodeId, cellTag);

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
                DoScheduleHeartbeat(cellTag, /*immediately*/ false, /*outOfOrder*/ false);
            }
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting incremental data node heartbeat to master (CellTag: %v)",
                cellTag);

            OnIncrementalHeartbeatFailed(cellTag);

            if (IsRetriableError(rspOrError)) {
                DoScheduleHeartbeat(cellTag, /*immediately*/ false, /*outOfOrder*/ false);
            } else {
                Bootstrap_->ResetAndRegisterAtMaster();
            }
        }
    }

    void ComputeStatistics(TDataNodeStatistics* statistics) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        i64 totalAvailableSpace = 0;
        i64 totalLowWatermarkSpace = 0;
        i64 totalUsedSpace = 0;
        int totalStoredChunkCount = 0;

        THashMap<int, double> mediumIndexToIOWeight;

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        const auto& ioThroughputMeter = Bootstrap_->GetIOThroughputMeter();

        // NB. We do not indicate that the node is full when it doesn't have storage locations. See YT-15393 for details.
        bool full = !chunkStore->Locations().empty();

        for (const auto& location : chunkStore->Locations()) {
            if (!(location->CanPublish() &&
                (location->IsEnabled() || chunkStore->ShouldPublishDisabledLocations()))) {
                continue;
            }

            auto mediumIndex = location->GetMediumDescriptor().Index;

            if (mediumIndex == GenericMediumIndex) {
                continue;
            }

            totalAvailableSpace += location->GetAvailableSpace();
            totalLowWatermarkSpace += location->GetLowWatermarkSpace();
            totalUsedSpace += location->GetUsedSpace();
            totalStoredChunkCount += location->GetChunkCount();

            full &= location->IsFull();

            auto* locationStatistics = statistics->add_chunk_locations();
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
            ToProto(locationStatistics->mutable_io_statistics(),
                location->GetIOStatistics(),
                ioThroughputMeter->GetLocationIOCapacity(location->GetUuid()));

            if (IsLocationWriteable(location)) {
                mediumIndexToIOWeight[mediumIndex] += location->GetIOWeight();
            }
        }

        for (auto [mediumIndex, ioWeight] : mediumIndexToIOWeight) {
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
        VERIFY_THREAD_AFFINITY_ANY();

        if (!location->IsWritable()) {
            return false;
        }

        if (Bootstrap_->IsReadOnly()) {
            return false;
        }

        return true;
    }

    TChunkAddInfo BuildAddChunkInfo(
        const IChunkPtr& chunk,
        TChunkLocationDirectory* locationDirectory,
        bool onMediumChange = false)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TChunkAddInfo chunkAddInfo;

        ToProto(chunkAddInfo.mutable_chunk_id(), chunk->GetId());
        chunkAddInfo.set_medium_index(chunk->GetLocation()->GetMediumDescriptor().Index);
        chunkAddInfo.set_active(chunk->IsActive());
        chunkAddInfo.set_sealed(chunk->GetInfo().sealed());

        auto locationUuid = chunk->GetLocation()->GetUuid();
        chunkAddInfo.set_location_index(locationDirectory->GetOrCreateIndex(locationUuid));
        // COMPAT(kvk1920): Remove after 23.2.
        if (LocationUuidsRequired_) {
            ToProto(chunkAddInfo.mutable_location_uuid(), locationUuid);
        }

        chunkAddInfo.set_caused_by_medium_change(onMediumChange);

        return chunkAddInfo;
    }

    TChunkRemoveInfo BuildRemoveChunkInfo(
        const IChunkPtr& chunk,
        TChunkLocationDirectory* locationDirectory,
        bool onMediumChange = false)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TChunkRemoveInfo chunkRemoveInfo;

        ToProto(chunkRemoveInfo.mutable_chunk_id(), chunk->GetId());
        chunkRemoveInfo.set_medium_index(chunk->GetLocation()->GetMediumDescriptor().Index);

        auto locationUuid = chunk->GetLocation()->GetUuid();
        chunkRemoveInfo.set_location_index(locationDirectory->GetOrCreateIndex(locationUuid));
        if (LocationUuidsRequired_) {
            ToProto(chunkRemoveInfo.mutable_location_uuid(), locationUuid);
        }

        chunkRemoveInfo.set_caused_by_medium_change(onMediumChange);

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

    TPerJobTrackerData* GetJobTrackerData(const TString& jobTrackerAddress)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetOrCrash(PerJobTrackerData_, jobTrackerAddress).get();
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

    // TODO(kvk1920): Do not send every replica.
    void OnChunkMediumChanged(const IChunkPtr& chunk, int mediumIndex)
    {
        auto* delta = GetChunksDelta(chunk->GetId());
        if (delta->State != EMasterConnectorState::Online) {
            return;
        }
        delta->ChangedMediumSinceLastSuccess.emplace(chunk, mediumIndex);
    }

    void ProcessHeartbeatResponseMediaInfo(const auto& response)
    {
        if (!Bootstrap_->IsDataNode()) {
            return;
        }

        if (!response.has_medium_directory() || !response.has_medium_overrides()) {
            return;
        }

        const auto& mediumDirectoryManager = Bootstrap_->GetMediumDirectoryManager();
        mediumDirectoryManager->UpdateMediumDirectory(response.medium_directory());

        const auto& mediumUpdater = Bootstrap_->GetMediumUpdater();
        mediumUpdater->UpdateLocationMedia(response.medium_overrides());
    }

    TMasterConnectorDynamicConfigPtr GetDynamicConfig() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->MasterConnector;
    }

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap)
{
    return New<TMasterConnector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    TIOStatistics* protoStatistics,
    const TStoreLocation::TIOStatistics& statistics,
    const IIOThroughputMeter::TIOCapacity& capacity)
{
    protoStatistics->set_filesystem_read_rate(statistics.FilesystemReadRate);
    protoStatistics->set_filesystem_write_rate(statistics.FilesystemWriteRate);
    protoStatistics->set_disk_read_rate(statistics.DiskReadRate);
    protoStatistics->set_disk_write_rate(statistics.DiskWriteRate);

    protoStatistics->set_disk_read_capacity(capacity.DiskReadCapacity);
    protoStatistics->set_disk_write_capacity(capacity.DiskWriteCapacity);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
