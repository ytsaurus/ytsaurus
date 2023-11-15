#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/core/actions/callback.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

struct TReplicatedTableTrackerHostCounters
{
    TReplicatedTableTrackerHostCounters();

    NProfiling::TCounter LoadFromSnapshotCounter;
    NProfiling::TCounter ReceivedActionCounter;
    NProfiling::TCounter FailedUpdateIterationCounter;
    NProfiling::TCounter SuccessfulUpdateIterationCounter;
    NProfiling::TCounter ReplicaLagSubrequestCounter;
    NProfiling::TCounter MissingReplicaLagSubresponseCounter;
    NProfiling::TCounter ApplyCommandSubrequestCounter;
    NProfiling::TCounter MissingApplyCommandSubresponseCounter;
};

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerHost
    : public NTabletServer::IReplicatedTableTrackerHost
{
public:
    TReplicatedTableTrackerHost(IBootstrap* bootstrap);

    void EnableUpdates();
    void DisableUpdates();

    bool AlwaysUseNewReplicatedTableTracker() const override;

    TFuture<NTabletServer::TReplicatedTableTrackerSnapshot> GetSnapshot() override;
    NTabletServer::TDynamicReplicatedTableTrackerConfigPtr GetConfig() const override;

    bool LoadingFromSnapshotRequested() const override;
    void RequestLoadingFromSnapshot() override;

    TFuture<NTabletServer::TReplicaLagTimes> ComputeReplicaLagTimes(
        std::vector<NTabletClient::TTableReplicaId> replicaIds) override;

    NApi::IClientPtr CreateClusterClient(const TString& clusterName) override;

    TFuture<NTabletServer::TApplyChangeReplicaCommandResults> ApplyChangeReplicaModeCommands(
        std::vector<NTabletServer::TChangeReplicaModeCommand> commands) override;

    void SubscribeReplicatedTableCreated(TCallback<void(NTabletServer::TReplicatedTableData)> callback) override;
    void SubscribeReplicatedTableDestroyed(TCallback<void(NTableClient::TTableId)> callback) override;

    void SubscribeReplicaCreated(TCallback<void(NTabletServer::TReplicaData)> callback) override;
    void SubscribeReplicaDestroyed(TCallback<void(NTabletClient::TTableReplicaId)> callback) override;

    void SubscribeReplicationCollocationCreated(TCallback<void(NTabletServer::TTableCollocationData)> callback) override;
    void SubscribeReplicationCollocationDestroyed(TCallback<void(NTableClient::TTableCollocationId)> callback) override;

private:
    IBootstrap* const Bootstrap_;

    const TReplicatedTableTrackerHostCounters Counters_;

    TCallback<void(NTabletServer::TReplicatedTableData)> ReplicatedTableCreated_;
    TCallback<void(NTableClient::TTableId)> ReplicatedTableDestroyed_;
    TCallback<void(NTabletServer::TReplicaData)> ReplicaCreated_;
    TCallback<void(NTabletClient::TTableReplicaId)> ReplicaDestroyed_;
    TCallback<void(NTabletServer::TTableCollocationData)> ReplicationCollocationCreated_;
    TCallback<void(NTableClient::TTableCollocationId)> ReplicationCollocationDestroyed_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SnapshotLock_);
    // NB: Null promise means no snapshot is requested. Unset promise means snapshot is requested.
    // Set promise means host is waiting for Rtt to pick it up and shall remain idle until that.
    TPromise<NTabletServer::TReplicatedTableTrackerSnapshot> SnapshotPromise_;

    struct TCellInfo
    {
        TString LeaderAddress = {};
        NReplicatedTableTrackerClient::TTrackerStateRevision Revision;
    };

    THashMap<NObjectClient::TCellTag, TCellInfo> CellTagToInfo_;

    std::atomic<bool> UpdatesEnabled_ = false;
    std::atomic<bool> TrackerEnabled_;

    std::atomic<TDuration> UpdatePeriod_;


    void ScheduleUpdateIteration();
    void RunUpdateIteration();

    void RequestStateUpdates();

    NTabletServer::TApplyChangeReplicaCommandResults DoApplyChangeReplicaModeCommands(
        const std::vector<NTabletServer::TChangeReplicaModeCommand>& commands);
    NTabletServer::TReplicaLagTimes DoComputeReplicaLagTimes(
        const std::vector<NTabletClient::TTableReplicaId>& replicaIds);

    TFuture<TGetStateUpdatesRsp> RequestStateUpdatesFromCell(
        NObjectClient::TCellTag cellTag,
        bool loadFromSnapshot);
    TFuture<TApplyChangeModeCommandsRsp> ApplyChangeModeCommandsOnCell(
        NObjectClient::TCellTag cellTag,
        const std::vector<NTabletServer::TChangeReplicaModeCommand>& commands);
    TFuture<TComputeLagTimesRsp> ComputeLagTimesOnCell(
        NObjectClient::TCellTag cellTag,
        const std::vector<NTabletClient::TTableReplicaId>& replicaIds);

    void OnConfigChanged(const NTabletServer::TDynamicReplicatedTableTrackerConfigPtr& oldConfig);
    void SubscribeConfigChanged(
        TCallback<void(NTabletServer::TDynamicReplicatedTableTrackerConfigPtr)> callback);
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableTrackerHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
