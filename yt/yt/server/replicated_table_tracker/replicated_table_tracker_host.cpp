#include "replicated_table_tracker_host.h"

#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "private.h"

#include <yt/yt/server/lib/tablet_server/config.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/replicated_table_tracker_client/replicated_table_tracker_proxy.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NReplicatedTableTracker {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NReplicatedTableTrackerClient;
using namespace NTabletServer;
using namespace NTableClient;
using namespace NTabletClient;

using NReplicatedTableTrackerClient::EErrorCode::NonResidentReplica;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ReplicatedTableTrackerLogger;

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableTrackerHostCounters::TReplicatedTableTrackerHostCounters()
{
    auto profiler = ReplicatedTableTrackerProfiler
        .WithSparse()
        .WithPrefix("/rtt_host");

    LoadFromSnapshotCounter = profiler.Counter("/load_from_snapshot_count");
    ReceivedActionCounter = profiler.Counter("/received_action_count");
    FailedUpdateIterationCounter = profiler.Counter("/failed_update_iteration_count");
    SuccessfulUpdateIterationCounter = profiler.Counter("/successful_update_iteration_count");
    ReplicaLagSubrequestCounter = profiler.Counter("/replica_lag_subrequest_count");
    MissingReplicaLagSubresponseCounter = profiler.Counter("/missing_replica_lag_subresponse_count");
    ApplyCommandSubrequestCounter = profiler.Counter("/apply_command_subrequest_count");
    MissingApplyCommandSubresponseCounter = profiler.Counter("/missing_apply_command_subresponse_count");
}

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableTrackerHost::TReplicatedTableTrackerHost(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{
    SubscribeConfigChanged(BIND_NO_PROPAGATE(&TReplicatedTableTrackerHost::OnConfigChanged, MakeWeak(this)));
    OnConfigChanged(Bootstrap_->GetDynamicConfigManager()->GetConfig());

    ScheduleUpdateIteration();
}

void TReplicatedTableTrackerHost::EnableUpdates()
{
    UpdatesEnabled_ = true;
}

void TReplicatedTableTrackerHost::DisableUpdates()
{
    UpdatesEnabled_ = false;
}

bool TReplicatedTableTrackerHost::AlwaysUseNewReplicatedTableTracker() const
{
    return false;
}

TFuture<TReplicatedTableTrackerSnapshot> TReplicatedTableTrackerHost::GetSnapshot()
{
    auto guard = Guard(SnapshotLock_);

    YT_VERIFY(SnapshotPromise_);

    if (SnapshotPromise_.IsSet()) {
        auto future = SnapshotPromise_.ToFuture();
        SnapshotPromise_.Reset();
        return future;
    }

    return SnapshotPromise_.ToFuture().ApplyUnique(BIND([
        this,
        this_ = MakeStrong(this)
    ] (TErrorOr<TReplicatedTableTrackerSnapshot>&& snapshotOrError) {
        {
            auto guard = Guard(SnapshotLock_);
            SnapshotPromise_.Reset();
        }
        return snapshotOrError.ValueOrThrow();
    })
        .AsyncVia(Bootstrap_->GetRttHostInvoker()));
}

TDynamicReplicatedTableTrackerConfigPtr TReplicatedTableTrackerHost::GetConfig() const
{
    return Bootstrap_->GetDynamicConfigManager()->GetConfig();
}

bool TReplicatedTableTrackerHost::LoadingFromSnapshotRequested() const
{
    auto guard = Guard(SnapshotLock_);
    return SnapshotPromise_.operator bool();
}

void TReplicatedTableTrackerHost::RequestLoadingFromSnapshot()
{
    auto guard = Guard(SnapshotLock_);
    if (!SnapshotPromise_) {
        SnapshotPromise_ = NewPromise<TReplicatedTableTrackerSnapshot>();
    }
}

TFuture<TReplicaLagTimes> TReplicatedTableTrackerHost::ComputeReplicaLagTimes(
    std::vector<TTableReplicaId> replicaIds)
{
    return BIND(
        &TReplicatedTableTrackerHost::DoComputeReplicaLagTimes,
        MakeStrong(this),
        std::move(replicaIds))
        .AsyncVia(Bootstrap_->GetRttHostInvoker())
        .Run();
}

NApi::IClientPtr TReplicatedTableTrackerHost::CreateClusterClient(const TString& clusterName)
{
    const auto& clusterDirectory = Bootstrap_->GetClusterConnection()->GetClusterDirectory();
    auto connection = clusterDirectory->FindConnection(clusterName);
    // TODO(akozhikhov): Consider employing specific user.
    return connection
        ? connection->CreateClient(NApi::TClientOptions::FromUser(NSecurityClient::RootUserName))
        : nullptr;
}

TFuture<TApplyChangeReplicaCommandResults> TReplicatedTableTrackerHost::ApplyChangeReplicaModeCommands(
    std::vector<TChangeReplicaModeCommand> commands)
{
    return BIND(
        &TReplicatedTableTrackerHost::DoApplyChangeReplicaModeCommands,
        MakeStrong(this),
        std::move(commands))
        .AsyncVia(Bootstrap_->GetRttHostInvoker())
        .Run();
}

void TReplicatedTableTrackerHost::SubscribeReplicatedTableCreated(
    TCallback<void(TReplicatedTableData)> callback)
{
    ReplicatedTableCreated_ = std::move(callback);
}

void TReplicatedTableTrackerHost::SubscribeReplicatedTableDestroyed(
    TCallback<void(NTableClient::TTableId)> callback)
{
    ReplicatedTableDestroyed_ = std::move(callback);
}

void TReplicatedTableTrackerHost::SubscribeReplicaCreated(
    TCallback<void(TReplicaData)> callback)
{
    ReplicaCreated_ = std::move(callback);
}

void TReplicatedTableTrackerHost::SubscribeReplicaDestroyed(
    TCallback<void(TTableReplicaId)> callback)
{
    ReplicaDestroyed_ = std::move(callback);
}

void TReplicatedTableTrackerHost::SubscribeReplicationCollocationCreated(
    TCallback<void(TTableCollocationData)> callback)
{
    ReplicationCollocationCreated_ = std::move(callback);
}

void TReplicatedTableTrackerHost::SubscribeReplicationCollocationDestroyed(
    TCallback<void(NTableClient::TTableCollocationId)> callback)
{
    ReplicationCollocationDestroyed_ = std::move(callback);
}

void TReplicatedTableTrackerHost::ScheduleUpdateIteration()
{
    TDelayedExecutor::Submit(
        BIND(&TReplicatedTableTrackerHost::RunUpdateIteration, MakeWeak(this))
            .Via(Bootstrap_->GetRttHostInvoker()),
        UpdatePeriod_.load());
}

void TReplicatedTableTrackerHost::RunUpdateIteration()
{
    if (!UpdatesEnabled_ || !TrackerEnabled_) {
        YT_LOG_DEBUG("Skipping host update iteration");
        ScheduleUpdateIteration();
        return;
    }

    try {
        RequestStateUpdates();
        Counters_.SuccessfulUpdateIterationCounter.Increment();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Update iteration failed");
        Counters_.FailedUpdateIterationCounter.Increment();
    }

    ScheduleUpdateIteration();
}

void TReplicatedTableTrackerHost::RequestStateUpdates()
{
    bool loadFromSnapshot;
    {
        auto guard = Guard(SnapshotLock_);
        if (SnapshotPromise_ && SnapshotPromise_.IsSet()) {
            YT_LOG_DEBUG("Skipping host update iteration due to set snapshot");
            return;
        }
        loadFromSnapshot = SnapshotPromise_.operator bool();
    }

    std::vector<TFuture<TGetStateUpdatesRsp>> futures;

    auto secondaryCellTags = Bootstrap_->GetClusterConnection()->GetSecondaryMasterCellTags();
    for (auto cellTag : secondaryCellTags) {
        futures.push_back(RequestStateUpdatesFromCell(
            cellTag,
            loadFromSnapshot));
    }

    auto primaryCellTag = Bootstrap_->GetClusterConnection()->GetPrimaryMasterCellTag();
    futures.push_back(RequestStateUpdatesFromCell(
        primaryCellTag,
        loadFromSnapshot));

    auto responseOrErrors = WaitFor(AllSet(std::move(futures)))
        .ValueOrThrow();

    for (int index = 0; index < std::ssize(responseOrErrors); ++index) {
        const auto& responseOrError = responseOrErrors[index];
        if (!responseOrError.IsOK() &&
            responseOrError.FindMatching(NReplicatedTableTrackerClient::EErrorCode::StateRevisionMismatch))
        {
            RequestLoadingFromSnapshot();
            THROW_ERROR_EXCEPTION("Rtt has stale state; will load from snapshot")
                << responseOrError;
        }

        auto response = responseOrError.ValueOrThrow();

        auto cellTag = index + 1 == std::ssize(responseOrErrors)
            ? primaryCellTag
            : secondaryCellTags[index];
        auto& cellInfo = CellTagToInfo_[cellTag];

        const auto& address = response->GetAddress();
        YT_VERIFY(!address.empty());
        if (cellInfo.LeaderAddress.empty()) {
            cellInfo.LeaderAddress = address;
        } else if (cellInfo.LeaderAddress != address) {
            auto oldAddress = cellInfo.LeaderAddress;
            cellInfo.LeaderAddress = address;
            cellInfo.Revision = InvalidTrackerStateRevision;
            RequestLoadingFromSnapshot();
            THROW_ERROR_EXCEPTION("Leader channel detected: %v -> %v; will load from snapshot",
                oldAddress,
                address);
        }
    }

    int snapshotCount = 0;
    int updateActionCount = 0;
    for (const auto& responseOrError : responseOrErrors) {
        snapshotCount += responseOrError.Value()->has_snapshot();
        updateActionCount += responseOrError.Value()->update_actions_size();
    }

    if (!loadFromSnapshot && snapshotCount != 0 && snapshotCount < std::ssize(futures)) {
        RequestLoadingFromSnapshot();
        THROW_ERROR_EXCEPTION("%v responses unexpectedly contain snapshot; will load from snapshot",
            snapshotCount);
    }

    Counters_.LoadFromSnapshotCounter.Increment(snapshotCount);
    Counters_.ReceivedActionCounter.Increment(updateActionCount);

    if (snapshotCount != 0) {
        TReplicatedTableTrackerSnapshot snapshot;
        for (int index = 0; index < std::ssize(responseOrErrors); ++index) {
            const auto& response = responseOrErrors[index].Value();

            auto cellTag = index + 1 == std::ssize(responseOrErrors)
                ? primaryCellTag
                : secondaryCellTags[index];

            YT_VERIFY(response->has_snapshot());
            FromProto(&snapshot, response->snapshot());
            // TODO(akozhikhov): Drop this when collocations are cleaned out from secondary master snapshots.
            if (cellTag != primaryCellTag) {
                snapshot.Collocations.clear();
            }
            CellTagToInfo_[cellTag].Revision = response->snapshot_revision();

            YT_LOG_DEBUG("Rtt host received new snapshot part (CellTag: %v, Revision: %v)",
                cellTag,
                response->snapshot_revision());
        }

        auto guard = Guard(SnapshotLock_);
        if (!SnapshotPromise_) {
            SnapshotPromise_ = NewPromise<TReplicatedTableTrackerSnapshot>();
        }
        YT_VERIFY(!SnapshotPromise_.IsSet());
        SnapshotPromise_.Set(std::move(snapshot));
    } else {
        for (int index = 0; index < std::ssize(responseOrErrors); ++index) {
            const auto& response = responseOrErrors[index].Value();

            auto cellTag = index + 1 == std::ssize(responseOrErrors)
                ? primaryCellTag
                : secondaryCellTags[index];

            for (const auto& action : response->update_actions()) {
                YT_VERIFY(CellTagToInfo_[cellTag].Revision < action.revision());
                CellTagToInfo_[cellTag].Revision = action.revision();

                YT_LOG_DEBUG("Rtt host received new action (CellTag: %v, Revision: %v)",
                    cellTag,
                    action.revision());

                if (action.has_created_replicated_table_data()) {
                    TReplicatedTableData tableData;
                    FromProto(&tableData, action.created_replicated_table_data());
                    ReplicatedTableCreated_(tableData);
                } else if (action.has_destroyed_replicated_table_id()) {
                    ReplicatedTableDestroyed_(FromProto<TTableId>(action.destroyed_replicated_table_id()));
                } else if (action.has_created_replica_data()) {
                    TReplicaData replicaData;
                    FromProto(&replicaData, action.created_replica_data());
                    ReplicaCreated_(replicaData);
                } else if (action.has_destroyed_replica_id()) {
                    ReplicaDestroyed_(FromProto<TTableReplicaId>(action.destroyed_replica_id()));
                } else if (action.has_created_collocation_data()) {
                    if (cellTag == primaryCellTag) {
                        TTableCollocationData collocationData;
                        FromProto(&collocationData, action.created_collocation_data());
                        ReplicationCollocationCreated_(collocationData);
                    }
                } else if (action.has_destroyed_collocation_id()) {
                    if (cellTag == primaryCellTag) {
                        ReplicationCollocationDestroyed_(FromProto<TTableCollocationId>(
                            action.destroyed_collocation_id()));
                    }
                } else {
                    YT_ABORT();
                }
            }
        }
    }
}

TApplyChangeReplicaCommandResults TReplicatedTableTrackerHost::DoApplyChangeReplicaModeCommands(
    const std::vector<TChangeReplicaModeCommand>& commands)
{
    if (!TrackerEnabled_) {
        THROW_ERROR_EXCEPTION("Disabled Rtt host will not apply change replica mode commands");
    }

    if (LoadingFromSnapshotRequested()) {
        THROW_ERROR_EXCEPTION("Will not apply change replica mode commands over a stale state");
    }

    std::vector<TFuture<TApplyChangeModeCommandsRsp>> futures;
    futures.push_back(ApplyChangeModeCommandsOnCell(
        Bootstrap_->GetClusterConnection()->GetPrimaryMasterCellTag(),
        commands));
    for (auto cellTag : Bootstrap_->GetClusterConnection()->GetSecondaryMasterCellTags()) {
        futures.push_back(ApplyChangeModeCommandsOnCell(cellTag, commands));
    }

    auto responses = WaitFor(AllSucceeded(std::move(futures)))
        .ValueOrThrow();

    TApplyChangeReplicaCommandResults results;
    results.resize(commands.size(), TError(NonResidentReplica, "Default non-resident replica error"));

    for (const auto& response : responses) {
        YT_VERIFY(response->inner_errors_size() == std::ssize(commands));

        for (int index = 0; index < std::ssize(commands); ++index) {
            auto error = FromProto<TError>(response->inner_errors(index));
            if (error.IsOK() ||
                !error.FindMatching(NonResidentReplica))
            {
                // NB: Only one response can contain something different from non-resident replica error.
                YT_VERIFY(results[index].GetCode() == NonResidentReplica);
                results[index] = error;
            }
        }
    }

    Counters_.ApplyCommandSubrequestCounter.Increment(commands.size());
    Counters_.MissingApplyCommandSubresponseCounter.Increment(std::count_if(
        results.begin(),
        results.end(),
        [] (const TError& error) { return error.GetCode() == NonResidentReplica; }));

    return results;
}

TReplicaLagTimes TReplicatedTableTrackerHost::DoComputeReplicaLagTimes(
    const std::vector<TTableReplicaId>& replicaIds)
{
    if (!TrackerEnabled_) {
        THROW_ERROR_EXCEPTION("Disabled Rtt host will not compute replica lag times");
    }

    if (LoadingFromSnapshotRequested()) {
        THROW_ERROR_EXCEPTION("Will not compute replica lag times over a stale state");
    }

    std::vector<TFuture<TComputeLagTimesRsp>> futures;
    futures.push_back(ComputeLagTimesOnCell(
        Bootstrap_->GetClusterConnection()->GetPrimaryMasterCellTag(),
        replicaIds));
    for (auto cellTag : Bootstrap_->GetClusterConnection()->GetSecondaryMasterCellTags()) {
        futures.push_back(ComputeLagTimesOnCell(cellTag, replicaIds));
    }

    auto responses = WaitFor(AllSucceeded(std::move(futures)))
        .ValueOrThrow();

    THashMap<TTableReplicaId, std::optional<TDuration>> replicaIdToLagTime;
    for (const auto& response : responses) {
        YT_VERIFY(response->replica_ids_size() == response->lag_times_size());
        for (int index = 0; index < response->replica_ids_size(); ++index) {
            EmplaceOrCrash(
                replicaIdToLagTime,
                FromProto<TTableReplicaId>(response->replica_ids(index)),
                std::make_optional(TDuration::FromValue(response->lag_times(index))));
        }
    }

    Counters_.ReplicaLagSubrequestCounter.Increment(replicaIds.size());
    Counters_.MissingReplicaLagSubresponseCounter.Increment(replicaIds.size() - replicaIdToLagTime.size());

    TReplicaLagTimes result(replicaIdToLagTime.begin(), replicaIdToLagTime.end());

    return result;
}

TFuture<TGetStateUpdatesRsp> TReplicatedTableTrackerHost::RequestStateUpdatesFromCell(
    NObjectClient::TCellTag cellTag,
    bool loadFromSnapshot)
{
    auto channel = Bootstrap_->GetClusterConnection()->GetMasterChannelOrThrow(
        NApi::EMasterChannelKind::Leader,
        cellTag);

    TReplicatedTableTrackerServiceProxy proxy(channel);

    proxy.SetDefaultTimeout(Bootstrap_->GetServerConfig()->RttServiceRequestTimeout);

    auto request = proxy.GetTrackerStateUpdates();
    request->SetResponseHeavy(true);

    request->set_snapshot_requested(loadFromSnapshot);
    request->set_revision(CellTagToInfo_[cellTag].Revision);

    return request->Invoke();
}

TFuture<TApplyChangeModeCommandsRsp> TReplicatedTableTrackerHost::ApplyChangeModeCommandsOnCell(
    NObjectClient::TCellTag cellTag,
    const std::vector<TChangeReplicaModeCommand>& commands)
{
    auto channel = Bootstrap_->GetClusterConnection()->GetMasterChannelOrThrow(
        NApi::EMasterChannelKind::Leader,
        cellTag);

    TReplicatedTableTrackerServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Bootstrap_->GetServerConfig()->RttServiceRequestTimeout);

    auto request = proxy.ApplyChangeReplicaModeCommands();
    request->SetRequestHeavy(true);
    request->SetResponseHeavy(true);

    for (const auto& command : commands) {
        ToProto(request->add_commands(), command);
    }

    return request->Invoke();
}

TFuture<TComputeLagTimesRsp> TReplicatedTableTrackerHost::ComputeLagTimesOnCell(
    NObjectClient::TCellTag cellTag,
    const std::vector<TTableReplicaId>& replicaIds)
{
    auto channel = Bootstrap_->GetClusterConnection()->GetMasterChannelOrThrow(
        NApi::EMasterChannelKind::Leader,
        cellTag);

    TReplicatedTableTrackerServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Bootstrap_->GetServerConfig()->RttServiceRequestTimeout);

    auto request = proxy.ComputeReplicaLagTimes();
    request->SetRequestHeavy(true);
    request->SetResponseHeavy(true);

    for (const auto& replicaId : replicaIds) {
        ToProto(request->add_replica_ids(), replicaId);
    }

    return request->Invoke();
}

void TReplicatedTableTrackerHost::OnConfigChanged(const TDynamicReplicatedTableTrackerConfigPtr& newConfig)
{
    UpdatePeriod_ = newConfig->UpdatePeriod;
    TrackerEnabled_ = newConfig->EnableReplicatedTableTracker && newConfig->UseNewReplicatedTableTracker;
}

void TReplicatedTableTrackerHost::SubscribeConfigChanged(
    TCallback<void(TDynamicReplicatedTableTrackerConfigPtr)> callback)
{
    Bootstrap_->GetDynamicConfigManager()->SubscribeConfigChanged(BIND(
        [callback = std::move(callback)] (
            const TDynamicReplicatedTableTrackerConfigPtr& /*oldConfig*/,
            const TDynamicReplicatedTableTrackerConfigPtr& newConfig)
        {
            callback(newConfig);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
