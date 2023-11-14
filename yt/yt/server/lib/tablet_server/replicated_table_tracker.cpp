#include "replicated_table_tracker.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/replicated_table_tracker_client/proto/replicated_table_tracker_client.pb.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/sync_expiring_cache.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NTabletServer {

using namespace NConcurrency;
using namespace NTabletClient;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;
using namespace NTableClient;
using namespace NCypressClient;
using namespace NApi;
using namespace NObjectClient;
using namespace NTracing;
using namespace NProfiling;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("ReplicatedTableTracker");

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TChangeReplicaModeCommand& command,
    TStringBuf /*spec*/)
{
    builder->AppendFormat("ReplicaId: %v, TargetMode: %v",
        command.ReplicaId,
        command.TargetMode);
}

TString ToString(const TChangeReplicaModeCommand& command)
{
    return ToStringViaBuilder(command);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NReplicatedTableTrackerClient::NProto::TReplicatedTableData* protoTableData,
    const TReplicatedTableData& tableData)
{
    ToProto(protoTableData->mutable_table_id(), tableData.Id);
    protoTableData->set_table_options(ConvertToYsonString(tableData.Options).ToString());
}

void FromProto(
    TReplicatedTableData* tableData,
    const NReplicatedTableTrackerClient::NProto::TReplicatedTableData& protoTableData)
{
    tableData->Id = FromProto<TTableId>(protoTableData.table_id());
    tableData->Options = ConvertTo<TReplicatedTableOptionsPtr>(TYsonString(protoTableData.table_options()));
}

void ToProto(
    NReplicatedTableTrackerClient::NProto::TReplicaData* protoReplicaData,
    const TReplicaData& replicaData)
{
    ToProto(protoReplicaData->mutable_table_id(), replicaData.TableId);
    ToProto(protoReplicaData->mutable_replica_id(), replicaData.Id);
    protoReplicaData->set_mode(ToProto<int>(replicaData.Mode));
    protoReplicaData->set_enabled(replicaData.Enabled);
    protoReplicaData->set_cluster_name(replicaData.ClusterName);
    protoReplicaData->set_table_path(replicaData.TablePath);
    protoReplicaData->set_tracking_enabled(replicaData.TrackingEnabled);
    protoReplicaData->set_content_type(ToProto<int>(replicaData.ContentType));
}

void FromProto(
    TReplicaData* replicaData,
    const NReplicatedTableTrackerClient::NProto::TReplicaData& protoReplicaData)
{
    replicaData->TableId = FromProto<TTableId>(protoReplicaData.table_id());
    replicaData->Id = FromProto<TTableReplicaId>(protoReplicaData.replica_id());
    replicaData->Mode = CheckedEnumCast<ETableReplicaMode>(protoReplicaData.mode());
    replicaData->Enabled = protoReplicaData.enabled();
    replicaData->ClusterName = protoReplicaData.cluster_name();
    replicaData->TablePath = protoReplicaData.table_path();
    replicaData->TrackingEnabled = protoReplicaData.tracking_enabled();
    replicaData->ContentType = CheckedEnumCast<ETableReplicaContentType>(protoReplicaData.content_type());
}

void ToProto(
    NReplicatedTableTrackerClient::NProto::TTableCollocationData* protoCollocationData,
    const TTableCollocationData& collocationData)
{
    ToProto(protoCollocationData->mutable_collocation_id(), collocationData.Id);
    for (auto tableId : collocationData.TableIds) {
        ToProto(protoCollocationData->add_table_ids(), tableId);
    }
}

void FromProto(
    TTableCollocationData* collocationData,
    const NReplicatedTableTrackerClient::NProto::TTableCollocationData& protoCollocationData)
{
    collocationData->Id = FromProto<TTableCollocationId>(protoCollocationData.collocation_id());
    collocationData->TableIds.reserve(protoCollocationData.table_ids_size());
    for (auto protoTableId : protoCollocationData.table_ids()) {
        collocationData->TableIds.push_back(FromProto<TTableId>(protoTableId));
    }
}

void ToProto(
    NReplicatedTableTrackerClient::NProto::TReplicatedTableTrackerSnapshot* protoTrackerSnapshot,
    const TReplicatedTableTrackerSnapshot& trackerSnapshot)
{
    for (const auto& tableData : trackerSnapshot.ReplicatedTables) {
        ToProto(
            protoTrackerSnapshot->add_replicated_table_data_list(),
            tableData);
    }
    for (const auto& replicaData : trackerSnapshot.Replicas) {
        ToProto(
            protoTrackerSnapshot->add_replica_data_list(),
            replicaData);
    }
    for (const auto& collocationData : trackerSnapshot.Collocations) {
        ToProto(
            protoTrackerSnapshot->add_collocation_data_list(),
            collocationData);
    }
}

void FromProto(
    TReplicatedTableTrackerSnapshot* trackerSnapshot,
    const NReplicatedTableTrackerClient::NProto::TReplicatedTableTrackerSnapshot& protoTrackerSnapshot)
{
    for (const auto& protoTableData : protoTrackerSnapshot.replicated_table_data_list()) {
        FromProto(
            &trackerSnapshot->ReplicatedTables.emplace_back(),
            protoTableData);
    }
    for (const auto& protoReplicaData : protoTrackerSnapshot.replica_data_list()) {
        FromProto(
            &trackerSnapshot->Replicas.emplace_back(),
            protoReplicaData);
    }
    for (const auto& protoCollocationData : protoTrackerSnapshot.collocation_data_list()) {
        FromProto(
            &trackerSnapshot->Collocations.emplace_back(),
            protoCollocationData);
    }
}

void ToProto(
    NReplicatedTableTrackerClient::NProto::TChangeReplicaModeCommand* protoCommand,
    const TChangeReplicaModeCommand& command)
{
    ToProto(protoCommand->mutable_replica_id(), command.ReplicaId);
    protoCommand->set_target_mode(ToProto<int>(command.TargetMode));
}

void FromProto(
    TChangeReplicaModeCommand* command,
    const NReplicatedTableTrackerClient::NProto::TChangeReplicaModeCommand& protoCommand)
{
    command->ReplicaId = FromProto<TTableReplicaId>(protoCommand.replica_id());
    command->TargetMode = CheckedEnumCast<ETableReplicaMode>(protoCommand.target_mode());
}

////////////////////////////////////////////////////////////////////////////////

using TClusterKey = TString;

using TClusterClientCache = TSyncExpiringCache<TString, TErrorOr<NApi::IClientPtr>>;
using TClusterClientCachePtr = TIntrusivePtr<TClusterClientCache>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClusterLivenessCheckCache)

class TClusterLivenessCheckCache
    : public TAsyncExpiringCache<TClusterKey, void>
{
public:
    TClusterLivenessCheckCache(
        TAsyncExpiringCacheConfigPtr config,
        TClusterClientCachePtr clusterClientCache)
        : TAsyncExpiringCache(
            std::move(config),
            Logger.WithTag("Cache: ClusterLivenessCheck"))
        , ClusterClientCache_(std::move(clusterClientCache))
    { }

protected:
    TFuture<void> DoGet(
        const TClusterKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        auto clientOrError = ClusterClientCache_->Get(key);
        if (!clientOrError.IsOK()) {
            return MakeFuture(TError(clientOrError));
        }

        NApi::TCheckClusterLivenessOptions options{
            .CheckCypressRoot = true,
            .CheckSecondaryMasterCells = true,
        };
        return clientOrError.Value()->CheckClusterLiveness(options)
            .Apply(BIND([=] (const TError& result) {
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error checking cluster %Qv liveness",
                    key);
            }));
    }

private:
    const TClusterClientCachePtr ClusterClientCache_;
};

DEFINE_REFCOUNTED_TYPE(TClusterLivenessCheckCache)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClusterIncomingReplicationCheckCache)

class TClusterIncomingReplicationCheckCache
    : public TAsyncExpiringCache<TClusterKey, void>
{
public:
    TClusterIncomingReplicationCheckCache(
        TAsyncExpiringCacheConfigPtr config,
        TClusterClientCachePtr clusterClientCache)
        : TAsyncExpiringCache(
            std::move(config),
            Logger.WithTag("Cache: ClusterIncomingReplicationCheck"))
        , ClusterClientCache_(std::move(clusterClientCache))
    { }

protected:
    TFuture<void> DoGet(
        const TClusterKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        auto clientOrError = ClusterClientCache_->Get(key);
        if (!clientOrError.IsOK()) {
            return MakeFuture(TError(clientOrError));
        }

        return clientOrError.Value()->GetNode("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/enable_incoming_replication")
            .Apply(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
                if (!resultOrError.IsOK()) {
                    // COMPAT(akozhikhov).
                    if (resultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                        return;
                    }
                    THROW_ERROR_EXCEPTION("Failed to check whether incoming replication to cluster %Qv is enabled",
                        key)
                        << TError(resultOrError);
                }
                if (!ConvertTo<bool>(resultOrError.Value())) {
                    THROW_ERROR_EXCEPTION("Replica cluster %Qv incoming replication is disabled",
                        key);
                }
            }));
    }

private:
    const TClusterClientCachePtr ClusterClientCache_;
};

DEFINE_REFCOUNTED_TYPE(TClusterIncomingReplicationCheckCache)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClusterSafeModeCheckCache)

class TClusterSafeModeCheckCache
    : public TAsyncExpiringCache<TClusterKey, void>
{
public:
    TClusterSafeModeCheckCache(
        TAsyncExpiringCacheConfigPtr config,
        TClusterClientCachePtr clusterClientCache)
        : TAsyncExpiringCache(
            std::move(config),
            Logger.WithTag("Cache: ClusterSafeModeCheck"))
        , ClusterClientCache_(std::move(clusterClientCache))
    { }

protected:
    TFuture<void> DoGet(
        const TClusterKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        auto clientOrError = ClusterClientCache_->Get(key);
        if (!clientOrError.IsOK()) {
            return MakeFuture(TError(clientOrError));
        }

        return clientOrError.Value()->GetNode("//sys/@config/enable_safe_mode")
            .Apply(BIND([=] (const TErrorOr<TYsonString>& error) {
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    error,
                    "Error getting @enable_safe_mode attribute for cluster %Qv",
                    key);
                if (ConvertTo<bool>(error.Value())) {
                    THROW_ERROR_EXCEPTION("Safe mode is enabled for cluster %Qv",
                        key);
                }
            }));
    }

private:
    const TClusterClientCachePtr ClusterClientCache_;
};

DEFINE_REFCOUNTED_TYPE(TClusterSafeModeCheckCache)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THydraReadOnlyCheckCache)

class THydraReadOnlyCheckCache
    : public TAsyncExpiringCache<TClusterKey, void>
{
public:
    THydraReadOnlyCheckCache(
        TAsyncExpiringCacheConfigPtr config,
        TClusterClientCachePtr clusterClientCache)
        : TAsyncExpiringCache(
            std::move(config),
            Logger.WithTag("Cache: HydraReadOnlyCheck"))
        , ClusterClientCache_(std::move(clusterClientCache))
    { }

protected:
    TFuture<void> DoGet(
        const TClusterKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        auto clientOrError = ClusterClientCache_->Get(key);
        if (!clientOrError.IsOK()) {
            return MakeFuture(TError(clientOrError));
        }

        return clientOrError.Value()->GetNode("//sys/@hydra_read_only")
            .Apply(BIND([=] (const TErrorOr<TYsonString>& error) {
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    error,
                    "Error getting @hydra_read_only attribute for cluster %Qv",
                    key);
                if (ConvertTo<bool>(error.Value())) {
                    THROW_ERROR_EXCEPTION("Hydra read only mode is activated for cluster %Qv",
                        key);
                }
            }));
    }

private:
    const TClusterClientCachePtr ClusterClientCache_;
};

DEFINE_REFCOUNTED_TYPE(THydraReadOnlyCheckCache)

////////////////////////////////////////////////////////////////////////////////

struct TBundleHealthKey
{
    TClusterKey ClusterKey;
    TString BundleName;

    bool operator == (const TBundleHealthKey& other) const
    {
        return ClusterKey == other.ClusterKey &&
            BundleName == other.BundleName;
    }

    operator size_t() const
    {
        return MultiHash(
            ClusterKey,
            BundleName);
    }
};

void FormatValue(TStringBuilderBase* builder, const TBundleHealthKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v@%v",
        key.BundleName,
        key.ClusterKey);
}

TString ToString(const TBundleHealthKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNewBundleHealthCache)

class TNewBundleHealthCache
    : public TAsyncExpiringCache<TBundleHealthKey, void>
{
public:
    TNewBundleHealthCache(
        TAsyncExpiringCacheConfigPtr config,
        TClusterClientCachePtr clusterClientCache)
        : TAsyncExpiringCache(
            std::move(config),
            Logger.WithTag("Cache: BundleHealth"))
        , ClusterClientCache_(std::move(clusterClientCache))
    { }

protected:
    TFuture<void> DoGet(
        const TBundleHealthKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        auto clientOrError = ClusterClientCache_->Get(key.ClusterKey);
        if (!clientOrError.IsOK()) {
            return MakeFuture(TError(clientOrError));
        }

        NApi::TCheckClusterLivenessOptions options{
            .CheckTabletCellBundle = key.BundleName,
        };
        return clientOrError.Value()->CheckClusterLiveness(options);
    }

private:
    const TClusterClientCachePtr ClusterClientCache_;
};

DEFINE_REFCOUNTED_TYPE(TNewBundleHealthCache)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EReplicaState,
    (GoodSync)
    (BadSync)
    (GoodAsync)
    (BadAsync)
);

ETableReplicaMode GetReplicaModeFromState(EReplicaState state)
{
    switch (state) {
        // TODO(akozhikhov): Support SyncToAsync and AsyncToSync for Chaos.
        case EReplicaState::GoodSync:
        case EReplicaState::BadSync:
            return ETableReplicaMode::Sync;
        case EReplicaState::GoodAsync:
        case EReplicaState::BadAsync:
            return ETableReplicaMode::Async;
        default:
            YT_ABORT();
    }
}

bool IsReplicaStateGood(EReplicaState state)
{
    switch (state) {
        // TODO(akozhikhov): Support SyncToAsync and AsyncToSync for Chaos.
        case EReplicaState::GoodSync:
        case EReplicaState::GoodAsync:
            return true;
        case EReplicaState::BadSync:
        case EReplicaState::BadAsync:
            return false;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TNewReplicatedTableTracker
    : public IReplicatedTableTracker
{
public:
    TNewReplicatedTableTracker(
        IReplicatedTableTrackerHostPtr host,
        TDynamicReplicatedTableTrackerConfigPtr config,
        TProfiler profiler)
        : Host_(std::move(host))
        , RttThread_(New<TActionQueue>("NewRtt"))
        , RttInvoker_(RttThread_->GetInvoker())
        , Profiler_(std::move(profiler))
        , Config_(std::move(config))
        , ClusterClientCache_(CreateClusterClientCache())
        , ClusterLivenessChecker_(Config_->ClusterStateCache, ClusterClientCache_)
        , ClusterSafeModeChecker_(Config_->ClusterStateCache, ClusterClientCache_)
        , HydraReadOnlyChecker_(Config_->ClusterStateCache, ClusterClientCache_)
        , ClusterIncomingReplicationChecker_(Config_->ClusterStateCache, ClusterClientCache_)
        , BundleHealthChecker_(Config_->BundleHealthCache, ClusterClientCache_)
    {
        MaxActionQueueSize_.store(Config_->MaxActionQueueSize);

        Host_->SubscribeReplicatedTableCreated(BIND_NO_PROPAGATE(&TNewReplicatedTableTracker::OnReplicatedTableCreated, MakeWeak(this)));
        Host_->SubscribeReplicatedTableDestroyed(BIND_NO_PROPAGATE(&TNewReplicatedTableTracker::OnReplicatedTableDestroyed, MakeWeak(this)));

        Host_->SubscribeReplicationCollocationCreated(BIND_NO_PROPAGATE(&TNewReplicatedTableTracker::OnReplicationCollocationCreated, MakeWeak(this)));
        Host_->SubscribeReplicationCollocationDestroyed(BIND_NO_PROPAGATE(&TNewReplicatedTableTracker::OnReplicationCollocationDestroyed, MakeWeak(this)));

        Host_->SubscribeReplicaCreated(BIND_NO_PROPAGATE(&TNewReplicatedTableTracker::OnReplicaCreated, MakeWeak(this)));
        Host_->SubscribeReplicaDestroyed(BIND_NO_PROPAGATE(&TNewReplicatedTableTracker::OnReplicaDestroyed, MakeWeak(this)));
    }

    void EnableTracking() override
    {
        TrackingEnabled_.store(true);
    }

    void DisableTracking() override
    {
        TrackingEnabled_.store(false);
    }

    void Initialize() override
    {
        // NB: RTT should always load from snapshot first (and not enqueue any actions till that).
        RequestLoadingFromSnapshot();

        if (!Initialized_.exchange(true)) {
            ScheduleTrackerIteration();
        }
    }

    void RequestLoadingFromSnapshot() override
    {
        auto guard = Guard(ActionQueueLock_);
        RequestLoadingFromSnapshot(guard);
    }

    TError CheckClusterState(const TClusterKey& key)
    {
        if (auto error = ClusterLivenessChecker_.Get(key); !error.IsOK()) {
            return error;
        }
        if (auto error = ClusterSafeModeChecker_.Get(key); !error.IsOK()) {
            return error;
        }
        if (auto error = HydraReadOnlyChecker_.Get(key); !error.IsOK()) {
            return error;
        }
        if (auto error = ClusterIncomingReplicationChecker_.Get(key); !error.IsOK()) {
            return error;
        }

        return {};
    }

    TError CheckBundleHealth(const TBundleHealthKey& key)
    {
        return BundleHealthChecker_.Get(key);
    }

    bool IsReplicaClusterBanned(TStringBuf clusterName) const
    {
        return Config_->ReplicatorHint->BannedReplicaClusters.contains(clusterName);
    }

    const TProfiler& GetProfiler() const
    {
        return Profiler_;
    }

    const TDynamicReplicatedTableTrackerConfigPtr& GetConfig() const
    {
        return Config_;
    }

    class TReplicatedTable;

    class TReplica
    {
    public:
        TReplica(
            TNewReplicatedTableTracker* const tableTracker,
            TReplicatedTable* const replicatedTable,
            TReplicaData data)
            : TableTracker_(tableTracker)
            , ReplicatedTable_(replicatedTable)
            , Id_(data.Id)
            , ClusterName_(std::move(data.ClusterName))
            , TablePath_(std::move(data.TablePath))
            , ContentType_(data.ContentType)
            , Enabled_(data.Enabled)
            , TrackingEnabled_(data.TrackingEnabled)
        {
            State_ = data.Mode == ETableReplicaMode::Sync
                ? EReplicaState::BadSync
                : EReplicaState::BadAsync;
        }

        void UpdateReplicaState()
        {
            auto clientOrError = TableTracker_->ClusterClientCache_->Get(ClusterName_);
            if (!clientOrError.IsOK()) {
                OnCheckFailed(clientOrError);
                return;
            }

            const auto& client = clientOrError.Value();

            if (TableTracker_->IsReplicaClusterBanned(ClusterName_)) {
                OnCheckFailed(TError("Replica cluster is banned"));
                return;
            }

            if (auto error = TableTracker_->CheckClusterState(ClusterName_);
                !error.IsOK() && !WarmingUp_)
            {
                OnCheckFailed(error);
                return;
            }

            if ((!ReplicaLagTime_ || *ReplicaLagTime_ > ReplicatedTable_->GetOptions()->SyncReplicaLagThreshold) &&
                !WarmingUp_)
            {
                OnCheckFailed(TError("Replica lag time threshold exceeded: %v > %v",
                    ReplicaLagTime_,
                    ReplicatedTable_->GetOptions()->SyncReplicaLagThreshold));
                return;
            }

            if (auto error = CheckBundleHealth(client);
                !error.IsOK() && !WarmingUp_)
            {
                OnCheckFailed(error);
                return;
            }

            if (auto error = CheckTableAttributes(client);
                !error.IsOK() && !WarmingUp_)
            {
                OnCheckFailed(error);
                return;
            }

            if (!WarmingUp_) {
                OnCheckSucceeded();
            } else {
                WarmingUp_ = false;
            }
        }

        TReplicatedTable* GetReplicatedTable() const
        {
            return ReplicatedTable_;
        }

        void ResetReplicatedTable()
        {
            YT_VERIFY(ReplicatedTable_);
            ReplicatedTable_ = nullptr;
        }

        EReplicaState GetState() const
        {
            return State_;
        }

        TTableReplicaId GetId() const
        {
            return Id_;
        }

        TStringBuf GetClusterName() const
        {
            return ClusterName_;
        }

        ETableReplicaContentType GetContentType() const
        {
            return ContentType_;
        }

        void SetMode(ETableReplicaMode mode)
        {
            auto oldState = State_;
            ChangeState(IsReplicaStateGood(oldState), mode);
        }

        void SetEnabled(bool enabled)
        {
            Enabled_ = enabled;
        }

        void SetTrackingPolicy(bool enableTracking)
        {
            TrackingEnabled_ = enableTracking;
        }

        bool ShouldTrack() const
        {
            return Enabled_ && TrackingEnabled_ && ReplicatedTable_;
        }

        std::optional<TDuration> GetReplicaLagTime() const
        {
            return ReplicaLagTime_;
        }

        void SetReplicaLagTime(std::optional<TDuration> replicaLagTime)
        {
            ReplicaLagTime_ = replicaLagTime;
        }

        const TCounter& GetReplicaModeSwitchCounter() const
        {
            return ReplicaModeSwitchCounter_;
        }

    private:
        TNewReplicatedTableTracker* const TableTracker_;

        TReplicatedTable* ReplicatedTable_;

        const TTableReplicaId Id_;
        const TString ClusterName_;
        const TYPath TablePath_;
        const ETableReplicaContentType ContentType_;

        bool Enabled_;
        bool TrackingEnabled_;
        EReplicaState State_;
        std::optional<TDuration> ReplicaLagTime_;

        TFuture<TString> BundleNameFuture_ = MakeFuture<TString>(
            TError("Bundle name has not been fetched yet"));
        TErrorOr<TString> CurrentBundleName_ = BundleNameFuture_.Get();
        TInstant LastBundleNameUpdateTime_ = TInstant::Zero();
        i64 IterationsWithoutAcceptableBundleHealth_ = 0;

        TFuture<TYsonString> TableAttributesFuture_ = MakeFuture<TYsonString>(
            TError("Table attributes have not been fetched yet"));
        TErrorOr<TYsonString> CurrentTableAttributes_;
        TInstant LastCompletePreloadTime_ = TInstant::Zero();

        bool WarmingUp_ = true;

        TCounter ReplicaModeSwitchCounter_;


        TError CheckBundleHealth(const NApi::IClientPtr& client)
        {
            auto bundleNameOrError = GetBundleName(client);
            if (!bundleNameOrError.IsOK()) {
                return bundleNameOrError;
            }

            auto bundleHealthOrError = TableTracker_->CheckBundleHealth({ClusterName_, bundleNameOrError.Value()});
            if (!bundleHealthOrError.IsOK()) {
                if (++IterationsWithoutAcceptableBundleHealth_ >
                    TableTracker_->GetConfig()->MaxIterationsWithoutAcceptableBundleHealth)
                {
                    return TError("Tablet cell bundle health check failed for %v times in a row",
                        IterationsWithoutAcceptableBundleHealth_)
                        << bundleHealthOrError;
                }
            } else {
                IterationsWithoutAcceptableBundleHealth_ = 0;
            }

            return {};
        }

        TErrorOr<TString> GetBundleName(const NApi::IClientPtr& client)
        {
            auto now = NProfiling::GetInstant();

            if (!BundleNameFuture_.IsSet()) {
                return CurrentBundleName_;
            }

            auto interval = BundleNameFuture_.Get().IsOK()
                ? ReplicatedTable_->GetOptions()->TabletCellBundleNameTtl
                : ReplicatedTable_->GetOptions()->RetryOnFailureInterval;

            if (LastBundleNameUpdateTime_ + interval < now) {
                LastBundleNameUpdateTime_ = now;
                CurrentBundleName_ = BundleNameFuture_.Get();
                BundleNameFuture_ = client->GetNode(TablePath_ + "/@tablet_cell_bundle")
                    .Apply(BIND([] (const TErrorOr<TYsonString>& bundleNameOrError) {
                        THROW_ERROR_EXCEPTION_IF_FAILED(bundleNameOrError,
                            "Error getting table bundle name");
                        return ConvertTo<TString>(bundleNameOrError.Value());
                    }));

                if (CurrentBundleName_.IsOK()) {
                    ReplicaModeSwitchCounter_ = TableTracker_->GetProfiler()
                        .WithTag("tablet_cell_bundle", CurrentBundleName_.Value())
                        .WithTag("table_path", TablePath_)
                        .WithTag("replica_cluster", ClusterName_)
                        .Counter("/replica_mode_switch_count");
                }

                return CurrentBundleName_;
            }

            return BundleNameFuture_.Get();
        }

        TError CheckTableAttributes(const NApi::IClientPtr& client)
        {
            auto checkPreloadState = ReplicatedTable_->GetOptions()->EnablePreloadStateCheck;

            if (TableAttributesFuture_.IsSet()) {
                CurrentTableAttributes_ = TableAttributesFuture_.Get();

                TGetNodeOptions options;
                if (checkPreloadState) {
                    options.Attributes = {"preload_state"};
                }
                TableAttributesFuture_ = client->GetNode(TablePath_, options);
            }

            if (!CurrentTableAttributes_.IsOK()) {
                return CurrentTableAttributes_;
            }

            if (checkPreloadState) {
                if (auto result = CurrentTableAttributes_.Value()) {
                    auto node = ConvertToNode(result);
                    if (auto preloadState = node->Attributes().Find<NTabletNode::EStorePreloadState>("preload_state")) {
                        auto now = TInstant::Now();
                        if (*preloadState == NTabletNode::EStorePreloadState::Complete) {
                            LastCompletePreloadTime_ = now;
                        }
                        auto incompletePreloadStateDuration = now - LastCompletePreloadTime_;
                        if (incompletePreloadStateDuration > ReplicatedTable_->GetOptions()->IncompletePreloadGracePeriod) {
                            return TError("Table preload is not complete for %v, actual state is %Qlv",
                                incompletePreloadStateDuration,
                                *preloadState);
                        }
                    }
                }
            }

            return CurrentTableAttributes_;
        }

        void OnCheckFailed(const TError& error)
        {
            auto oldState = State_;
            ChangeState(/*good*/ false, GetReplicaModeFromState(oldState));

            YT_LOG_DEBUG(error, "Table replica check failed (ReplicaId: %v, State: %v -> %v)",
                Id_,
                oldState,
                State_);
        }

        void OnCheckSucceeded()
        {
            auto oldState = State_;
            ChangeState(/*good*/ true, GetReplicaModeFromState(oldState));

            if (oldState != State_) {
                YT_LOG_DEBUG("Table replica check succeeded (ReplicaId: %v, State: %v -> %v)",
                    Id_,
                    oldState,
                    State_);
            }
        }

        void ChangeState(bool good, ETableReplicaMode mode)
        {
            if (good) {
                switch(mode) {
                    case ETableReplicaMode::Sync:
                        State_ = EReplicaState::GoodSync;
                        break;
                    case ETableReplicaMode::Async:
                        State_ = EReplicaState::GoodAsync;
                        break;
                    default:
                        YT_ABORT();
                }
            } else {
                switch(mode) {
                    case ETableReplicaMode::Sync:
                        State_ = EReplicaState::BadSync;
                        break;
                    case ETableReplicaMode::Async:
                        State_ = EReplicaState::BadAsync;
                        break;
                    default:
                        YT_ABORT();
                }
            }
        }
    };

    TReplica* GetReplica(TTableReplicaId replicaId)
    {
        YT_VERIFY(IsTableReplicaType(TypeFromId(replicaId)));
        return &GetOrCrash(IdToReplica_, replicaId);
    }

    TReplica* FindReplica(TTableReplicaId replicaId)
    {
        auto it = IdToReplica_.find(replicaId);
        return it == IdToReplica_.end()
            ? nullptr
            : &it->second;
    }

    struct TReplicaIdFormatter
    {
        void operator()(TStringBuilderBase *builder, const TReplica* replica) const
        {
            FormatValue(builder, replica->GetId(), TStringBuf());
        }
    };

    using TReplicaList = TCompactVector<TReplica*, TypicalTableReplicaCount>;
    using TReplicasByState = TEnumIndexedVector<EReplicaState, TReplicaList>;

    class TReplicatedTable
    {
    public:
        TReplicatedTable(
            TNewReplicatedTableTracker* tableTracker,
            TReplicatedTableData data)
            : TableTracker_(tableTracker)
            , Id_(data.Id)
            , Options_(std::move(data.Options))
            , CollocationId_(NullObjectId)
        { }

        const TReplicatedTableOptionsPtr& GetOptions() const
        {
            return Options_;
        }

        TTableId GetId() const
        {
            return Id_;
        }

        void SetOptions(TReplicatedTableOptionsPtr options)
        {
            Options_ = std::move(options);
        }

        void UpdateReplicaStates()
        {
            for (auto replicaId : ReplicaIds_) {
                auto* replica = TableTracker_->GetReplica(replicaId);
                if (replica->ShouldTrack()) {
                    replica->UpdateReplicaState();
                }
            }
        }

        TTableCollocationId GetCollocationId() const
        {
            return CollocationId_;
        }

        void SetCollocationId(TTableCollocationId collocationId)
        {
            CollocationId_ = collocationId;
        }

        TReplicasByState GroupReplicasByTargetState(ETableReplicaContentType contentType)
        {
            TReplicasByState replicasByState;
            int trackedReplicaCount = 0;
            for (auto replicaId : ReplicaIds_) {
                auto* replica = TableTracker_->GetReplica(replicaId);
                if (replica->ShouldTrack() && contentType == replica->GetContentType()) {
                    ++trackedReplicaCount;
                    replicasByState[replica->GetState()].push_back(replica);
                }
            }

            if (trackedReplicaCount == 0) {
                return replicasByState;
            }

            i64 minSyncReplicaCount;
            i64 maxSyncReplicaCount;
            std::tie(minSyncReplicaCount, maxSyncReplicaCount) = Options_->GetEffectiveMinMaxReplicaCount(trackedReplicaCount);
            if (contentType == ETableReplicaContentType::Queue) {
                minSyncReplicaCount = std::max(minSyncReplicaCount, 1L);
                maxSyncReplicaCount = std::max(maxSyncReplicaCount, 2L);
            }

            // Replicas with larger lag go first.
            Sort(
                replicasByState[EReplicaState::GoodAsync],
                [] (const auto* leftReplica, const auto* rightReplica) {
                    YT_VERIFY(leftReplica->GetReplicaLagTime() && rightReplica->GetReplicaLagTime());
                    return leftReplica->GetReplicaLagTime() > rightReplica->GetReplicaLagTime();
                });

            auto changeReplicaState = [&] (EReplicaState from, EReplicaState to) {
                auto* replica = replicasByState[from].back();
                replicasByState[from].pop_back();
                replicasByState[to].push_back(replica);
            };

            i64 currentSyncReplicaCount = std::min(maxSyncReplicaCount, std::ssize(replicasByState[EReplicaState::GoodSync]));
            while (currentSyncReplicaCount < maxSyncReplicaCount && !replicasByState[EReplicaState::GoodAsync].empty()) {
                changeReplicaState(EReplicaState::GoodAsync, EReplicaState::GoodSync);
                ++currentSyncReplicaCount;
            }

            i64 untouchedBadSyncReplicaCount = std::max(0L, minSyncReplicaCount - currentSyncReplicaCount);
            while (untouchedBadSyncReplicaCount < std::ssize(replicasByState[EReplicaState::BadSync])) {
                changeReplicaState(EReplicaState::BadSync, EReplicaState::BadAsync);
            }

            while (maxSyncReplicaCount < std::ssize(replicasByState[EReplicaState::GoodSync])) {
                changeReplicaState(EReplicaState::GoodSync, EReplicaState::GoodAsync);
            }

            YT_LOG_DEBUG("Grouped replicas by target state (TableId: %v, ContentType: %v, GoodSync: %v, BadSync: %v, GoodAsync: %v, BadAsync: %v)",
                Id_,
                contentType,
                MakeFormattableView(replicasByState[EReplicaState::GoodSync], TReplicaIdFormatter()),
                MakeFormattableView(replicasByState[EReplicaState::BadSync], TReplicaIdFormatter()),
                MakeFormattableView(replicasByState[EReplicaState::GoodAsync], TReplicaIdFormatter()),
                MakeFormattableView(replicasByState[EReplicaState::BadAsync], TReplicaIdFormatter()));

            return replicasByState;
        }

        THashSet<TTableReplicaId>* GetReplicaIds()
        {
            return &ReplicaIds_;
        }

    private:
        TNewReplicatedTableTracker* const TableTracker_;

        const TTableId Id_;

        TReplicatedTableOptionsPtr Options_;

        TTableCollocationId CollocationId_;
        THashSet<TTableReplicaId> ReplicaIds_;
    };

private:
    const IReplicatedTableTrackerHostPtr Host_;

    const TActionQueuePtr RttThread_;
    const IInvokerPtr RttInvoker_;
    const TProfiler Profiler_;

    std::atomic<bool> Initialized_ = false;
    std::atomic<bool> TrackingEnabled_ = false;

    TDynamicReplicatedTableTrackerConfigPtr Config_;

    std::atomic<i64> MaxActionQueueSize_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ActionQueueLock_);
    std::deque<TClosure> ActionQueue_;

    struct TTableCollocation
    {
        THashSet<TTableId> TableIds;
    };

    THashMap<TTableId, TReplicatedTable> IdToTable_;
    THashMap<TTableReplicaId, TReplica> IdToReplica_;
    THashMap<TTableCollocationId, TTableCollocation> IdToCollocation_;

    const TClusterClientCachePtr ClusterClientCache_;

    template <typename TCache>
    class TCacheSynchronousAdapter
    {
    public:
        using TKey = typename TCache::KeyType;
        using TValue = typename TCache::ValueType;

        TCacheSynchronousAdapter(
            TAsyncExpiringCacheConfigPtr config,
            TClusterClientCachePtr clusterClientCache)
            : Cache_(New<TCache>(
                std::move(config),
                std::move(clusterClientCache)))
        { }

        TErrorOr<TValue> Get(const TKey& key)
        {
            auto& state = States_[key];

            if (state.Future.IsSet()) {
                state.CurrentError = state.Future.Get();
                state.Future = Cache_->Get(key);
            }

            return state.CurrentError;
        }

        void Reconfigure(const TAsyncExpiringCacheConfigPtr& config) const
        {
            Cache_->Reconfigure(config);
        }

    private:
        struct TState
        {
            TFuture<TValue> Future = MakeFuture<TValue>(TError("No result yet"));
            TErrorOr<TValue> CurrentError;
        };

        const TIntrusivePtr<TCache> Cache_;

        THashMap<TKey, TState> States_;
    };

    TCacheSynchronousAdapter<TClusterLivenessCheckCache> ClusterLivenessChecker_;
    TCacheSynchronousAdapter<TClusterSafeModeCheckCache> ClusterSafeModeChecker_;
    TCacheSynchronousAdapter<THydraReadOnlyCheckCache> HydraReadOnlyChecker_;
    TCacheSynchronousAdapter<TClusterIncomingReplicationCheckCache> ClusterIncomingReplicationChecker_;

    TCacheSynchronousAdapter<TNewBundleHealthCache> BundleHealthChecker_;

    TFuture<TReplicaLagTimes> ReplicaLagTimesFuture_ = MakeFuture<TReplicaLagTimes>(TError("No result yet"));
    TErrorOr<TReplicaLagTimes> ReplicaLagTimesOrError_;


    TClusterClientCachePtr CreateClusterClientCache() const
    {
        return New<TClusterClientCache>(
            BIND([weakThis_ = MakeWeak(this)] (TString clusterName) -> TErrorOr<NApi::IClientPtr> {
                auto tracker = weakThis_.Lock();
                if (!tracker) {
                    return TError("Replicated table tracker was destroyed");
                }

                YT_LOG_DEBUG("Creating client for (Cluster: %v)",
                    clusterName);

                if (auto client = tracker->Host_->CreateClusterClient(clusterName)) {
                    return client;
                }

                return TError("No client is available");
            }),
            Config_->ClientExpirationTime,
            RttInvoker_);
    }

    void ScheduleTrackerIteration()
    {
        TDelayedExecutor::Submit(
            BIND(&TNewReplicatedTableTracker::RunTrackerIteration, MakeWeak(this))
                .Via(RttInvoker_),
            Config_->CheckPeriod);
    }

    void RunTrackerIteration()
    {
        if (!AreNodesEqual(ConvertToNode(Config_), ConvertToNode(Host_->GetConfig()))) {
            ApplyConfigChange(Host_->GetConfig());
        }

        if (!UseNewReplicatedTableTracker(Config_)) {
            YT_LOG_DEBUG("New replicated table tracker is disabled");
            ScheduleTrackerIteration();
            return;
        }

        {
            TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("NewRtt"));

            try {
                DrainActionQueue();

                if (TrackingEnabled_.load() &&
                    Config_->EnableReplicatedTableTracker)
                {
                    UpdateReplicaLagTimes();
                    UpdateReplicaStates();
                    UpdateReplicaModes();
                } else {
                    YT_LOG_INFO("Replicated table tracker is disabled");
                }
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Replicated table tracker iteration failed");
            }
        }

        ScheduleTrackerIteration();
    }

    void DrainActionQueue()
    {
        while (true) {
            auto guard = Guard(ActionQueueLock_);

            if (Host_->LoadingFromSnapshotRequested()) {
                ActionQueue_.clear();
                guard.Release();

                YT_LOG_DEBUG("Replicated table tracker started loading from snapshot");

                // NB: RTT thread is blocked intentionally here.
                auto snapshot = Host_->GetSnapshot()
                    .Get()
                    .ValueOrThrow();
                DoLoadFromSnapshot(std::move(snapshot));

                YT_LOG_DEBUG("Replicated table tracker finished loading from snapshot");

                continue;
            }

            if (ActionQueue_.empty()) {
                break;
            }

            auto action = std::move(ActionQueue_.front());
            ActionQueue_.pop_front();
            guard.Release();

            action();
        }
    }

    void UpdateReplicaStates()
    {
        int trackedTableCount = 0;
        for (auto& [_, table] : IdToTable_) {
            if (!table.GetOptions()->EnableReplicatedTableTracker) {
                continue;
            }
            table.UpdateReplicaStates();
            ++trackedTableCount;
        }

        YT_LOG_DEBUG("Replicated table tracker check iteration finished "
            "(TrackedTableCount: %v, TableCount: %v)",
            trackedTableCount,
            IdToTable_.size());
    }

    void UpdateReplicaModes()
    {
        std::vector<TChangeReplicaModeCommand> commands;

        struct TReplicaFamily
        {
            TTableId TableId;
            ETableReplicaContentType ContentType;

            bool operator == (const TReplicaFamily& other) const
            {
                return TableId == other.TableId && ContentType == other.ContentType;
            }

            operator size_t() const
            {
                return MultiHash(TableId, ContentType);
            }
        };

        THashMap<TReplicaFamily, TReplicasByState> replicaFamilyToReplicasByState;
        for (auto& [tableId, table] : IdToTable_) {
            if (!table.GetOptions()->EnableReplicatedTableTracker) {
                continue;
            }

            for (auto contentType : TEnumTraits<ETableReplicaContentType>::GetDomainValues()) {
                EmplaceOrCrash(
                    replicaFamilyToReplicasByState,
                    TReplicaFamily{
                        .TableId = tableId,
                        .ContentType = contentType
                    },
                    table.GroupReplicasByTargetState(contentType));
            }
        }

        THashMap<ETableReplicaContentType, THashMap<TTableCollocationId, THashMap<TString, int>>> collocationIdToClusterPriorities;
        for (const auto& [collocationId, collocation] : IdToCollocation_) {
            for (auto contentType : TEnumTraits<ETableReplicaContentType>::GetDomainValues()) {
                std::optional<THashSet<TStringBuf>> goodReplicaClusters;

                for (auto tableId : collocation.TableIds) {
                    auto it = replicaFamilyToReplicasByState.find(TReplicaFamily{
                        .TableId = tableId,
                        .ContentType = contentType
                    });
                    if (it != replicaFamilyToReplicasByState.end()) {
                        THashSet<TStringBuf> tableGoodReplicaClusters;
                        for (auto* replica : it->second[EReplicaState::GoodSync]) {
                            tableGoodReplicaClusters.insert(replica->GetClusterName());
                        }
                        for (auto* replica : it->second[EReplicaState::GoodAsync]) {
                            tableGoodReplicaClusters.insert(replica->GetClusterName());
                        }

                        if (!goodReplicaClusters) {
                            goodReplicaClusters = tableGoodReplicaClusters;
                        } else {
                            for (auto it = goodReplicaClusters->begin(); it != goodReplicaClusters->end();) {
                                auto nextIt = std::next(it);
                                if (!tableGoodReplicaClusters.contains(*it)) {
                                    goodReplicaClusters->erase(it);
                                }
                                it = nextIt;
                            }
                        }
                    }
                }

                YT_LOG_DEBUG("Identified good clusters for collocation (CollocationId: %v, ContentType: %v, GoodClusters: %v)",
                    collocationId,
                    contentType,
                    goodReplicaClusters);

                auto it = EmplaceOrCrash(collocationIdToClusterPriorities[contentType], collocationId, THashMap<TString, int>());
                if (goodReplicaClusters) {
                    for (auto clusterName : *goodReplicaClusters) {
                        it->second[clusterName] = 2;
                    }
                }
            }
        }

        for (auto& [replicaFamily, replicasByState] : replicaFamilyToReplicasByState) {
            auto& table = GetOrCrash(IdToTable_, replicaFamily.TableId);
            std::optional<THashMap<TString, int>> replicaClusterPriorities;
            const auto& preferredSyncReplicaClusters = table.GetOptions()->PreferredSyncReplicaClusters;
            if (preferredSyncReplicaClusters) {
                replicaClusterPriorities.emplace();
            }
            if (table.GetCollocationId() != NullObjectId) {
                replicaClusterPriorities = GetOrCrash(collocationIdToClusterPriorities[replicaFamily.ContentType], table.GetCollocationId());
            }

            if (preferredSyncReplicaClusters) {
                for (const auto& clusterName : *preferredSyncReplicaClusters) {
                    ++(*replicaClusterPriorities)[clusterName];
                }
            }

            auto tableCommands = GenerateCommandsForTable(&replicasByState, replicaClusterPriorities);
            YT_LOG_DEBUG_IF(!tableCommands.empty(),
                "Generated replica mode change commands "
                "(TableId: %v, ContentType: %v, CollocationId: %v, Commands: %v)",
                replicaFamily.TableId,
                replicaFamily.ContentType,
                table.GetCollocationId(),
                tableCommands);

            std::move(tableCommands.begin(), tableCommands.end(), std::back_inserter(commands));
        }

        YT_LOG_DEBUG("Replicated table tracker replica mode update iteration finished (UpdateCommandCount: %v)",
            commands.size());

        if (commands.empty()) {
            return;
        }

        Host_->ApplyChangeReplicaModeCommands(std::move(commands)).Subscribe(BIND(
            [=] (const TErrorOr<TApplyChangeReplicaCommandResults>& resultsOrError)
            {
                if (!resultsOrError.IsOK()) {
                    YT_LOG_ERROR(resultsOrError, "Failed to apply change replica mode commands");
                    return;
                }

                bool failed = false;
                for (const auto& commandResultOrError : resultsOrError.Value()) {
                    if (!commandResultOrError.IsOK()) {
                        YT_LOG_ERROR(commandResultOrError, "Failed to apply change replica mode command");
                        failed = true;
                    }
                }

                if (failed) {
                    return;
                }

                YT_LOG_DEBUG("Successfully applied change replica mode commands (CommandCount: %v)",
                    resultsOrError.Value().size());
            })
            .Via(RttInvoker_));
    }

    std::vector<TChangeReplicaModeCommand> GenerateCommandsForTable(
        TReplicasByState* replicasByState,
        const std::optional<THashMap<TString, int>>& replicaClusterPriorities)
    {
        auto& syncReplicas = (*replicasByState)[EReplicaState::GoodSync];
        auto& asyncReplicas = (*replicasByState)[EReplicaState::GoodAsync];
        auto syncReplicaCount = syncReplicas.size();

        if (replicaClusterPriorities && !syncReplicas.empty() && !asyncReplicas.empty()) {
            auto getPriority = [&] (auto* replica) {
                auto it = replicaClusterPriorities->find(replica->GetClusterName());
                return it != replicaClusterPriorities->end()
                    ? std::make_pair(it->second, it->first)
                    : std::make_pair(0, replica->GetClusterName());
            };

            SortBy(syncReplicas, getPriority);
            SortBy(asyncReplicas, getPriority);

            TReplicaList newSyncReplicas;
            TReplicaList newAsyncReplicas;

            while (!syncReplicas.empty() || !asyncReplicas.empty()) {
                TReplicaList* oldReplicas;
                if (syncReplicas.empty()) {
                    oldReplicas = &asyncReplicas;
                } else if (asyncReplicas.empty()) {
                    oldReplicas = &syncReplicas;
                } else if (getPriority(syncReplicas.back()) >= getPriority(asyncReplicas.back())) {
                    oldReplicas = &syncReplicas;
                } else {
                    oldReplicas = &asyncReplicas;
                }

                TReplicaList* newReplicas = newSyncReplicas.size() < syncReplicaCount
                    ? &newSyncReplicas
                    : &newAsyncReplicas;

                newReplicas->push_back(oldReplicas->back());
                oldReplicas->pop_back();
            }

            syncReplicas = std::move(newSyncReplicas);
            asyncReplicas = std::move(newAsyncReplicas);
        }

        std::vector<TChangeReplicaModeCommand> commands;
        for (auto state : TEnumTraits<EReplicaState>::GetDomainValues()) {
            for (auto* replica : (*replicasByState)[state]) {
                auto currentMode = GetReplicaModeFromState(replica->GetState());
                auto targetMode = GetReplicaModeFromState(state);
                if (currentMode != targetMode) {
                    commands.push_back(TChangeReplicaModeCommand{
                        .ReplicaId = replica->GetId(),
                        .TargetMode = targetMode
                    });
                    replica->GetReplicaModeSwitchCounter().Increment();
                }
            }
        }

        return commands;
    }

    void UpdateReplicaLagTimes()
    {
        if (ReplicaLagTimesFuture_.IsSet()) {
            ReplicaLagTimesOrError_ = ReplicaLagTimesFuture_.Get();
            ReplicaLagTimesFuture_ = Host_->ComputeReplicaLagTimes(GetKeys(IdToReplica_));
        }

        if (!ReplicaLagTimesOrError_.IsOK()) {
            YT_LOG_ERROR(ReplicaLagTimesOrError_, "Failed to compute replica lag times");
            return;
        }

        int updateCount = 0;
        for (auto [replicaId, lagTime] : ReplicaLagTimesOrError_.Value()) {
            auto it = IdToReplica_.find(replicaId);
            if (it != IdToReplica_.end()) {
                ++updateCount;
                it->second.SetReplicaLagTime(lagTime);
            }
        }

        YT_LOG_DEBUG("Successfully updated replica lag times (RequestedReplicaCount: %v, UpdatedReplicaCount: %v)",
            IdToReplica_.size(),
            updateCount);
    }

    TReplicatedTable* GetTable(TTableId tableId)
    {
        YT_VERIFY(IsReplicatedTableType(TypeFromId(tableId)));
        return &GetOrCrash(IdToTable_, tableId);
    }

    TReplicatedTable* FindTable(TTableId tableId)
    {
        auto it = IdToTable_.find(tableId);
        return it == IdToTable_.end()
            ? nullptr
            : &it->second;
    }

    template <typename TAction>
    void EnqueueAction(TAction action)
    {
        if (!Initialized_.load()) {
            return;
        }

        auto guard = Guard(ActionQueueLock_);

        if (Host_->LoadingFromSnapshotRequested()) {
            return;
        }

        if (std::ssize(ActionQueue_) >= MaxActionQueueSize_.load()) {
            RequestLoadingFromSnapshot(guard);
            guard.Release();
            YT_LOG_WARNING("Action queue is reset due to overflow (MaxActionQueueSize: %v)",
                MaxActionQueueSize_.load());
            return;
        }

        ActionQueue_.push_back(std::move(action));
    }

    void OnReplicatedTableCreated(TReplicatedTableData data)
    {
        EnqueueAction(BIND([=, this, this_ = MakeStrong(this), data = std::move(data)] {
            auto tableId = data.Id;

            YT_VERIFY(IsReplicatedTableType(TypeFromId(tableId)));

            auto it = IdToTable_.find(tableId);
            if (it == IdToTable_.end()) {
                YT_LOG_DEBUG("Replicated table created (TableId: %v, Options: %v)",
                    tableId,
                    ConvertToYsonString(data.Options, EYsonFormat::Text).AsStringBuf());

                auto it = EmplaceOrCrash(
                    IdToTable_,
                    tableId,
                    TReplicatedTable(this, std::move(data)));

                std::optional<TTableCollocationId> tableCollocationId;
                for (const auto& [collocationId, collocation] : IdToCollocation_) {
                    if (collocation.TableIds.contains(tableId)) {
                        YT_VERIFY(!tableCollocationId);
                        tableCollocationId = collocationId;
                    }
                }

                if (tableCollocationId) {
                    it->second.SetCollocationId(*tableCollocationId);
                }
            } else {
                YT_LOG_DEBUG("Replicated table updated (TableId: %v, Options: %v)",
                    tableId,
                    ConvertToYsonString(data.Options, EYsonFormat::Text).AsStringBuf());

                it->second.SetOptions(std::move(data.Options));
            }
        }));
    }

    void OnReplicatedTableDestroyed(TTableId tableId)
    {
        EnqueueAction(BIND([=, this, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Replicated table destroyed (TableId: %v)",
                tableId);

            auto* table = GetTable(tableId);
            for (auto replicaId : *table->GetReplicaIds()) {
                GetReplica(replicaId)->ResetReplicatedTable();
            }
            if (table->GetCollocationId() != NullObjectId) {
                auto& collocation = GetOrCrash(IdToCollocation_, table->GetCollocationId());
                EraseOrCrash(collocation.TableIds, tableId);
            }
            EraseOrCrash(IdToTable_, tableId);
        }));
    }

    void OnReplicaCreated(TReplicaData data)
    {
        EnqueueAction(BIND([=, this, this_ = MakeStrong(this), data = std::move(data)] {
            auto tableId = data.TableId;
            auto replicaId = data.Id;

            auto replicaType = TypeFromId(replicaId);
            YT_VERIFY(IsTableReplicaType(replicaType));

            switch (replicaType) {
                case EObjectType::TableReplica:
                    YT_VERIFY(data.ContentType == ETableReplicaContentType::Data);
                    break;
                case EObjectType::ChaosTableReplica:
                    YT_VERIFY(data.ContentType == ETableReplicaContentType::Data ||
                        data.ContentType == ETableReplicaContentType::Queue);
                    break;
                default:
                    YT_ABORT();
            }

            auto* table = GetTable(tableId);

            auto formattedParameters = Format(
                "TableId: %v, ReplicaId: %v, Mode: %v, Enabled: %v, ClusterName: %v, TablePath: %v, ContentType: %v",
                tableId,
                replicaId,
                data.Mode,
                data.Enabled,
                data.ClusterName,
                data.TablePath,
                data.ContentType);

            auto it = IdToReplica_.find(replicaId);
            if (it == IdToReplica_.end()) {
                YT_LOG_DEBUG("Table replica created (%v)",
                    formattedParameters);

                EmplaceOrCrash(
                    IdToReplica_,
                    replicaId,
                    TReplica(this, table, std::move(data)));
                EmplaceOrCrash(*table->GetReplicaIds(), replicaId);
            } else {
                YT_LOG_DEBUG("Table replica updated (%v)",
                    formattedParameters);

                it->second.SetMode(data.Mode);
                it->second.SetEnabled(data.Enabled);
                it->second.SetTrackingPolicy(data.TrackingEnabled);
            }
        }));
    }

    void OnReplicaDestroyed(TTableReplicaId replicaId)
    {
        EnqueueAction(BIND([=, this, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Table replica destroyed (ReplicaId: %v)",
                replicaId);

            YT_VERIFY(IsTableReplicaType(TypeFromId(replicaId)));

            if (auto* replica = FindReplica(replicaId)) {
                if (auto* replicatedTable = replica->GetReplicatedTable()) {
                    EraseOrCrash(*GetTable(replicatedTable->GetId())->GetReplicaIds(), replicaId);
                }
                EraseOrCrash(IdToReplica_, replicaId);
            }
        }));
    }

    void OnReplicationCollocationCreated(TTableCollocationData data)
    {
        EnqueueAction(BIND([=, this, this_ = MakeStrong(this), data = std::move(data)] {
            YT_LOG_DEBUG("Replication collocation created (CollocationId: %v)",
                data.Id);

            YT_VERIFY(IsCollocationType(TypeFromId(data.Id)));

            auto [it, _] = IdToCollocation_.try_emplace(data.Id);
            for (auto tableId : it->second.TableIds) {
                if (auto* table = FindTable(tableId)) {
                    table->SetCollocationId(NullObjectId);
                }
            }

            it->second.TableIds.clear();

            for (auto tableId : data.TableIds) {
                EmplaceOrCrash(it->second.TableIds, tableId);
                if (auto* table = FindTable(tableId)) {
                    table->SetCollocationId(data.Id);
                }
            }
        }));
    }

    void OnReplicationCollocationDestroyed(TTableCollocationId collocationId)
    {
        EnqueueAction(BIND([=, this, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Replication collocation destroyed (CollocationId: %v)",
                collocationId);

            YT_VERIFY(IsCollocationType(TypeFromId(collocationId)));

            const auto& collocation = GetOrCrash(IdToCollocation_, collocationId);
            for (auto tableId : collocation.TableIds) {
                if (auto* table = FindTable(tableId)) {
                    table->SetCollocationId(NullObjectId);
                }
            }

            EraseOrCrash(IdToCollocation_, collocationId);
        }));
    }

    void ApplyConfigChange(TDynamicReplicatedTableTrackerConfigPtr config)
    {
        YT_LOG_DEBUG("Applying config change (NewConfig: %v)",
            ConvertToYsonString(config, EYsonFormat::Text));

        if (UseNewReplicatedTableTracker(Config_) != UseNewReplicatedTableTracker(config)) {
            YT_LOG_DEBUG("New replicated table tracker is turned on; will load state from snapshot");
            RequestLoadingFromSnapshot();
        }

        Config_ = std::move(config);

        ClusterLivenessChecker_.Reconfigure(Config_->ClusterStateCache);
        ClusterSafeModeChecker_.Reconfigure(Config_->ClusterStateCache);
        HydraReadOnlyChecker_.Reconfigure(Config_->ClusterStateCache);
        ClusterIncomingReplicationChecker_.Reconfigure(Config_->ClusterStateCache);
        BundleHealthChecker_.Reconfigure(Config_->BundleHealthCache);

        MaxActionQueueSize_.store(Config_->MaxActionQueueSize);
    }

    void RequestLoadingFromSnapshot(const TGuard<NThreading::TSpinLock>& /*guard*/)
    {
        Host_->RequestLoadingFromSnapshot();
        ActionQueue_.clear();
    }

    void DoLoadFromSnapshot(TReplicatedTableTrackerSnapshot snapshot)
    {
        IdToTable_.clear();
        IdToReplica_.clear();
        IdToCollocation_.clear();

        for (auto& tableData : snapshot.ReplicatedTables) {
            auto tableId = tableData.Id;
            EmplaceOrCrash(
                IdToTable_,
                tableId,
                TReplicatedTable(this, std::move(tableData)));
        }

        for (auto& replicaData : snapshot.Replicas) {
            auto tableId = replicaData.TableId;
            YT_VERIFY(IsReplicatedTableType(TypeFromId(tableId)));
            auto replicaId = replicaData.Id;
            YT_VERIFY(IsTableReplicaType(TypeFromId(replicaId)));

            auto* table = GetTable(tableId);
            EmplaceOrCrash(
                IdToReplica_,
                replicaId,
                TReplica(this, table, std::move(replicaData)));
            EmplaceOrCrash(*table->GetReplicaIds(), replicaId);
        }

        for (const auto& collocationData : snapshot.Collocations) {
            auto collocationId = collocationData.Id;
            YT_VERIFY(IsCollocationType(TypeFromId(collocationId)));

            auto it = EmplaceOrCrash(IdToCollocation_, collocationId, TTableCollocation{});
            for (auto tableId : collocationData.TableIds) {
                EmplaceOrCrash(it->second.TableIds, tableId);
                if (auto* table = FindTable(tableId)) {
                    table->SetCollocationId(collocationId);
                }
            }
        }
    }

    bool UseNewReplicatedTableTracker(const TDynamicReplicatedTableTrackerConfigPtr& config) const
    {
        return Host_->AlwaysUseNewReplicatedTableTracker() || config->UseNewReplicatedTableTracker;
    }
};

////////////////////////////////////////////////////////////////////////////////

IReplicatedTableTrackerPtr CreateReplicatedTableTracker(
    IReplicatedTableTrackerHostPtr host,
    TDynamicReplicatedTableTrackerConfigPtr config,
    TProfiler profiler)
{
    return New<TNewReplicatedTableTracker>(
        std::move(host),
        std::move(config),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
