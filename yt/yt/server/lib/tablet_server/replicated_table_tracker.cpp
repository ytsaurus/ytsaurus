#include "replicated_table_tracker.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/collection_helpers.h>

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

struct TClusterKey
{
    NApi::IClientPtr Client;
    TString ClusterName; // for diagnostics only

    bool operator == (const TClusterKey& other) const
    {
        return Client == other.Client;
    }

    operator size_t() const
    {
        return MultiHash(Client);
    }
};

void FormatValue(TStringBuilderBase* builder, const TClusterKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", key.ClusterName);
}

TString ToString(const TClusterKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClusterLivenessCheckCache)

class TClusterLivenessCheckCache
    : public TAsyncExpiringCache<TClusterKey, void>
{
public:
    explicit TClusterLivenessCheckCache(TAsyncExpiringCacheConfigPtr config)
        : TAsyncExpiringCache(
            std::move(config),
            Logger.WithTag("Cache: ClusterLivenessCheck"))
    { }

protected:
    TFuture<void> DoGet(
        const TClusterKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        NApi::TCheckClusterLivenessOptions options{
            .CheckCypressRoot = true,
            .CheckSecondaryMasterCells = true,
        };
        return key.Client->CheckClusterLiveness(options)
            .Apply(BIND([clusterName = key.ClusterName] (const TError& result) {
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error checking cluster %Qv liveness",
                    clusterName);
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterLivenessCheckCache)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClusterSafeModeCheckCache)

class TClusterSafeModeCheckCache
    : public TAsyncExpiringCache<TClusterKey, void>
{
public:
    explicit TClusterSafeModeCheckCache(TAsyncExpiringCacheConfigPtr config)
        : TAsyncExpiringCache(
            std::move(config),
            Logger.WithTag("Cache: ClusterSafeModeCheck"))
    { }

protected:
    TFuture<void> DoGet(
        const TClusterKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return key.Client->GetNode("//sys/@config/enable_safe_mode")
            .Apply(BIND([clusterName = key.ClusterName] (const TErrorOr<TYsonString>& error) {
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    error,
                    "Error getting @enable_safe_mode attribute for cluster %Qv",
                    clusterName);
                if (ConvertTo<bool>(error.Value())) {
                    THROW_ERROR_EXCEPTION("Safe mode is enabled for cluster %Qv",
                        clusterName);
                }
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterSafeModeCheckCache)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THydraReadOnlyCheckCache)

class THydraReadOnlyCheckCache
    : public TAsyncExpiringCache<TClusterKey, void>
{
public:
    explicit THydraReadOnlyCheckCache(TAsyncExpiringCacheConfigPtr config)
        : TAsyncExpiringCache(
            std::move(config),
            Logger.WithTag("Cache: HydraReadOnlyCheck"))
    { }

protected:
    TFuture<void> DoGet(
        const TClusterKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return key.Client->GetNode("//sys/@hydra_read_only")
            .Apply(BIND([clusterName = key.ClusterName] (const TErrorOr<TYsonString>& error) {
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    error,
                    "Error getting @hydra_read_only attribute for cluster %Qv",
                    clusterName);
                if (ConvertTo<bool>(error.Value())) {
                    THROW_ERROR_EXCEPTION("Hydra read only mode is activated for cluster %Qv",
                        clusterName);
                }
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(THydraReadOnlyCheckCache)

////////////////////////////////////////////////////////////////////////////////

struct TBundleHealthKey
{
    NApi::IClientPtr Client;
    TString ClusterName; // for diagnostics only
    TString BundleName;

    bool operator == (const TBundleHealthKey& other) const
    {
        return Client == other.Client &&
            BundleName == other.BundleName;
    }

    operator size_t() const
    {
        return MultiHash(
            Client,
            BundleName);
    }
};

void FormatValue(TStringBuilderBase* builder, const TBundleHealthKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v@%v",
        key.BundleName,
        key.ClusterName);
}

TString ToString(const TBundleHealthKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBundleHealthCache)

class TBundleHealthCache
    : public TAsyncExpiringCache<TBundleHealthKey, ETabletCellHealth>
{
public:
    explicit TBundleHealthCache(TAsyncExpiringCacheConfigPtr config)
        : TAsyncExpiringCache(
            std::move(config),
            Logger.WithTag("Cache: BundleHealth"))
    { }

protected:
    TFuture<ETabletCellHealth> DoGet(
        const TBundleHealthKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return key.Client->GetNode("//sys/tablet_cell_bundles/" + ToYPathLiteral(key.BundleName) + "/@health")
            // TODO(akozhikhov): Get rid of ToUncancelable here?
            .ToUncancelable()
            .Apply(BIND([] (const TErrorOr<TYsonString>& error) {
                return ConvertTo<ETabletCellHealth>(error.ValueOrThrow());
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(TBundleHealthCache)

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
        TDynamicReplicatedTableTrackerConfigPtr config)
        : Host_(std::move(host))
        , RttThread_(New<TActionQueue>("NewRTT"))
        , RttInvoker_(RttThread_->GetInvoker())
        , Config_(std::move(config))
        , ClusterLivenessChecker_(Config_->ClusterStateCache)
        , ClusterSafeModeChecker_(Config_->ClusterStateCache)
        , HydraReadOnlyChecker_(Config_->ClusterStateCache)
        , BundleHealthChecker_(Config_->BundleHealthCache)
    {
        MaxActionQueueSize_.store(Config_->MaxActionQueueSize);

        Host_->SubscribeReplicatedTableCreated(BIND(&TNewReplicatedTableTracker::OnReplicatedTableCreated, MakeWeak(this)));
        Host_->SubscribeReplicatedTableDestroyed(BIND(&TNewReplicatedTableTracker::OnReplicatedTableDestroyed, MakeWeak(this)));
        Host_->SubscribeReplicatedTableOptionsUpdated(BIND(&TNewReplicatedTableTracker::OnReplicatedTableOptionsUpdated, MakeWeak(this)));

        Host_->SubscribeReplicationCollocationUpdated(BIND(&TNewReplicatedTableTracker::OnReplicationCollocationUpdated, MakeWeak(this)));
        Host_->SubscribeReplicationCollocationDestroyed(BIND(&TNewReplicatedTableTracker::OnReplicationCollocationDestroyed, MakeWeak(this)));

        Host_->SubscribeReplicaCreated(BIND(&TNewReplicatedTableTracker::OnReplicaCreated, MakeWeak(this)));
        Host_->SubscribeReplicaDestroyed(BIND(&TNewReplicatedTableTracker::OnReplicaDestroyed, MakeWeak(this)));
        Host_->SubscribeReplicaModeUpdated(BIND(&TNewReplicatedTableTracker::OnReplicaModeUpdated, MakeWeak(this)));
        Host_->SubscribeReplicaEnablementUpdated(BIND(&TNewReplicatedTableTracker::OnReplicaEnablementUpdated, MakeWeak(this)));
        Host_->SubscribeReplicaTrackingPolicyUpdated(BIND(&TNewReplicatedTableTracker::OnReplicaTrackingPolicyUpdated, MakeWeak(this)));

        Host_->SubscribeConfigChanged(BIND(&TNewReplicatedTableTracker::OnConfigChanged, MakeWeak(this)));
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

        return {};
    }

    TErrorOr<ETabletCellHealth> CheckBundleHealth(const TBundleHealthKey& key)
    {
        return BundleHealthChecker_.Get(key);
    }

    bool IsReplicaClusterBanned(TStringBuf clusterName) const
    {
        return Config_->ReplicatorHint->BannedReplicaClusters.contains(clusterName);
    }

    const TDynamicReplicatedTableTrackerConfigPtr& GetConfig() const
    {
        return Config_;
    }

    NApi::IClientPtr GetOrCreateClusterClient(const TString& clusterName)
    {
        auto now = NProfiling::GetInstant();

        auto it = ClusterNameToClient_.find(clusterName);
        if (it != ClusterNameToClient_.end() &&
            now - it->second.CreationTime < Config_->ClientExpirationTime)
        {
            return it->second.Client;
        } else {
            YT_LOG_DEBUG("Creating client for cluster %Qv",
                clusterName);

            auto client = Host_->CreateClusterClient(clusterName);
            ClusterNameToClient_[clusterName] = TClusterClientData{
                .Client = client,
                .CreationTime = now,
            };

            return client;
        }
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
            , Enabled_(data.Enabled)
            , TrackingEnabled_(data.TrackingEnabled)
        {
            State_ = data.Mode == ETableReplicaMode::Sync
                ? EReplicaState::BadSync
                : EReplicaState::BadAsync;
        }

        void UpdateReplicaState()
        {
            auto client = TableTracker_->GetOrCreateClusterClient(ClusterName_);
            if (!client) {
                OnCheckFailed(TError("No client is available"));
                return;
            }

            if (TableTracker_->IsReplicaClusterBanned(ClusterName_)) {
                OnCheckFailed(TError("Replica cluster is banned"));
                return;
            }

            if (auto error = TableTracker_->CheckClusterState({client, ClusterName_});
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

        TTableId GetReplicatedTableId() const
        {
            return ReplicatedTable_->GetId();
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
            return Enabled_ && TrackingEnabled_;
        }

        std::optional<TDuration> GetReplicaLagTime() const
        {
            return ReplicaLagTime_;
        }

        void SetReplicaLagTime(std::optional<TDuration> replicaLagTime)
        {
            ReplicaLagTime_ = replicaLagTime;
        }

    private:
        TNewReplicatedTableTracker* const TableTracker_;
        TReplicatedTable* const ReplicatedTable_;

        const TTableReplicaId Id_;
        const TString ClusterName_;
        const TYPath TablePath_;

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


        TError CheckBundleHealth(const NApi::IClientPtr& client)
        {
            auto bundleNameOrError = GetBundleName(client);
            if (!bundleNameOrError.IsOK()) {
                return bundleNameOrError;
            }

            auto bundleHealthOrError = TableTracker_->CheckBundleHealth({client, ClusterName_, bundleNameOrError.Value()});
            if (!bundleHealthOrError.IsOK()) {
                return bundleHealthOrError;
            }

            auto bundleHealth = bundleHealthOrError.Value();
            if (bundleHealth != ETabletCellHealth::Good && bundleHealth != ETabletCellHealth::Degraded) {
                if (++IterationsWithoutAcceptableBundleHealth_ >
                    TableTracker_->GetConfig()->MaxIterationsWithoutAcceptableBundleHealth)
                {
                    return TError("Bad tablet cell health for %v times in a row; actual health is %Qlv",
                        IterationsWithoutAcceptableBundleHealth_,
                        bundleHealth);
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
        YT_VERIFY(TypeFromId(replicaId) == EObjectType::TableReplica);
        return &GetOrCrash(IdToReplica_, replicaId);
    }

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
            , ReplicaModeSwitchCounter_(data.ReplicaModeSwitchCounter)
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

        TReplicasByState GroupReplicasByTargetState()
        {
            TReplicasByState replicasByState;
            int trackedReplicaCount = 0;
            for (auto replicaId : ReplicaIds_) {
                auto* replica = TableTracker_->GetReplica(replicaId);
                if (replica->ShouldTrack()) {
                    ++trackedReplicaCount;
                    replicasByState[replica->GetState()].push_back(replica);
                }
            }

            i64 minSyncReplicaCount;
            i64 maxSyncReplicaCount;
            std::tie(minSyncReplicaCount, maxSyncReplicaCount) = Options_->GetEffectiveMinMaxReplicaCount(trackedReplicaCount);

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

            return replicasByState;
        }

        NProfiling::TCounter GetReplicaModeSwitchCounter() const
        {
            return ReplicaModeSwitchCounter_;
        }

        THashSet<TTableReplicaId>* GetReplicaIds()
        {
            return &ReplicaIds_;
        }

    private:
        TNewReplicatedTableTracker* const TableTracker_;

        const TTableId Id_;
        const NProfiling::TCounter ReplicaModeSwitchCounter_;

        TReplicatedTableOptionsPtr Options_;
        TTableCollocationId CollocationId_;
        THashSet<TTableReplicaId> ReplicaIds_;
    };

private:
    const IReplicatedTableTrackerHostPtr Host_;

    const TActionQueuePtr RttThread_;
    const IInvokerPtr RttInvoker_;

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

    template <typename TCache>
    class TCacheSynchronousAdapter
    {
    public:
        using TKey = typename TCache::KeyType;
        using TValue = typename TCache::ValueType;

        TCacheSynchronousAdapter(TAsyncExpiringCacheConfigPtr config)
            : Cache_(New<TCache>(std::move(config)))
        { }

        TErrorOr<TValue> Get(const TKey& key)
        {
            if (Future_.IsSet()) {
                CurrentError_ = Future_.Get();
                Future_ = Cache_->Get(key);
            }

            return CurrentError_;
        }

        void Reconfigure(const TAsyncExpiringCacheConfigPtr& config) const
        {
            Cache_->Reconfigure(config);
        }

    private:
        const TIntrusivePtr<TCache> Cache_;

        TFuture<TValue> Future_ = MakeFuture<TValue>(TError("No result yet"));
        TErrorOr<TValue> CurrentError_;
    };

    TCacheSynchronousAdapter<TClusterLivenessCheckCache> ClusterLivenessChecker_;
    TCacheSynchronousAdapter<TClusterSafeModeCheckCache> ClusterSafeModeChecker_;
    TCacheSynchronousAdapter<THydraReadOnlyCheckCache> HydraReadOnlyChecker_;

    TCacheSynchronousAdapter<TBundleHealthCache> BundleHealthChecker_;

    struct TClusterClientData
    {
        NApi::IClientPtr Client;
        TInstant CreationTime;
    };

    THashMap<TString, TClusterClientData> ClusterNameToClient_;

    TFuture<TReplicaLagTimes> ReplicaLagTimesFuture_ = MakeFuture<TReplicaLagTimes>(TError("No result yet"));
    TErrorOr<TReplicaLagTimes> ReplicaLagTimesOrError_;


    void ScheduleTrackerIteration()
    {
        TDelayedExecutor::Submit(
            BIND(&TNewReplicatedTableTracker::RunTrackerIteration, MakeWeak(this))
                .Via(RttInvoker_),
            Config_->CheckPeriod);
    }

    void RunTrackerIteration()
    {
        if (!Config_->UseNewReplicatedTableTracker) {
            YT_LOG_DEBUG("New replicated table tracker is disabled");
            ScheduleTrackerIteration();
            return;
        }

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

        ScheduleTrackerIteration();
    }

    void DrainActionQueue()
    {
        while (true) {
            auto guard = Guard(ActionQueueLock_);

            if (Host_->LoadingFromSnapshotRequested()) {
                guard.Release();

                YT_LOG_DEBUG("Replicated table tracker started loading from snapshot");

                YT_VERIFY(ActionQueue_.empty());
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

        THashMap<TTableId, TReplicasByState> tableIdToReplicasByState;
        for (auto& [tableId, table] : IdToTable_) {
            if (!table.GetOptions()->EnableReplicatedTableTracker) {
                continue;
            }

            EmplaceOrCrash(
                tableIdToReplicasByState,
                tableId,
                table.GroupReplicasByTargetState());
        }

        THashMap<TTableCollocationId, THashMap<TString, int>> collocationIdToClusterPriorities;
        for (const auto& [collocationId, collocation] : IdToCollocation_) {
            std::optional<THashSet<TStringBuf>> goodReplicaClusters;

            for (auto tableId : collocation.TableIds) {
                auto it = tableIdToReplicasByState.find(tableId);
                if (it != tableIdToReplicasByState.end()) {
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

            auto it = EmplaceOrCrash(collocationIdToClusterPriorities, collocationId, THashMap<TString, int>());
            if (goodReplicaClusters) {
                for (auto clusterName : *goodReplicaClusters) {
                    it->second[clusterName] = 2;
                }
            }
        }

        for (auto& [tableId, replicasByState] : tableIdToReplicasByState) {
            auto& table = GetOrCrash(IdToTable_, tableId);
            std::optional<THashMap<TString, int>> replicaClusterPriorities;
            const auto& preferredSyncReplicaClusters = table.GetOptions()->PreferredSyncReplicaClusters;
            if (preferredSyncReplicaClusters) {
                replicaClusterPriorities.emplace();
            }
            if (table.GetCollocationId() != NullObjectId) {
                replicaClusterPriorities = GetOrCrash(collocationIdToClusterPriorities, table.GetCollocationId());
            }

            if (preferredSyncReplicaClusters) {
                for (const auto& clusterName : *preferredSyncReplicaClusters) {
                    ++(*replicaClusterPriorities)[clusterName];
                }
            }

            auto tableCommands = GenerateCommandsForTable(&replicasByState, replicaClusterPriorities);
            YT_LOG_DEBUG_IF(!tableCommands.empty(),
                "Generated replica mode change commands "
                "(TableId: %v, CollocationId: %v, Commands; %v)",
                tableId,
                table.GetCollocationId(),
                tableCommands);

            table.GetReplicaModeSwitchCounter().Increment(tableCommands.size());

            std::move(tableCommands.begin(), tableCommands.end(), std::back_inserter(commands));
        }

        YT_LOG_DEBUG("Replicated table tracker replica mode update iteration finished (UpdateCommandCount: %v)",
            commands.size());

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

                TReplicaList* newReplicas = std::ssize(newSyncReplicas) < std::ssize(syncReplicas)
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

        for (auto [replicaId, lagTime] : ReplicaLagTimesOrError_.Value()) {
            auto it = IdToReplica_.find(replicaId);
            if (it != IdToReplica_.end()) {
                it->second.SetReplicaLagTime(lagTime);
            }
        }

        YT_LOG_DEBUG("Successfully updated replica lag times");
    }

    TReplicatedTable* GetTable(TTableId tableId)
    {
        YT_VERIFY(TypeFromId(tableId) == EObjectType::ReplicatedTable);
        return &GetOrCrash(IdToTable_, tableId);
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
        EnqueueAction(BIND([=, this_ = MakeStrong(this), data = std::move(data)] {
            auto tableId = data.Id;

            YT_LOG_DEBUG("Replicated table created (TableId: %v)",
                tableId);

            YT_VERIFY(TypeFromId(tableId) == EObjectType::ReplicatedTable);

            EmplaceOrCrash(
                IdToTable_,
                tableId,
                TReplicatedTable(this, std::move(data)));
        }));
    }

    void OnReplicatedTableDestroyed(TTableId tableId)
    {
        EnqueueAction(BIND([=, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Replicated table destroyed (TableId: %v)",
                tableId);

            auto* table = GetTable(tableId);
            // NB: Replicas should be destroyed prior.
            YT_VERIFY(table->GetReplicaIds()->empty());
            if (table->GetCollocationId() != NullObjectId) {
                auto& collocation = GetOrCrash(IdToCollocation_, table->GetCollocationId());
                EraseOrCrash(collocation.TableIds, tableId);
            }
            EraseOrCrash(IdToTable_, tableId);
        }));
    }

    void OnReplicatedTableOptionsUpdated(TTableId tableId, TReplicatedTableOptionsPtr options)
    {
        EnqueueAction(BIND([=, this_ = MakeStrong(this), options = std::move(options)] {
            YT_LOG_DEBUG("Replicated table options updated (TableId: %v)",
                tableId);

            GetTable(tableId)->SetOptions(std::move(options));
        }));
    }

    void OnReplicaCreated(TReplicaData data)
    {
        EnqueueAction(BIND([=, this_ = MakeStrong(this), data = std::move(data)] {
            auto tableId = data.TableId;
            auto replicaId = data.Id;

            YT_LOG_DEBUG("Table replica created "
                "(TableId: %v, ReplicaId: %v, Mode: %v, Enabled: %v, ClusterName: %v, TablePath: %v)",
                tableId,
                replicaId,
                data.Mode,
                data.Enabled,
                data.ClusterName,
                data.TablePath);

            YT_VERIFY(TypeFromId(replicaId) == EObjectType::TableReplica);

            auto* table = GetTable(tableId);
            EmplaceOrCrash(
                IdToReplica_,
                replicaId,
                TReplica(this, table, std::move(data)));
            EmplaceOrCrash(*table->GetReplicaIds(), replicaId);
        }));
    }

    void OnReplicaDestroyed(TTableReplicaId replicaId)
    {
        EnqueueAction(BIND([=, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Table replica destroyed (ReplicaId: %v)",
                replicaId);

            YT_VERIFY(TypeFromId(replicaId) == EObjectType::TableReplica);
            auto tableId = GetReplica(replicaId)->GetReplicatedTableId();

            EraseOrCrash(*GetTable(tableId)->GetReplicaIds(), replicaId);
            EraseOrCrash(IdToReplica_, replicaId);
        }));
    }

    void OnReplicaModeUpdated(TTableReplicaId replicaId, ETableReplicaMode mode)
    {
        EnqueueAction(BIND([=, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Replica mode updated (ReplicaId: %v, Mode: %v)",
                replicaId,
                mode);

            GetReplica(replicaId)->SetMode(mode);
        }));
    }

    void OnReplicaEnablementUpdated(TTableReplicaId replicaId, bool enabled)
    {
        EnqueueAction(BIND([=, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Replica enablement updated (ReplicaId: %v, Enabled: %v)",
                replicaId,
                enabled);

            GetReplica(replicaId)->SetEnabled(enabled);
        }));
    }

    void OnReplicaTrackingPolicyUpdated(TTableReplicaId replicaId, bool enableTracking)
    {
        EnqueueAction(BIND([=, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Replica tracking policy updated (ReplicaId: %v, EnableTracking: %v)",
                replicaId,
                enableTracking);

            GetReplica(replicaId)->SetTrackingPolicy(enableTracking);
        }));
    }

    void OnReplicationCollocationUpdated(TTableCollocationData data)
    {
        EnqueueAction(BIND([=, this_ = MakeStrong(this), data = std::move(data)] {
            YT_LOG_DEBUG("Replication collocation updated (CollocationId: %v)",
                data.Id);

            YT_VERIFY(TypeFromId(data.Id) == EObjectType::TableCollocation);

            auto [it, _] = IdToCollocation_.try_emplace(data.Id);
            for (auto tableId : it->second.TableIds) {
                GetTable(tableId)->SetCollocationId(NullObjectId);
            }

            it->second.TableIds.clear();

            for (auto tableId : data.TableIds) {
                EmplaceOrCrash(it->second.TableIds, tableId);
                GetTable(tableId)->SetCollocationId(data.Id);
            }
        }));
    }

    void OnReplicationCollocationDestroyed(TTableCollocationId collocationId)
    {
        EnqueueAction(BIND([=, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Replication collocation destroyed (CollocationId: %v)",
                collocationId);

            YT_VERIFY(TypeFromId(collocationId) == EObjectType::TableCollocation);

            const auto& collocation = GetOrCrash(IdToCollocation_, collocationId);
            for (auto tableId : collocation.TableIds) {
                GetTable(tableId)->SetCollocationId(NullObjectId);
            }

            EraseOrCrash(IdToCollocation_, collocationId);
        }));
    }

    void OnConfigChanged(TDynamicReplicatedTableTrackerConfigPtr config)
    {
        EnqueueAction(BIND([=, this_ = MakeStrong(this), config = std::move(config)] {
            YT_LOG_DEBUG("Replicated table tracker config changed");

            if (Config_->UseNewReplicatedTableTracker != config->UseNewReplicatedTableTracker) {
                YT_LOG_DEBUG("New replicated table tracker is turned on; will load state from snapshot");
                RequestLoadingFromSnapshot();
            }

            Config_ = std::move(config);

            ClusterLivenessChecker_.Reconfigure(Config_->ClusterStateCache);
            ClusterSafeModeChecker_.Reconfigure(Config_->ClusterStateCache);
            HydraReadOnlyChecker_.Reconfigure(Config_->ClusterStateCache);
            BundleHealthChecker_.Reconfigure(Config_->BundleHealthCache);

            MaxActionQueueSize_.store(Config_->MaxActionQueueSize);
        }));
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
            YT_VERIFY(TypeFromId(tableId) == EObjectType::ReplicatedTable);
            auto replicaId = replicaData.Id;
            YT_VERIFY(TypeFromId(replicaId) == EObjectType::TableReplica);

            auto* table = GetTable(tableId);
            EmplaceOrCrash(
                IdToReplica_,
                replicaId,
                TReplica(this, table, std::move(replicaData)));
            EmplaceOrCrash(*table->GetReplicaIds(), replicaId);
        }

        for (const auto& collocationData : snapshot.Collocations) {
            auto collocationId = collocationData.Id;
            YT_VERIFY(TypeFromId(collocationId) == EObjectType::TableCollocation);

            auto it = EmplaceOrCrash(IdToCollocation_, collocationId, TTableCollocation{});
            for (auto tableId : collocationData.TableIds) {
                EmplaceOrCrash(it->second.TableIds, tableId);
                GetTable(tableId)->SetCollocationId(collocationId);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IReplicatedTableTrackerPtr CreateReplicatedTableTracker(
    IReplicatedTableTrackerHostPtr host,
    TDynamicReplicatedTableTrackerConfigPtr config)
{
    return New<TNewReplicatedTableTracker>(
        std::move(host),
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
