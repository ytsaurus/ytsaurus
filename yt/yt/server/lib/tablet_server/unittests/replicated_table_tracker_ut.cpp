#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/tablet_server/config.h>
#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/client/object_client/public.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/library/profiling/testing.h>

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletServer {
namespace {

using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletServer;
using namespace NYPath;
using namespace NApi;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;
using namespace NTabletNode;
using namespace NProfiling;

using ::testing::StrictMock;
using ::testing::Return;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Mock;

////////////////////////////////////////////////////////////////////////////////

static constexpr TDuration CheckPeriod = TDuration::MilliSeconds(10);

static const int WarmUpIterationCount = 10;

static const std::string Cluster1 = "ReplicaCluster1";
static const std::string Cluster2 = "ReplicaCluster2";

static const TYPath TablePath1 = "//tmp/replica_table_1";
static const TYPath TablePath2 = "//tmp/replica_table_2";

static const TString BundleName = "default";

using TStrictMockClient = StrictMock<TMockClient>;
using TStrictMockClientPtr = TIntrusivePtr<TStrictMockClient>;

DEFINE_REFCOUNTED_TYPE(TStrictMockClient)

////////////////////////////////////////////////////////////////////////////////

class TMockReplicatedTableTrackerHost
    : public IReplicatedTableTrackerHost
{
public:
    bool AlwaysUseNewReplicatedTableTracker() const override
    {
        return false;
    }

    // Snapshot stuff.
    TFuture<TReplicatedTableTrackerSnapshot> GetSnapshot() override
    {
        EXPECT_TRUE(LoadingFromSnapshotRequested_.load());
        SnapshotFutureRequested_.store(true);
        return SnapshotPromise_.ToFuture().Apply(BIND(
            [
                =,
                this,
                this_ = MakeStrong(this)
            ] (const TReplicatedTableTrackerSnapshot& snapshot)
        {
            YT_VERIFY(LoadingFromSnapshotRequested_.exchange(false));
            YT_VERIFY(SnapshotFutureRequested_.exchange(false));
            return snapshot;
        }));
    }

    TDynamicReplicatedTableTrackerConfigPtr GetConfig() const override
    {
        return Config_;
    }

    void SetSnapshot(TReplicatedTableTrackerSnapshot snapshot)
    {
        YT_VERIFY(LoadingFromSnapshotRequested_.load());
        SnapshotPromise_.Set(std::move(snapshot));
    }

    bool LoadingFromSnapshotRequested() const override
    {
        return LoadingFromSnapshotRequested_.load();
    }

    void RequestLoadingFromSnapshot() override
    {
        LoadingFromSnapshotRequested_.store(true);
    }

    bool SnapshotFutureRequested() const
    {
        return SnapshotFutureRequested_.load();
    }

    void ResetSnapshotPromise()
    {
        SnapshotPromise_ = NewPromise<TReplicatedTableTrackerSnapshot>();
    }

    TFuture<TReplicaLagTimes> ComputeReplicaLagTimes(std::vector<TTableReplicaId> replicaIds) override
    {
        auto guard = Guard(ReplicaLagTimesLock_);

        TReplicaLagTimes replicaLagTimes;
        replicaLagTimes.reserve(replicaIds.size());
        for (auto replicaId : replicaIds) {
            auto it = ReplicaIdToLagTime_.find(replicaId);
            EXPECT_NE(it, ReplicaIdToLagTime_.end());
            replicaLagTimes.emplace_back(replicaId, it->second);
        }

        return MakeFuture(std::move(replicaLagTimes));
    }

    // Cluster client registry stuff.
    NApi::IClientPtr CreateClusterClient(const std::string& clusterName) override
    {
        auto it = Clusters_.find(clusterName);
        EXPECT_NE(it, Clusters_.end());
        return it->second;
    }

    const TStrictMockClientPtr& GetMockClient(TStringBuf clusterName)
    {
        return GetOrCrash(Clusters_, clusterName);
    }

    // Replica mode changer stuff.
    TFuture<TApplyChangeReplicaCommandResults> ApplyChangeReplicaModeCommands(
        std::vector<TChangeReplicaModeCommand> commands) override
    {
        for (const auto& command : commands) {
            auto it = ReplicaIdToInfo_.find(command.ReplicaId);
            EXPECT_NE(it, ReplicaIdToInfo_.end());
            ++it->second.CommandCount;
            it->second.Data.Mode = command.TargetMode;
            ReplicaCreated_(it->second.Data);
        }

        TApplyChangeReplicaCommandResults result(commands.size());
        return MakeFuture(result);
    }

    void ValidateReplicaModeChanged(TTableReplicaId replicaId, ETableReplicaMode targetMode)
    {
        auto it = ReplicaIdToInfo_.find(replicaId);
        EXPECT_NE(it, ReplicaIdToInfo_.end());
        EXPECT_GT(it->second.CommandCount, 0);
        it->second.CommandCount = 0;
        EXPECT_EQ(it->second.Data.Mode, targetMode);
    }

    void ValidateReplicaModeRemained(TTableReplicaId replicaId) const
    {
        EXPECT_EQ(GetOrCrash(ReplicaIdToInfo_, replicaId).CommandCount, 0);
    }

    ETableReplicaMode GetReplicaMode(TTableReplicaId replicaId) const
    {
        return GetOrCrash(ReplicaIdToInfo_, replicaId).Data.Mode;
    }

    // Host stuff.
    void SubscribeReplicatedTableCreated(TCallback<void(TReplicatedTableData)> callback) override
    {
        ReplicatedTableCreated_ = std::move(callback);
    }

    void SubscribeReplicatedTableDestroyed(TCallback<void(NTableClient::TTableId)> callback) override
    {
        ReplicatedTableDestroyed_ = std::move(callback);
    }

    void SubscribeReplicationCollocationCreated(TCallback<void(TTableCollocationData)> callback) override
    {
        ReplicationCollocationCreated_ = std::move(callback);
    }

    void SubscribeReplicationCollocationDestroyed(
        TCallback<void(NTableClient::TTableCollocationId)> callback) override
    {
        ReplicationCollocationDestroyed_ = std::move(callback);
    }

    void SubscribeReplicaCreated(TCallback<void(TReplicaData)> callback) override
    {
        ReplicaCreated_ = std::move(callback);
    }

    void SubscribeReplicaDestroyed(
        TCallback<void(NTabletClient::TTableReplicaId)> callback) override
    {
        ReplicaDestroyed_ = std::move(callback);
    }

    // Host testing interop.
    TReplicatedTableData CreateReplicatedTableData()
    {
        auto tableId = MakeRegularId(
            EObjectType::ReplicatedTable,
            InvalidCellTag,
            NHydra::TVersion(),
            std::ssize(TableIdToInfo_));

        auto options = New<TReplicatedTableOptions>();
        options->EnableReplicatedTableTracker = true;
        options->MinSyncReplicaCount = 0;
        options->MaxSyncReplicaCount = 1;

        auto it = EmplaceOrCrash(TableIdToInfo_, tableId, TTableInfo{
            .Options = std::move(options),
        });

        return TReplicatedTableData{
            .Id = tableId,
            .Options = it->second.Options,
        };
    }

    TTableId CreateReplicatedTable()
    {
        auto data = CreateReplicatedTableData();
        ReplicatedTableCreated_(data);
        return data.Id;
    }

    TReplicaData CreateTableReplicaData(
        TTableId tableId,
        ETableReplicaMode mode = ETableReplicaMode::Async,
        bool enabled = true,
        const std::string& clusterName = Cluster1,
        const TYPath& tablePath = TablePath1,
        std::optional<TDuration> replicaLagTime = TDuration::Zero(),
        EObjectType replicaObjectType = EObjectType::TableReplica,
        ETableReplicaContentType contentType = ETableReplicaContentType::Data)
    {
        auto replicaId = MakeRegularId(
            replicaObjectType,
            InvalidCellTag,
            NHydra::TVersion(),
            std::ssize(ReplicaIdToInfo_));

        auto& replicaIds = GetOrCrash(TableIdToInfo_, tableId).ReplicaIds;
        InsertOrCrash(replicaIds, replicaId);

        TReplicaData replicaData{
            .TableId = tableId,
            .Id = replicaId,
            .Mode = mode,
            .Enabled = enabled,
            .ClusterName = clusterName,
            .TablePath = tablePath,
            .TrackingEnabled = true,
            .ContentType = contentType,
        };
        EmplaceOrCrash(ReplicaIdToInfo_, replicaId, TReplicaInfo{ .Data = replicaData });

        auto guard = Guard(ReplicaLagTimesLock_);
        EmplaceOrCrash(ReplicaIdToLagTime_, replicaId, replicaLagTime);

        return replicaData;
    }

    TTableReplicaId CreateTableReplica(
        TTableId tableId,
        ETableReplicaMode mode = ETableReplicaMode::Async,
        bool enabled = true,
        const std::string& clusterName = Cluster1,
        const TYPath& tablePath = TablePath1,
        std::optional<TDuration> replicaLagTime = TDuration::Zero(),
        EObjectType replicaObjectType = EObjectType::TableReplica,
        ETableReplicaContentType contentType = ETableReplicaContentType::Data)
    {
        auto data = CreateTableReplicaData(tableId, mode, enabled, clusterName, tablePath, replicaLagTime, replicaObjectType, contentType);
        ReplicaCreated_(data);
        return data.Id;
    }

    void SetReplicaLagTime(TTableReplicaId replicaId, std::optional<TDuration> replicaLagTime)
    {
        auto guard = Guard(ReplicaLagTimesLock_);
        GetOrCrash(ReplicaIdToLagTime_, replicaId) = replicaLagTime;
    }

    const TReplicatedTableOptionsPtr& GetTableOptions(TTableId tableId) const
    {
        return GetOrCrash(TableIdToInfo_, tableId).Options;
    }

    void SetTableOptions(TTableId tableId, TReplicatedTableOptionsPtr options)
    {
        auto& info = GetOrCrash(TableIdToInfo_, tableId);
        info.Options = options;
        ReplicatedTableCreated_(TReplicatedTableData{
            .Id = tableId,
            .Options = info.Options,
        });
    }

    void UpdateReplicaEnablement(TTableReplicaId replicaId, bool enabled)
    {
        auto& replicaInfo = GetOrCrash(ReplicaIdToInfo_, replicaId);
        replicaInfo.Data.Enabled = enabled;
        ReplicaCreated_(replicaInfo.Data);
    }

    void UpdateReplicaTrackingPolicy(TTableReplicaId replicaId, bool trackingEnabled)
    {
        auto& replicaInfo = GetOrCrash(ReplicaIdToInfo_, replicaId);
        replicaInfo.Data.TrackingEnabled = trackingEnabled;
        ReplicaCreated_(replicaInfo.Data);
    }

    void UpdateReplicaMode(TTableReplicaId replicaId, ETableReplicaMode mode)
    {
        auto& replicaInfo = GetOrCrash(ReplicaIdToInfo_, replicaId);
        replicaInfo.Data.Mode = mode;
        ReplicaCreated_(replicaInfo.Data);
    }

    TTableCollocationId CreateReplicationCollocation(std::vector<TTableId> tableIds)
    {
        auto collocationId = MakeRegularId(
            EObjectType::TableCollocation,
            InvalidCellTag,
            NHydra::TVersion(),
            std::ssize(CollocationIdToInfo_));
        EmplaceOrCrash(CollocationIdToInfo_, collocationId, TCollocationInfo{ .TableIds = tableIds });

        ReplicationCollocationCreated_(TTableCollocationData{
            .Id = collocationId,
            .TableIds = std::move(tableIds),
        });

        return collocationId;
    }

    void UpdateReplicationCollocationOptions(
        TTableCollocationId collocationId,
        std::optional<std::vector<std::string>> preferredSyncReplicaClusters)
    {
        const auto& collocationInfo = GetOrCrash(CollocationIdToInfo_, collocationId);

        auto options = New<TReplicationCollocationOptions>();
        options->PreferredSyncReplicaClusters = std::move(preferredSyncReplicaClusters);

        ReplicationCollocationCreated_(TTableCollocationData{
            .Id = collocationId,
            .TableIds = collocationInfo.TableIds,
            .Options = std::move(options),
        });
    }

    void DestroyTable(TTableId tableId)
    {
        ReplicatedTableDestroyed_(tableId);
    }

    void DestroyTableReplica(TTableReplicaId replicaId)
    {
        ReplicaDestroyed_(replicaId);
    }

    void DestroyReplicationCollocation(TTableCollocationId collocationId)
    {
        ReplicationCollocationDestroyed_(collocationId);
    }

private:
    const TDynamicReplicatedTableTrackerConfigPtr Config_ = New<TDynamicReplicatedTableTrackerConfig>();

    TPromise<TReplicatedTableTrackerSnapshot> SnapshotPromise_ = NewPromise<TReplicatedTableTrackerSnapshot>();
    std::atomic<bool> LoadingFromSnapshotRequested_ = false;
    std::atomic<bool> SnapshotFutureRequested_ = false;

    const THashMap<std::string, TStrictMockClientPtr, THash<TStringBuf>, TEqualTo<TStringBuf>> Clusters_ = {
        {Cluster1, New<TStrictMockClient>()},
        {Cluster2, New<TStrictMockClient>()}
    };

    struct TReplicaInfo
    {
        int CommandCount = 0;
        TReplicaData Data;
    };

    THashMap<TTableReplicaId, TReplicaInfo> ReplicaIdToInfo_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ReplicaLagTimesLock_);
    THashMap<TTableReplicaId, std::optional<TDuration>> ReplicaIdToLagTime_;

    struct TTableInfo
    {
        TReplicatedTableOptionsPtr Options;
        THashSet<TTableReplicaId> ReplicaIds;
    };

    THashMap<TTableId, TTableInfo> TableIdToInfo_;

    struct TCollocationInfo
    {
        std::vector<TTableId> TableIds;
    };

    THashMap<TTableCollocationId, TCollocationInfo> CollocationIdToInfo_;

    TCallback<void(TReplicatedTableData)> ReplicatedTableCreated_;
    TCallback<void(NTableClient::TTableId)> ReplicatedTableDestroyed_;
    TCallback<void(TReplicaData)> ReplicaCreated_;
    TCallback<void(NTabletClient::TTableReplicaId)> ReplicaDestroyed_;
    TCallback<void(TTableCollocationData)> ReplicationCollocationCreated_;
    TCallback<void(NTableClient::TTableCollocationId)> ReplicationCollocationDestroyed_;
    TCallback<void(TDynamicReplicatedTableTrackerConfigPtr)> ConfigChanged_;
};

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        Host_->GetConfig()->CheckPeriod = CheckPeriod;
        Host_->GetConfig()->UseNewReplicatedTableTracker = true;
        Tracker_ = ::NYT::NTabletServer::CreateReplicatedTableTracker(
            Host_,
            CloneYsonStruct(Host_->GetConfig()),
            /*profiler*/ {});
        Tracker_->Initialize();
        Tracker_->EnableTracking();

        EXPECT_TRUE(Host_->LoadingFromSnapshotRequested());
        Host_->SetSnapshot(TReplicatedTableTrackerSnapshot());
        WaitForUpdatesFromTracker();
        EXPECT_FALSE(Host_->LoadingFromSnapshotRequested());
        Host_->ResetSnapshotPromise();
    }

    void TearDown() override
    {
        auto weakTracker = MakeWeak(Tracker_);
        Tracker_.Reset();
        // NB: With that we ensure that mock clients will be destructed properly,
        // so all test expectations will be verified.
        while (true) {
            Sleep(CheckPeriod);
            if (!weakTracker.Lock()) {
                break;
            }
        }
    }

    void MockGoodReplicaCluster(const TStrictMockClientPtr& client)
    {
        NApi::TCheckClusterLivenessOptions options{
            .CheckCypressRoot = true,
            .CheckSecondaryMasterCells = true,
        };
        EXPECT_CALL(*client, CheckClusterLiveness(options))
            .WillRepeatedly(Return(VoidFuture));
        EXPECT_CALL(*client, GetNode("//sys/@config/enable_safe_mode", _))
            .WillRepeatedly(Return(MakeFuture(ConvertToYsonString(false))));
        EXPECT_CALL(*client, GetNode("//sys/@hydra_read_only", _))
            .WillRepeatedly(Return(MakeFuture(ConvertToYsonString(false))));
        EXPECT_CALL(*client, GetNode("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/enable_incoming_replication", _))
            .WillRepeatedly(Return(MakeFuture(ConvertToYsonString(true))));
    }

    void MockGoodBundle(const TStrictMockClientPtr& client, const NYPath::TYPath& tablePath = TablePath1)
    {
        EXPECT_CALL(*client, GetNode(tablePath + "/@tablet_cell_bundle", _))
            .WillOnce(Return(MakeFuture(ConvertToYsonString(BundleName))));
        NApi::TCheckClusterLivenessOptions options{
            .CheckTabletCellBundle = BundleName
        };
        EXPECT_CALL(*client, CheckClusterLiveness(options))
            .WillRepeatedly(Return(VoidFuture));
    }

    void MockGoodTable(const TStrictMockClientPtr& client, const NYPath::TYPath& tablePath = TablePath1)
    {
        EXPECT_CALL(*client, GetNode(tablePath, _))
            .WillRepeatedly(Invoke([=] (const NYPath::TYPath& /*path*/, const TGetNodeOptions& options) {
                EXPECT_FALSE(options.Attributes);
                return MakeFuture(TYsonString());
            }));
    }

    void MockBadTable(const TStrictMockClientPtr& client, const NYPath::TYPath& tablePath = TablePath1)
    {
        EXPECT_CALL(*client, GetNode(tablePath, _))
            .WillRepeatedly(Return(MakeFuture<TYsonString>(TError("Bad table"))));
    }

    std::tuple<TTableId, TTableReplicaId, TTableReplicaId, TTableId, TTableReplicaId, TTableReplicaId>
    CreateTablesForCollocation()
    {
        auto client1 = Host_->GetMockClient(Cluster1);
        MockGoodReplicaCluster(client1);
        MockGoodBundle(client1);
        MockGoodBundle(client1, TablePath2);
        MockGoodTable(client1);
        MockGoodTable(client1, TablePath2);

        auto client2 = Host_->GetMockClient(Cluster2);
        MockGoodReplicaCluster(client2);
        MockGoodBundle(client2);
        MockGoodBundle(client2, TablePath2);
        MockGoodTable(client2);
        MockGoodTable(client2, TablePath2);

        auto table1 = Host_->CreateReplicatedTable();
        auto replica11 = Host_->CreateTableReplica(table1);
        auto replica12 = Host_->CreateTableReplica(
            table1,
            ETableReplicaMode::Async,
            /*enabled*/ false,
            Cluster2);

        auto table2 = Host_->CreateReplicatedTable();
        auto replica21 = Host_->CreateTableReplica(
            table2,
            ETableReplicaMode::Async,
            /*enabled*/ false,
            Cluster1,
            TablePath2);
        auto replica22 = Host_->CreateTableReplica(
            table2,
            ETableReplicaMode::Async,
            /*enabled*/ true,
            Cluster2,
            TablePath2);

        WaitForTrackerWarmUp();
        Host_->ValidateReplicaModeChanged(replica11, ETableReplicaMode::Sync);
        Host_->ValidateReplicaModeChanged(replica22, ETableReplicaMode::Sync);

        Host_->UpdateReplicaEnablement(replica12, /*enabled*/ true);
        Host_->UpdateReplicaEnablement(replica21, /*enabled*/ true);

        WaitForTrackerWarmUp();
        Host_->ValidateReplicaModeRemained(replica12);
        Host_->ValidateReplicaModeRemained(replica21);

        // NB: Table1 has sync replica11 on Cluster1 and async replica12 on Cluster2.
        // and table2 has sync replica22 on Cluster2 and async replica21 on Cluster1.
        return {
            table1, replica11, replica12,
            table2, replica21, replica22
        };
    }

    void WaitForTrackerWarmUp()
    {
        // NB: We need a few check iterations to warm Rtt up, invoking all checks for the first time.
        DoWaitForTracker(WarmUpIterationCount);
    }

    void WaitForUpdatesFromTracker()
    {
        // NB: This should be enough to apply all new mocks and state updates.
        DoWaitForTracker(3);
    }

    void DoWaitForTracker(int iterationCount)
    {
        auto startIteration = Tracker_->GetIterationCount();
        while (true) {
            Sleep(CheckPeriod);
            auto currentIteration = Tracker_->GetIterationCount();
            if (currentIteration - startIteration >= iterationCount) {
                break;
            }
            if (Host_->SnapshotFutureRequested()) {
                break;
            }
        }
    }

protected:
    const TIntrusivePtr<TMockReplicatedTableTrackerHost> Host_ = New<TMockReplicatedTableTrackerHost>();

    IReplicatedTableTrackerPtr Tracker_;

};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TReplicatedTableTrackerTest, Simple)
{
    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodBundle(client);
    MockGoodTable(client);

    auto tableId = Host_->CreateReplicatedTable();
    auto replicaId = Host_->CreateTableReplica(tableId);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);
}

TEST_F(TReplicatedTableTrackerTest, BannedReplicaCluster)
{
    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodBundle(client);
    MockGoodTable(client);

    auto tableId = Host_->CreateReplicatedTable();
    auto replicaId = Host_->CreateTableReplica(tableId);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);

    Host_->GetConfig()->ReplicatorHint->BannedReplicaClusters = {Cluster1};

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Async);

    Host_->GetConfig()->ReplicatorHint->BannedReplicaClusters.clear();

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);
}

TFuture<TYsonString> ReturnSerializedPreloadStateFuture(
    EStorePreloadState preloadState,
    const NYPath::TYPath& /*path*/,
    const TGetNodeOptions& options)
{
    if (options.Attributes) {
        EXPECT_EQ(options.Attributes.Keys, std::vector<std::string>{"preload_state"});
        return MakeFuture(
            BuildYsonStringFluently()
                .BeginAttributes()
                    .Item("preload_state").Value(preloadState)
                .EndAttributes()
                .Entity());
    } else {
        return MakeFuture(TYsonString());
    }
}

TEST_F(TReplicatedTableTrackerTest, PreloadCheck)
{
    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodBundle(client);
    MockGoodTable(client);

    auto tableId = Host_->CreateReplicatedTable();
    auto replicaId = Host_->CreateTableReplica(tableId);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);

    auto mockPreloadState = [&] (EStorePreloadState preloadState) {
        EXPECT_CALL(*client, GetNode(TablePath1, _))
            .WillRepeatedly(Invoke(std::bind(
                ReturnSerializedPreloadStateFuture,
                preloadState,
                std::placeholders::_1,
                std::placeholders::_2)));

        Sleep(CheckPeriod);
    };

    mockPreloadState(EStorePreloadState::Complete);

    auto options = Host_->GetTableOptions(tableId);
    options->EnablePreloadStateCheck = true;
    options->IncompletePreloadGracePeriod = TDuration::Zero();
    Host_->SetTableOptions(tableId, std::move(options));

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeRemained(replicaId);

    mockPreloadState(EStorePreloadState::Running);
    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Async);

    mockPreloadState(EStorePreloadState::Complete);
    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);
}

TEST_F(TReplicatedTableTrackerTest, BundleHealthCheck)
{
    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodTable(client);

    constexpr int MaxIterationsWithoutAcceptableBundleHealth = 4;
    Host_->GetConfig()->MaxIterationsWithoutAcceptableBundleHealth = MaxIterationsWithoutAcceptableBundleHealth;
    Host_->GetConfig()->BundleHealthCache->RefreshTime = CheckPeriod / 2;
    Host_->GetConfig()->BundleHealthCache->ExpireAfterFailedUpdateTime = CheckPeriod / 2;

    auto tableId = Host_->CreateReplicatedTable();
    auto options = Host_->GetTableOptions(tableId);
    options->RetryOnFailureInterval = TDuration::Zero();
    Host_->SetTableOptions(tableId, std::move(options));

    EXPECT_CALL(*client, GetNode(TablePath1 + "/@tablet_cell_bundle", _))
        .WillRepeatedly(Return(MakeFuture<TYsonString>(TError("Error fetching bundle name"))));

    auto replicaId = Host_->CreateTableReplica(tableId);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replicaId);

    EXPECT_CALL(*client, CheckClusterLiveness(NApi::TCheckClusterLivenessOptions{
        .CheckTabletCellBundle = BundleName
    }))
        .WillRepeatedly(Return(VoidFuture));

    EXPECT_CALL(*client, GetNode(TablePath1 + "/@tablet_cell_bundle", _))
        .WillOnce(Return(MakeFuture(ConvertToYsonString(BundleName))));

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);

    EXPECT_CALL(*client, CheckClusterLiveness(NApi::TCheckClusterLivenessOptions{
        .CheckTabletCellBundle = BundleName
    }))
        .WillRepeatedly(Return(MakeFuture(TError("Err"))));

    Host_->ValidateReplicaModeRemained(replicaId);
    DoWaitForTracker(MaxIterationsWithoutAcceptableBundleHealth * 2);
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Async);

    EXPECT_CALL(*client, CheckClusterLiveness(NApi::TCheckClusterLivenessOptions{
        .CheckTabletCellBundle = BundleName
    }))
        .WillRepeatedly(Return(VoidFuture));

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);
}

TEST_F(TReplicatedTableTrackerTest, PreferredReplicaClusters)
{
    auto client1 = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client1);
    MockGoodBundle(client1);
    MockGoodTable(client1);

    auto client2 = Host_->GetMockClient(Cluster2);
    MockGoodReplicaCluster(client2);
    MockGoodBundle(client2);
    MockGoodTable(client2);

    auto tableId = Host_->CreateReplicatedTable();
    auto replica1 = Host_->CreateTableReplica(tableId);
    auto replica2 = Host_->CreateTableReplica(
        tableId,
        ETableReplicaMode::Async,
        /*enabled*/ false,
        Cluster2);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replica1, ETableReplicaMode::Sync);

    Host_->UpdateReplicaEnablement(replica2, /*enabled*/ true);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replica2);

    auto options = Host_->GetTableOptions(tableId);
    options->PreferredSyncReplicaClusters = {Cluster2};
    Host_->SetTableOptions(tableId, std::move(options));

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replica1, ETableReplicaMode::Async);
    Host_->ValidateReplicaModeChanged(replica2, ETableReplicaMode::Sync);

    options = Host_->GetTableOptions(tableId);
    options->PreferredSyncReplicaClusters = {Cluster1};
    Host_->SetTableOptions(tableId, std::move(options));

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replica1, ETableReplicaMode::Sync);
    Host_->ValidateReplicaModeChanged(replica2, ETableReplicaMode::Async);

    Host_->GetConfig()->ReplicatorHint->PreferredSyncReplicaClusters = {Cluster2};

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replica1, ETableReplicaMode::Async);
    Host_->ValidateReplicaModeChanged(replica2, ETableReplicaMode::Sync);

    options = Host_->GetTableOptions(tableId);
    options->PreferredSyncReplicaClusters = {};
    Host_->SetTableOptions(tableId, std::move(options));

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeRemained(replica1);
    Host_->ValidateReplicaModeRemained(replica2);

    Host_->GetConfig()->ReplicatorHint->PreferredSyncReplicaClusters = {};

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeRemained(replica1);
    Host_->ValidateReplicaModeRemained(replica2);
}

TEST_F(TReplicatedTableTrackerTest, TableCollocationSimple)
{
    auto [table1, replica11, replica12, table2, replica21, replica22] = CreateTablesForCollocation();

    Host_->CreateReplicationCollocation({table1, table2});

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replica12, ETableReplicaMode::Sync);
    Host_->ValidateReplicaModeChanged(replica11, ETableReplicaMode::Async);
    Host_->ValidateReplicaModeRemained(replica21);
    Host_->ValidateReplicaModeRemained(replica22);
}

TEST_F(TReplicatedTableTrackerTest, TableCollocationWithBadReplicaTable)
{
    auto [table1, replica11, replica12, table2, replica21, replica22] = CreateTablesForCollocation();

    Host_->CreateReplicationCollocation({table1, table2});

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replica12, ETableReplicaMode::Sync);
    Host_->ValidateReplicaModeChanged(replica11, ETableReplicaMode::Async);
    Host_->ValidateReplicaModeRemained(replica21);
    Host_->ValidateReplicaModeRemained(replica22);

    auto client2 = Host_->GetMockClient(Cluster2);
    MockBadTable(client2);

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replica12, ETableReplicaMode::Async);
    Host_->ValidateReplicaModeChanged(replica11, ETableReplicaMode::Sync);
    Host_->ValidateReplicaModeChanged(replica21, ETableReplicaMode::Sync);
    Host_->ValidateReplicaModeChanged(replica22, ETableReplicaMode::Async);
}

TEST_F(TReplicatedTableTrackerTest, TableCollocationWithPreferredReplicaClusters)
{
    auto [table1, replica11, replica12, table2, replica21, replica22] = CreateTablesForCollocation();

    auto collocationId = Host_->CreateReplicationCollocation({table1, table2});

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeChanged(replica12, ETableReplicaMode::Sync);
    Host_->ValidateReplicaModeChanged(replica11, ETableReplicaMode::Async);
    Host_->ValidateReplicaModeRemained(replica21);
    Host_->ValidateReplicaModeRemained(replica22);

    auto options = Host_->GetTableOptions(table1);
    options->PreferredSyncReplicaClusters = {Cluster1};
    Host_->SetTableOptions(table1, std::move(options));

    options = Host_->GetTableOptions(table2);
    options->PreferredSyncReplicaClusters = {Cluster1};
    Host_->SetTableOptions(table2, std::move(options));

    auto validateSyncOnCluster1 = [&] {
        WaitForUpdatesFromTracker();
        Host_->ValidateReplicaModeChanged(replica12, ETableReplicaMode::Async);
        Host_->ValidateReplicaModeChanged(replica11, ETableReplicaMode::Sync);
        Host_->ValidateReplicaModeChanged(replica21, ETableReplicaMode::Sync);
        Host_->ValidateReplicaModeChanged(replica22, ETableReplicaMode::Async);
    };

    auto validateSyncOnCluster2 = [&] {
        WaitForUpdatesFromTracker();
        Host_->ValidateReplicaModeChanged(replica11, ETableReplicaMode::Async);
        Host_->ValidateReplicaModeChanged(replica12, ETableReplicaMode::Sync);
        Host_->ValidateReplicaModeChanged(replica22, ETableReplicaMode::Sync);
        Host_->ValidateReplicaModeChanged(replica21, ETableReplicaMode::Async);
    };

    validateSyncOnCluster1();

    Host_->UpdateReplicationCollocationOptions(collocationId, std::vector<std::string>{Cluster1, Cluster2});

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeRemained(replica12);
    Host_->ValidateReplicaModeRemained(replica11);
    Host_->ValidateReplicaModeRemained(replica21);
    Host_->ValidateReplicaModeRemained(replica22);

    Host_->UpdateReplicationCollocationOptions(collocationId, std::vector<std::string>{Cluster2});

    validateSyncOnCluster2();

    Host_->UpdateReplicationCollocationOptions(collocationId, std::nullopt);

    validateSyncOnCluster1();

    auto client1 = Host_->GetMockClient(Cluster1);
    MockBadTable(client1);

    validateSyncOnCluster2();
}

TEST_F(TReplicatedTableTrackerTest, LoadFromSnapshot)
{
    auto client1 = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client1);
    MockGoodBundle(client1);
    MockGoodTable(client1);

    EXPECT_FALSE(Host_->LoadingFromSnapshotRequested());
    Tracker_->RequestLoadingFromSnapshot();
    WaitForUpdatesFromTracker();
    EXPECT_TRUE(Host_->LoadingFromSnapshotRequested());

    TReplicatedTableTrackerSnapshot snapshot;
    auto tableData = Host_->CreateReplicatedTableData();
    auto replicaData = Host_->CreateTableReplicaData(tableData.Id);
    snapshot.ReplicatedTables.push_back(tableData);
    snapshot.Replicas.push_back(replicaData);
    Host_->SetSnapshot(std::move(snapshot));

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaData.Id, ETableReplicaMode::Sync);
}

TEST_F(TReplicatedTableTrackerTest, IgnoreNewActionsIfLoadingFromSnapshotRequested)
{
    EXPECT_FALSE(Host_->LoadingFromSnapshotRequested());
    Tracker_->RequestLoadingFromSnapshot();
    WaitForUpdatesFromTracker();
    EXPECT_TRUE(Host_->LoadingFromSnapshotRequested());

    TReplicatedTableTrackerSnapshot snapshot;
    auto tableData = Host_->CreateReplicatedTableData();
    auto replicaData = Host_->CreateTableReplicaData(
        tableData.Id,
        ETableReplicaMode::Async,
        /*enabled*/ false);
    snapshot.ReplicatedTables.push_back(tableData);
    snapshot.Replicas.push_back(replicaData);

    // NB: This action should be ignored by RTT.
    Host_->UpdateReplicaEnablement(replicaData.Id, /*enabled*/ true);

    Host_->SetSnapshot(std::move(snapshot));

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replicaData.Id);
}

TEST_F(TReplicatedTableTrackerTest, LoadFromSnapshotUponActionQueueOverflow)
{
    Host_->GetConfig()->MaxActionQueueSize = 1;
    WaitForUpdatesFromTracker();

    EXPECT_FALSE(Host_->LoadingFromSnapshotRequested());
    while (!Host_->LoadingFromSnapshotRequested()) {
        // This is racy so do retries.
        auto tableId = Host_->CreateReplicatedTable();
        Host_->CreateTableReplica(tableId);
    }

    auto client1 = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client1);
    MockGoodBundle(client1);
    MockGoodTable(client1);

    TReplicatedTableTrackerSnapshot snapshot;
    auto tableData = Host_->CreateReplicatedTableData();
    auto replicaData = Host_->CreateTableReplicaData(tableData.Id);
    snapshot.ReplicatedTables.push_back(tableData);
    snapshot.Replicas.push_back(replicaData);
    Host_->SetSnapshot(std::move(snapshot));

    WaitForTrackerWarmUp();
    EXPECT_FALSE(Host_->LoadingFromSnapshotRequested());
    Host_->ValidateReplicaModeChanged(replicaData.Id, ETableReplicaMode::Sync);
}

TEST_F(TReplicatedTableTrackerTest, BadSyncReplicaPresence)
{
    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodBundle(client);
    MockBadTable(client);

    auto tableId = Host_->CreateReplicatedTable();
    auto options = Host_->GetTableOptions(tableId);
    options->MinSyncReplicaCount = 1;
    Host_->SetTableOptions(tableId, std::move(options));

    auto replicaId = Host_->CreateTableReplica(
        tableId,
        ETableReplicaMode::Sync);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replicaId);
}

TEST_F(TReplicatedTableTrackerTest, DisabledReplica)
{
    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodBundle(client);
    MockGoodTable(client);

    auto tableId = Host_->CreateReplicatedTable();
    auto replicaId = Host_->CreateTableReplica(
        tableId,
        ETableReplicaMode::Async,
        /*enabled*/ false);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replicaId);

    Host_->UpdateReplicaEnablement(replicaId, /*enabled*/ true);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);
}

TEST_F(TReplicatedTableTrackerTest, ReplicaWithDisabledTracking)
{
    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodBundle(client);
    MockBadTable(client);

    auto tableId = Host_->CreateReplicatedTable();
    auto replicaId = Host_->CreateTableReplica(tableId);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replicaId);

    Host_->UpdateReplicaTrackingPolicy(replicaId, /*enableTracking*/ false);
    Host_->UpdateReplicaMode(replicaId, ETableReplicaMode::Sync);

    WaitForUpdatesFromTracker();
    Host_->ValidateReplicaModeRemained(replicaId);
}

TEST_F(TReplicatedTableTrackerTest, TableWithDisabledTracking)
{
    // NB: No need to mock calls to client here.

    auto tableId = Host_->CreateReplicatedTable();
    auto options = Host_->GetTableOptions(tableId);
    options->EnableReplicatedTableTracker = false;
    Host_->SetTableOptions(tableId, std::move(options));

    auto replicaId = Host_->CreateTableReplica(tableId);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replicaId);
}

TEST_F(TReplicatedTableTrackerTest, DisabledTracker1)
{
    Host_->GetConfig()->EnableReplicatedTableTracker = false;
    WaitForUpdatesFromTracker();

    auto tableId = Host_->CreateReplicatedTable();
    auto replicaId = Host_->CreateTableReplica(tableId);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replicaId);
}

TEST_F(TReplicatedTableTrackerTest, DisabledTracker2)
{
    Tracker_->DisableTracking();

    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodBundle(client);
    MockGoodTable(client);

    auto tableId = Host_->CreateReplicatedTable();
    auto replicaId = Host_->CreateTableReplica(tableId);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replicaId);

    Tracker_->EnableTracking();
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);
}

TEST_F(TReplicatedTableTrackerTest, ReplicaLagPreference)
{
    auto client1 = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client1);
    MockGoodBundle(client1);
    MockGoodTable(client1);

    auto client2 = Host_->GetMockClient(Cluster2);
    MockGoodReplicaCluster(client2);
    MockGoodBundle(client2);
    MockGoodTable(client2);

    auto tableId = Host_->CreateReplicatedTable();
    auto replica1 = Host_->CreateTableReplica(
        tableId,
        ETableReplicaMode::Async,
        /*enabled*/ true,
        Cluster1,
        TablePath1,
        /*replicaLagTime*/ TDuration::Minutes(3));
    auto replica2 = Host_->CreateTableReplica(
        tableId,
        ETableReplicaMode::Async,
        /*enabled*/ true,
        Cluster2,
        TablePath1,
        /*replicaLagTime*/ TDuration::Minutes(2));

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replica1);
    Host_->ValidateReplicaModeChanged(replica2, ETableReplicaMode::Sync);

    Host_->SetReplicaLagTime(replica1, TDuration::Minutes(1));
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replica1);
    Host_->ValidateReplicaModeRemained(replica2);
}

TEST_F(TReplicatedTableTrackerTest, DestroyObjects)
{
    Tracker_->DisableTracking();

    {
        auto tableId = Host_->CreateReplicatedTable();
        auto replicaId = Host_->CreateTableReplica(tableId);
        Host_->DestroyTableReplica(replicaId);
        Host_->DestroyTable(tableId);
        WaitForUpdatesFromTracker();
    }

    {
        auto tableId = Host_->CreateReplicatedTable();
        auto collocationId = Host_->CreateReplicationCollocation({tableId});
        Host_->DestroyTable(tableId);
        Host_->DestroyReplicationCollocation(collocationId);
        WaitForUpdatesFromTracker();
    }

    {
        auto tableId = Host_->CreateReplicatedTable();
        auto collocationId = Host_->CreateReplicationCollocation({tableId});
        Host_->DestroyReplicationCollocation(collocationId);
        Host_->DestroyTable(tableId);
        WaitForUpdatesFromTracker();
    }
}

TEST_F(TReplicatedTableTrackerTest, ReplicaContentTypes)
{
    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodBundle(client);
    MockGoodBundle(client, TablePath2);
    MockGoodTable(client);
    MockGoodTable(client, TablePath2);

    auto tableId = Host_->CreateReplicatedTable();
    auto options = Host_->GetTableOptions(tableId);
    options->MaxSyncReplicaCount = 1;
    Host_->SetTableOptions(tableId, std::move(options));

    auto dataReplica = Host_->CreateTableReplica(
        tableId,
        ETableReplicaMode::Async,
        true,
        Cluster1,
        TablePath1,
        /*replicaLagTime*/ TDuration::Zero(),
        EObjectType::ChaosTableReplica,
        ETableReplicaContentType::Data);
    auto queueReplica = Host_->CreateTableReplica(
        tableId,
        ETableReplicaMode::Async,
        true,
        Cluster1,
        TablePath2,
        /*replicaLagTime*/ TDuration::Zero(),
        EObjectType::ChaosTableReplica,
        ETableReplicaContentType::Queue);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(dataReplica, ETableReplicaMode::Sync);
    Host_->ValidateReplicaModeChanged(queueReplica, ETableReplicaMode::Sync);

    MockBadTable(client);
    MockBadTable(client, TablePath2);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(dataReplica, ETableReplicaMode::Async);
    Host_->ValidateReplicaModeRemained(queueReplica);
}

TEST_F(TReplicatedTableTrackerTest, ClusterStateChecks)
{
    Host_->GetConfig()->ClusterStateCache->RefreshTime = CheckPeriod / 2;
    Host_->GetConfig()->ClusterStateCache->ExpireAfterFailedUpdateTime = CheckPeriod / 2;

    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodBundle(client);
    MockGoodTable(client);

    auto tableId = Host_->CreateReplicatedTable();
    auto replicaId = Host_->CreateTableReplica(tableId);

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);

    EXPECT_CALL(*client, GetNode("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/enable_incoming_replication", _))
        .WillRepeatedly(Return(MakeFuture(ConvertToYsonString(false))));
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Async);

    EXPECT_CALL(*client, GetNode("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/enable_incoming_replication", _))
        .WillRepeatedly(Return(MakeFuture<TYsonString>(TError(NYTree::EErrorCode::ResolveError, "Failure"))));
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);

    EXPECT_CALL(*client, GetNode("//sys/@hydra_read_only", _))
        .WillRepeatedly(Return(MakeFuture(ConvertToYsonString(true))));
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Async);

    EXPECT_CALL(*client, GetNode("//sys/@hydra_read_only", _))
        .WillRepeatedly(Return(MakeFuture(ConvertToYsonString(false))));
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);

    EXPECT_CALL(*client, GetNode("//sys/@config/enable_safe_mode", _))
        .WillRepeatedly(Return(MakeFuture(ConvertToYsonString(true))));
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Async);

    EXPECT_CALL(*client, GetNode("//sys/@config/enable_safe_mode", _))
        .WillRepeatedly(Return(MakeFuture(ConvertToYsonString(false))));
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);

    NApi::TCheckClusterLivenessOptions options{
        .CheckCypressRoot = true,
        .CheckSecondaryMasterCells = true,
    };
    EXPECT_CALL(*client, CheckClusterLiveness(options))
        .WillRepeatedly(Return(MakeFuture(TError("Failure"))));
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Async);

    EXPECT_CALL(*client, CheckClusterLiveness(options))
        .WillRepeatedly(Return(VoidFuture));
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);
}

TEST_F(TReplicatedTableTrackerTest, ReplicaLagTimeCapping)
{
    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodBundle(client);
    MockGoodTable(client);

    auto tableId = Host_->CreateReplicatedTable();
    auto replicaId = Host_->CreateTableReplica(
        tableId,
        ETableReplicaMode::Async,
        /*enabled*/ true,
        Cluster1,
        TablePath1,
        /*replicaLagTime*/ std::nullopt);

    auto options = Host_->GetTableOptions(tableId);
    options->SyncReplicaLagThreshold = TDuration::Seconds(5);
    Host_->SetTableOptions(tableId, std::move(options));

    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replicaId);

    Host_->SetReplicaLagTime(replicaId, TDuration::Seconds(1));
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Sync);

    Host_->SetReplicaLagTime(replicaId, TDuration::Minutes(10));
    WaitForTrackerWarmUp();
    Host_->ValidateReplicaModeRemained(replicaId);

    Sleep(TDuration::Seconds(10));
    Host_->ValidateReplicaModeChanged(replicaId, ETableReplicaMode::Async);
}

TEST_F(TReplicatedTableTrackerTest, ChangePreferredClustersOption)
{
    auto client = Host_->GetMockClient(Cluster1);
    MockGoodReplicaCluster(client);
    MockGoodBundle(client);
    MockGoodTable(client);

    auto client2 = Host_->GetMockClient(Cluster2);
    MockGoodReplicaCluster(client2);
    MockGoodBundle(client2);
    MockGoodTable(client2);

    auto tableId = Host_->CreateReplicatedTable();

    auto options = Host_->GetTableOptions(tableId);
    options->PreferredSyncReplicaClusters = {Cluster2};
    Host_->SetTableOptions(tableId, std::move(options));
    WaitForUpdatesFromTracker();

    auto replica1 = Host_->CreateTableReplica(tableId);
    auto replica2 = Host_->CreateTableReplica(
        tableId,
        ETableReplicaMode::Async,
        /*enabled*/ true,
        Cluster2);

    WaitForTrackerWarmUp();

    Host_->ValidateReplicaModeRemained(replica1);
    Host_->ValidateReplicaModeChanged(replica2, ETableReplicaMode::Sync);

    options = Host_->GetTableOptions(tableId);
    options->PreferredSyncReplicaClusters = {Cluster1, Cluster2};
    Host_->SetTableOptions(tableId, std::move(options));
    WaitForUpdatesFromTracker();

    Host_->ValidateReplicaModeRemained(replica1);
    Host_->ValidateReplicaModeRemained(replica2);

    options = Host_->GetTableOptions(tableId);
    options->PreferredSyncReplicaClusters = {Cluster1};
    Host_->SetTableOptions(tableId, std::move(options));
    WaitForUpdatesFromTracker();

    Host_->ValidateReplicaModeChanged(replica1, ETableReplicaMode::Sync);
    Host_->ValidateReplicaModeChanged(replica2, ETableReplicaMode::Async);

    options = Host_->GetTableOptions(tableId);
    options->PreferredSyncReplicaClusters = {Cluster1, Cluster2};
    Host_->SetTableOptions(tableId, std::move(options));
    WaitForUpdatesFromTracker();

    Host_->ValidateReplicaModeRemained(replica1);
    Host_->ValidateReplicaModeRemained(replica2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletServer
