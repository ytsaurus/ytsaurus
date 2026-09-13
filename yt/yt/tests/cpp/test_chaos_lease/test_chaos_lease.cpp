#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/generic/hash_set.h>

#include <util/random/random.h>

#include <atomic>

namespace NYT::NCppTests {

using namespace NApi;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

constinit const auto Logger = CppTestsLogger;

////////////////////////////////////////////////////////////////////////////////

static TCellId GenerateChaosCellId(TCellTag cellTag)
{
    return MakeId(
        EObjectType::ChaosCell,
        cellTag,
        RandomNumber<ui64>() % (1ull << 48),
        RandomNumber<ui32>());
}

////////////////////////////////////////////////////////////////////////////////

class TChaosLeaseTest
    : public TApiTestBase
    , public ::testing::WithParamInterface<bool>
{
protected:
    static TCellId ChaosCell1_;
    static TCellId ChaosCell2_;

    TCellId EnabledCell_;
    TCellId DisabledCell_;

    static void SetUpTestCase()
    {
        TApiTestBase::SetUpTestCase();

        CreateChaosCellBundle();

        ChaosCell1_ = SyncCreateChaosCell(TCellTag(202));
        ChaosCell2_ = SyncCreateChaosCell(TCellTag(203));
    }

    void SetUp() override
    {
        ResumeBothCells();
        std::tie(EnabledCell_, DisabledCell_) = FindEnabledAndDisabledCells();
        YT_TLOG_INFO("Test starting")
            .With("EnabledCell", EnabledCell_)
            .With("DisabledCell", DisabledCell_)
            .With("Migrate", GetParam());
    }

    void TearDown() override
    {
        ResumeBothCells();
    }

    void MaybeMigrate()
    {
        if (!GetParam()) {
            return;
        }

        WaitFor(Client_->SuspendChaosCells({EnabledCell_}))
            .ThrowOnError();

        WaitUntil(
            [&] { return GetLeaseManagerState(DisabledCell_) == "enabled"; },
            "lease manager did not migrate",
            {.Timeout = TDuration::Seconds(120), .IgnoreExceptions = true});

        std::swap(EnabledCell_, DisabledCell_);
        YT_TLOG_INFO("Migrated lease manager")
            .With("NewEnabledCell", EnabledCell_);
    }

    static IPrerequisitePtr CreateLease(
        TCellId cellId,
        TDuration timeout,
        TObjectId parentId = {})
    {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("chaos_cell_id", cellId);
        attributes->Set("timeout", timeout);
        if (parentId) {
            attributes->Set("parent_id", parentId);
        }

        TCreateObjectOptions options;
        options.Attributes = std::move(attributes);
        auto leaseId = WaitFor(Client_->CreateObject(EObjectType::ChaosLease, options))
            .ValueOrThrow();
        return WaitFor(Client_->AttachChaosLease(leaseId, {}))
            .ValueOrThrow();
    }

    static bool LeaseExists(TObjectId leaseId)
    {
        try {
            return WaitFor(Client_->NodeExists(Format("#%v", leaseId)))
                .ValueOrThrow();
        } catch (const TErrorException& ex) {
            if (ex.Error().FindMatching(NYTree::EErrorCode::ResolveError)) {
                return false;
            }
            throw;
        }
    }

    static void CreateChaosCellBundle()
    {
        auto primaryCellTag = ConvertTo<int>(WaitFor(Client_->GetNode("//sys/@primary_cell_tag"))
            .ValueOrThrow());

        auto attributes = CreateEphemeralAttributes();
        attributes->SetYson("name", ConvertToYsonString("c"));
        attributes->SetYson("chaos_options", BuildYsonStringFluently()
            .BeginMap()
                .Item("peers").BeginList()
                    .Item().BeginMap().EndMap()
                .EndList()
            .EndMap());
        attributes->SetYson("options", BuildYsonStringFluently()
            .BeginMap()
                .Item("changelog_account").Value("sys")
                .Item("snapshot_account").Value("sys")
                .Item("peer_count").Value(1)
                .Item("independent_peers").Value(true)
                .Item("clock_cluster_tag").Value(primaryCellTag)
            .EndMap());

        TCreateObjectOptions options;
        options.Attributes = std::move(attributes);
        WaitFor(Client_->CreateObject(EObjectType::ChaosCellBundle, options))
            .ValueOrThrow();

        YT_TLOG_INFO("Chaos cell bundle created");
    }

    static TCellId SyncCreateChaosCell(TCellTag cellTag)
    {
        auto cellId = GenerateChaosCellId(cellTag);

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("id", cellId);
        attributes->SetYson("cell_bundle", ConvertToYsonString("c"));
        attributes->SetYson("area", ConvertToYsonString("default"));

        TCreateObjectOptions options;
        options.Attributes = std::move(attributes);
        auto resultId = WaitFor(Client_->CreateObject(EObjectType::ChaosCell, options))
            .ValueOrThrow();
        EXPECT_EQ(resultId, cellId);

        WaitUntilEqual(Format("#%v/@health", cellId), "good");

        YT_TLOG_INFO("Chaos cell created and healthy")
            .With("CellId", cellId);
        return cellId;
    }

    static TYPath GetCellOrchidPath(TCellId cellId)
    {
        auto address = ConvertTo<std::string>(WaitFor(Client_->GetNode(Format("#%v/@peers/0/address", cellId)))
            .ValueOrThrow());
        return Format("//sys/cluster_nodes/%v/orchid/chaos_cells/%v", address, cellId);
    }

    static std::string GetLeaseManagerState(TCellId cellId)
    {
        auto path = GetCellOrchidPath(cellId) + "/chaos_lease_manager/internal/state";
        return ConvertTo<std::string>(WaitFor(Client_->GetNode(path))
            .ValueOrThrow());
    }

    static void ResumeBothCells()
    {
        WaitFor(Client_->ResumeChaosCells({ChaosCell1_, ChaosCell2_}))
            .ThrowOnError();
    }

    static std::pair<TCellId, TCellId> FindEnabledAndDisabledCells()
    {
        TCellId enabled, disabled;
        WaitUntil(
            [&] {
                auto s1 = GetLeaseManagerState(ChaosCell1_);
                auto s2 = GetLeaseManagerState(ChaosCell2_);
                if (s1 == "enabled" && s2 == "disabled") {
                    enabled = ChaosCell1_;
                    disabled = ChaosCell2_;
                    return true;
                }
                if (s2 == "enabled" && s1 == "disabled") {
                    enabled = ChaosCell2_;
                    disabled = ChaosCell1_;
                    return true;
                }
                return false;
            },
            "chaos_lease_manager enabled/disabled pair not established",
            {.IgnoreExceptions = true});
        return {enabled, disabled};
    }
};

TCellId TChaosLeaseTest::ChaosCell1_;
TCellId TChaosLeaseTest::ChaosCell2_;

////////////////////////////////////////////////////////////////////////////////

INSTANTIATE_TEST_SUITE_P(
    Migration,
    TChaosLeaseTest,
    ::testing::Bool(),
    [] (const auto& info) -> std::string {
        return info.param ? "WithMigration" : "NoMigration";
    });

////////////////////////////////////////////////////////////////////////////////

TEST_P(TChaosLeaseTest, PingProlongsTtl)
{
    auto lease = CreateLease(EnabledCell_, TDuration::Seconds(5));

    YT_TLOG_INFO("Lease created")
        .With("LeaseId", lease->GetId());

    MaybeMigrate();

    for (int i = 0; i < 5; ++i) {
        Sleep(TDuration::Seconds(1));
        WaitFor(lease->Ping())
            .ThrowOnError();
        YT_TLOG_INFO("Ping succeeded")
                .With("Index", i);
    }

    ASSERT_TRUE(LeaseExists(lease->GetId()));

    WaitUntil(
        [&] { return !LeaseExists(lease->GetId()); },
        "lease did not expire after pinging stopped");

    YT_TLOG_INFO("Lease expired after pinging stopped");
}

TEST_P(TChaosLeaseTest, RemoveParentCascadesToChildren)
{
    MaybeMigrate();

    auto parent = CreateLease(EnabledCell_, TDuration::Seconds(120));
    auto child1 = CreateLease(EnabledCell_, TDuration::Seconds(120), parent->GetId());
    auto child2 = CreateLease(EnabledCell_, TDuration::Seconds(120), parent->GetId());

    YT_TLOG_INFO("Lease tree created")
        .With("Parent", parent->GetId())
        .With("Child1", child1->GetId())
        .With("Child2", child2->GetId());

    ASSERT_TRUE(LeaseExists(parent->GetId()));
    ASSERT_TRUE(LeaseExists(child1->GetId()));
    ASSERT_TRUE(LeaseExists(child2->GetId()));

    WaitFor(Client_->RemoveNode(Format("#%v", parent->GetId())))
        .ThrowOnError();

    WaitUntil(
        [&] {
            return
                !LeaseExists(parent->GetId()) &&
                !LeaseExists(child1->GetId()) &&
                !LeaseExists(child2->GetId());
        },
        "lease hierarchy was not fully removed");
}

TEST_P(TChaosLeaseTest, PingFailsAfterExpiration)
{
    auto lease = CreateLease(EnabledCell_, TDuration::Seconds(2));

    WaitFor(lease->Ping())
        .ThrowOnError();

    YT_TLOG_INFO("Initial ping succeeded")
        .With("LeaseId", lease->GetId());

    MaybeMigrate();

    WaitUntil(
        [&] { return !LeaseExists(lease->GetId()); },
        "lease did not expire");

    std::atomic<bool> aborted = false;
    lease->SubscribeAborted(BIND([&] (const TError& /*error*/) {
        aborted.store(true);
    }));

    auto result = WaitFor(lease->Ping());
    ASSERT_FALSE(result.IsOK());
    ASSERT_TRUE(aborted.load());
    YT_TLOG_INFO("Ping after expiration failed as expected")
        .With("Error", result.GetCode());
}

TEST_P(TChaosLeaseTest, WatchCoordinatorChange)
{
    auto lease = CreateLease(EnabledCell_, TDuration::Seconds(120));
    auto leaseId = lease->GetId();

    auto connectionConfig = ConvertTo<NNative::TConnectionCompoundConfigPtr>(
        WaitFor(Client_->GetNode("//sys/@cluster_connection"))
            .ValueOrThrow());
    auto nativeConnection = NNative::CreateConnection(std::move(connectionConfig));
    auto terminateConnectionGuard = Finally([&] {
        nativeConnection->Terminate();
    });

    auto watchCellId = EnabledCell_;
    auto invokeWatch = [&] (TTimestamp cacheTimestamp) {
        TChaosNodeServiceProxy proxy(nativeConnection->GetChaosChannelByCellId(watchCellId));
        proxy.SetDefaultTimeout(TDuration::Seconds(120));

        auto request = proxy.WatchChaosLease();
        ToProto(request->mutable_chaos_lease_id(), leaseId);
        request->set_chaos_lease_cache_timestamp(ToProto(cacheTimestamp));
        return request->Invoke();
    };

    THashSet<TCellId> expectedInitialCoordinatorCellIds{ChaosCell1_, ChaosCell2_};

    auto cacheTimestamp = NullTimestamp;
    auto watchFuture = invokeWatch(cacheTimestamp);
    auto waitForCoordinators = [&] (const THashSet<TCellId>& expectedCoordinatorCellIds) {
        WaitUntil(
            [&] {
                auto response = WaitFor(watchFuture)
                    .ValueOrThrow();
                if (response->has_chaos_lease_not_changed()) {
                    watchFuture = invokeWatch(cacheTimestamp);
                    return false;
                }

                if (!response->has_chaos_lease_changed()) {
                    THROW_ERROR_EXCEPTION("Unexpected WatchChaosLease response")
                        .With("response_case", static_cast<int>(response->chaos_lease_state_case()));
                }

                const auto& changedLease = response->chaos_lease_changed();
                cacheTimestamp = FromProto<TTimestamp>(changedLease.chaos_lease_cache_timestamp());

                auto coordinatorCellIds = FromProto<THashSet<TCellId>>(changedLease.coordinator_cell_ids());
                if (coordinatorCellIds == expectedCoordinatorCellIds) {
                    return true;
                }

                watchFuture = invokeWatch(cacheTimestamp);
                return false;
            },
            "lease did not acquire expected coordinators",
            {.Timeout = TDuration::Seconds(120)});
    };

    waitForCoordinators(expectedInitialCoordinatorCellIds);

    MaybeMigrate();
    if (GetParam()) {
        watchCellId = EnabledCell_;
        ResumeBothCells();
        watchFuture = invokeWatch(cacheTimestamp);
        waitForCoordinators(expectedInitialCoordinatorCellIds);
    }

    watchFuture = invokeWatch(cacheTimestamp);

    auto expectedUpdatedCoordinatorCellIds = expectedInitialCoordinatorCellIds;
    auto coordinatorToSuspend = ChaosCell2_;
    expectedUpdatedCoordinatorCellIds.erase(coordinatorToSuspend);
    WaitFor(Client_->SuspendCoordinator(coordinatorToSuspend))
        .ThrowOnError();
    auto resumeCoordinatorGuard = Finally([&] {
        WaitFor(Client_->ResumeCoordinator(coordinatorToSuspend))
            .ThrowOnError();
    });

    waitForCoordinators(expectedUpdatedCoordinatorCellIds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCppTests
