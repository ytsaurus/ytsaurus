#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/random/random.h>

namespace NYT::NCppTests {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
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
        YT_LOG_INFO("Test starting (EnabledCell: %v, DisabledCell: %v, Migrate: %v)",
            EnabledCell_, DisabledCell_, GetParam());
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
        YT_LOG_INFO("Migrated lease manager (NewEnabledCell: %v)", EnabledCell_);
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

        YT_LOG_INFO("Chaos cell bundle created");
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

        YT_LOG_INFO("Chaos cell created and healthy (CellId: %v)", cellId);
        return cellId;
    }

    static TYPath GetCellOrchidPath(TCellId cellId)
    {
        auto address = ConvertTo<TString>(WaitFor(Client_->GetNode(Format("#%v/@peers/0/address", cellId)))
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

    YT_LOG_INFO("Lease created (LeaseId: %v)", lease->GetId());

    MaybeMigrate();

    for (int i = 0; i < 5; ++i) {
        Sleep(TDuration::Seconds(1));
        WaitFor(lease->Ping())
            .ThrowOnError();
        YT_LOG_INFO("Ping %v succeeded", i);
    }

    ASSERT_TRUE(LeaseExists(lease->GetId()));

    WaitUntil(
        [&] { return !LeaseExists(lease->GetId()); },
        "lease did not expire after pinging stopped");

    YT_LOG_INFO("Lease expired after pinging stopped");
}

TEST_P(TChaosLeaseTest, RemoveParentCascadesToChildren)
{
    MaybeMigrate();

    auto parent = CreateLease(EnabledCell_, TDuration::Seconds(120));
    auto child1 = CreateLease(EnabledCell_, TDuration::Seconds(120), parent->GetId());
    auto child2 = CreateLease(EnabledCell_, TDuration::Seconds(120), parent->GetId());

    YT_LOG_INFO("Lease tree created (Parent: %v, Child1: %v, Child2: %v)",
        parent->GetId(), child1->GetId(), child2->GetId());

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

    YT_LOG_INFO("Initial ping succeeded (LeaseId: %v)", lease->GetId());

    MaybeMigrate();

    WaitUntil(
        [&] { return !LeaseExists(lease->GetId()); },
        "lease did not expire");

    auto result = WaitFor(lease->Ping());
    ASSERT_FALSE(result.IsOK());
    YT_LOG_INFO("Ping after expiration failed as expected (Error: %v)",
        result.GetCode());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCppTests
