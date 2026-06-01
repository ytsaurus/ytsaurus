#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/server/lib/chaos_election/config.h>
#include <yt/yt/server/lib/chaos_election/election_manager.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/random/random.h>

namespace NYT {
namespace {

using namespace NApi;
using namespace NChaosElection;
using namespace NConcurrency;
using namespace NCppTests;
using namespace NLockElection;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

constinit const auto Logger = CppTestsLogger;

////////////////////////////////////////////////////////////////////////////////

static constexpr TStringBuf LockTablePath = "//tmp/chaos_election_lock";

static TCellId GenerateChaosCellId(int cellTag)
{
    return TObjectId(
        RandomNumber<ui32>(),
        static_cast<ui32>(cellTag << 16) | static_cast<ui32>(EObjectType::ChaosCell),
        RandomNumber<ui32>(),
        RandomNumber<ui32>() % (1u << 16));
}

////////////////////////////////////////////////////////////////////////////////

class TChaosElectionTest
    : public TDynamicTablesTestBase
{
protected:
    static TCellId ChaosCell1_;
    static TCellId ChaosCell2_;

    TCellId EnabledCell_;
    TCellId DisabledCell_;

    TActionQueuePtr ActionQueue_ = New<TActionQueue>();

    std::atomic<int> StartCount_ = 0;
    std::atomic<int> EndCount_ = 0;

    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();

        SetupChaosConfig();
        CreateChaosCellBundle();

        ChaosCell1_ = SyncCreateChaosCell(204);
        ChaosCell2_ = SyncCreateChaosCell(205);

        WaitFor(Client_->SetNode(
            "//sys/chaos_cell_bundles/c/@metadata_cell_ids",
            ConvertToYsonString(std::vector<TCellId>{ChaosCell1_, ChaosCell2_})))
            .ThrowOnError();
    }

    static void TearDownTestCase()
    {
        WaitFor(Client_->RemoveNode(Format("#%v", ChaosCell1_)))
            .ThrowOnError();
        WaitFor(Client_->RemoveNode(Format("#%v", ChaosCell2_)))
            .ThrowOnError();

        TDynamicTablesTestBase::TearDownTestCase();
    }

    void SetUp() override
    {
        StartCount_ = 0;
        EndCount_ = 0;

        ResumeBothCells();
        std::tie(EnabledCell_, DisabledCell_) = FindEnabledAndDisabledCells();

        CreateLockTable();
    }

    void TearDown() override
    {
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode(TString(LockTablePath))).IsOK();
        };
        WaitForPredicate(tryRemove, /*iterationCount*/ 100, /*period*/ TDuration::MilliSeconds(200));

        ResumeBothCells();
    }

    ILockElectionManagerPtr CreateElectionManager(
        const TString& memberName,
        TActionQueuePtr actionQueue = nullptr)
    {
        auto config = New<TChaosElectionManagerConfig>();
        config->LockTablePath = TString(LockTablePath);
        config->ChaosCellBundle = "c";
        config->LeaseTimeout = TDuration::Seconds(10);
        config->LeasePingPeriod = TDuration::MilliSeconds(500);
        config->LockAcquisitionPeriod = TDuration::MilliSeconds(500);

        auto options = New<TChaosElectionManagerOptions>();
        options->GroupName = "test_group";
        options->MemberName = memberName;

        if (!actionQueue) {
            actionQueue = ActionQueue_;
        }

        auto electionManager = NChaosElection::CreateChaosElectionManager(
            Client_,
            actionQueue->GetInvoker(),
            config,
            options);

        electionManager->SubscribeLeadingStarted(BIND([this] {
            ++StartCount_;
        }));
        electionManager->SubscribeLeadingEnded(BIND([this] {
            ++EndCount_;
        }));

        return electionManager;
    }

    static bool IsActive(NPrerequisiteClient::TPrerequisiteId prerequisiteId)
    {
        if (prerequisiteId == NPrerequisiteClient::TPrerequisiteId{}) {
            return false;
        }
        auto result = WaitFor(Client_->NodeExists(Format("#%v", prerequisiteId)));
        return result.IsOK() && result.Value();
    }

    void MigrateLeaseManager()
    {
        WaitFor(Client_->SuspendChaosCells({EnabledCell_}))
            .ThrowOnError();

        WaitUntil(
            [&] { return GetLeaseManagerState(DisabledCell_) == "enabled"; },
            "lease manager did not migrate",
            {.Timeout = TDuration::Seconds(120), .IgnoreExceptions = true});

        std::swap(EnabledCell_, DisabledCell_);
        YT_LOG_INFO("Migrated lease manager (NewEnabledCell: %v)", EnabledCell_);
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

    static void CreateLockTable()
    {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("dynamic", true);
        attributes->Set("schema", *GetChaosElectionLockTableSchema());

        TCreateNodeOptions options;
        options.Attributes = std::move(attributes);

        WaitFor(Client_->CreateNode(TString(LockTablePath), EObjectType::Table, options))
            .ThrowOnError();
        SyncMountTable(TString(LockTablePath));
    }

    static void SetupChaosConfig()
    {
        WaitFor(Client_->SetNode(
            "//sys/@config/chaos_manager/alien_cell_synchronizer",
            ConvertToYsonString(BuildYsonNodeFluently()
                .BeginMap()
                    .Item("enable").Value(true)
                    .Item("sync_period").Value(100)
                    .Item("full_sync_period").Value(200)
                .EndMap())))
            .ThrowOnError();

        WaitFor(Client_->SetNode(
            "//sys/@config/node_tracker/master_cache_manager",
            ConvertToYsonString(BuildYsonNodeFluently()
                .BeginMap()
                    .Item("peer_count").Value(1)
                    .Item("update_period").Value(100)
                    .Item("node_tag_filter").Value("master_cache")
                .EndMap())))
            .ThrowOnError();

        TListNodeOptions listOptions;
        listOptions.Attributes = TAttributeFilter({"state", "flavors"});
        auto nodesNode = WaitFor(Client_->ListNode(
            "//sys/cluster_nodes", listOptions))
            .ValueOrThrow();
        for (const auto& node : ConvertTo<IListNodePtr>(nodesNode)->GetChildren()) {
            auto flavors = node->Attributes().Get<std::vector<TString>>("flavors");
            if (std::find(flavors.begin(), flavors.end(), "chaos") != flavors.end()) {
                auto address = node->AsString()->GetValue();
                WaitFor(Client_->SetNode(
                    Format("//sys/cluster_nodes/%v/@user_tags/end", address),
                    ConvertToYsonString("chaos_cache")))
                    .ThrowOnError();
            }
        }
    }

    static void CreateChaosCellBundle()
    {
        auto primaryCellTag = ConvertTo<int>(WaitFor(Client_->GetNode("//sys/@primary_cell_tag"))
            .ValueOrThrow());

        auto attributes = NYTree::CreateEphemeralAttributes();
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

    static TCellId SyncCreateChaosCell(int cellTag)
    {
        auto cellId = GenerateChaosCellId(cellTag);

        auto attributes = NYTree::CreateEphemeralAttributes();
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
};

TCellId TChaosElectionTest::ChaosCell1_;
TCellId TChaosElectionTest::ChaosCell2_;

////////////////////////////////////////////////////////////////////////////////

TEST_F(TChaosElectionTest, SingleManagerBecomesLeader)
{
    auto manager = CreateElectionManager("member1");
    manager->Start();

    WaitForPredicate([&] {
        return manager->IsLeader();
    });

    EXPECT_TRUE(manager->IsLeader());
    EXPECT_TRUE(IsActive(manager->GetPrerequisiteId()));
    EXPECT_EQ(StartCount_.load(), 1);
    EXPECT_EQ(EndCount_.load(), 0);

    YT_LOG_INFO("Single manager became leader (PrerequisiteId: %v)",
        manager->GetPrerequisiteId());

    WaitFor(manager->Stop())
        .ThrowOnError();

    EXPECT_FALSE(manager->IsLeader());
    EXPECT_EQ(StartCount_.load(), 1);
    EXPECT_EQ(EndCount_.load(), 1);
}

TEST_F(TChaosElectionTest, StopLeadingAndReElect)
{
    auto manager = CreateElectionManager("member1");
    manager->Start();

    WaitForPredicate([&] {
        return manager->IsLeader();
    });

    auto firstPrerequisiteId = manager->GetPrerequisiteId();
    EXPECT_EQ(StartCount_.load(), 1);

    YT_LOG_INFO("Stopping leading (FirstPrerequisiteId: %v)", firstPrerequisiteId);

    YT_UNUSED_FUTURE(manager->StopLeading());

    WaitForPredicate([&] {
        return !manager->IsLeader();
    });
    EXPECT_EQ(EndCount_.load(), 1);

    WaitForPredicate([&] {
        return manager->IsLeader();
    });

    auto secondPrerequisiteId = manager->GetPrerequisiteId();
    EXPECT_NE(firstPrerequisiteId, secondPrerequisiteId);
    EXPECT_EQ(StartCount_.load(), 2);
    EXPECT_EQ(EndCount_.load(), 1);

    YT_LOG_INFO("Re-elected (SecondPrerequisiteId: %v)", secondPrerequisiteId);

    WaitFor(manager->Stop())
        .ThrowOnError();

    EXPECT_EQ(StartCount_.load(), 2);
    EXPECT_EQ(EndCount_.load(), 2);
}

TEST_F(TChaosElectionTest, TwoManagersExactlyOneLeader)
{
    auto queue1 = New<TActionQueue>();
    auto queue2 = New<TActionQueue>();

    auto manager1 = CreateElectionManager("member1", queue1);
    auto manager2 = CreateElectionManager("member2", queue2);

    manager1->Start();
    manager2->Start();

    WaitForPredicate([&] {
        return manager1->IsLeader() || manager2->IsLeader();
    });

    WaitForPredicate([&] {
        return manager1->IsLeader() != manager2->IsLeader();
    }, "expected exactly one leader");

    auto* leader = manager1->IsLeader() ? &manager1 : &manager2;
    auto* follower = manager1->IsLeader() ? &manager2 : &manager1;

    YT_LOG_INFO("Leader elected (LeaderPrerequisiteId: %v)",
        (*leader)->GetPrerequisiteId());

    WaitFor((*leader)->Stop())
        .ThrowOnError();

    WaitForPredicate([&] {
        return (*follower)->IsLeader();
    });

    EXPECT_TRUE((*follower)->IsLeader());
    EXPECT_FALSE((*leader)->IsLeader());

    YT_LOG_INFO("Follower took over (NewLeaderPrerequisiteId: %v)",
        (*follower)->GetPrerequisiteId());

    WaitFor((*follower)->Stop())
        .ThrowOnError();
}

TEST_F(TChaosElectionTest, ExternalLeaseRemoval)
{
    auto manager = CreateElectionManager("member1");
    manager->Start();

    WaitForPredicate([&] {
        return manager->IsLeader();
    });

    auto prerequisiteId = manager->GetPrerequisiteId();
    EXPECT_TRUE(IsActive(prerequisiteId));
    EXPECT_EQ(StartCount_.load(), 1);

    YT_LOG_INFO("Removing prerequisite externally (PrerequisiteId: %v)", prerequisiteId);

    WaitFor(Client_->RemoveNode(Format("#%v", prerequisiteId)))
        .ThrowOnError();

    EXPECT_FALSE(IsActive(prerequisiteId));

    WaitForPredicate([&] {
        auto newId = manager->GetPrerequisiteId();
        return newId != prerequisiteId && IsActive(newId);
    });

    EXPECT_EQ(StartCount_.load(), 2);
    EXPECT_EQ(EndCount_.load(), 1);

    YT_LOG_INFO("Re-elected after external removal (NewPrerequisiteId: %v)",
        manager->GetPrerequisiteId());

    WaitFor(manager->Stop())
        .ThrowOnError();

    EXPECT_EQ(StartCount_.load(), 2);
    EXPECT_EQ(EndCount_.load(), 2);
}

TEST_F(TChaosElectionTest, LeaderSurvivesMigration)
{
    auto manager = CreateElectionManager("member1");
    manager->Start();

    WaitForPredicate([&] {
        return manager->IsLeader();
    });

    YT_LOG_INFO("Leader elected before migration (PrerequisiteId: %v)",
        manager->GetPrerequisiteId());

    MigrateLeaseManager();

    WaitForPredicate([&] {
        return manager->IsLeader();
    });

    EXPECT_TRUE(IsActive(manager->GetPrerequisiteId()));

    YT_LOG_INFO("Leader survived migration (PrerequisiteId: %v)",
        manager->GetPrerequisiteId());

    WaitFor(manager->Stop())
        .ThrowOnError();
}

TEST_F(TChaosElectionTest, MigrationDoesNotReElectStableLeader)
{
    auto manager = CreateElectionManager("member1");
    manager->Start();

    WaitForPredicate([&] {
        return manager->IsLeader();
    });

    auto prerequisiteId = manager->GetPrerequisiteId();
    EXPECT_EQ(StartCount_.load(), 1);
    EXPECT_EQ(EndCount_.load(), 0);

    YT_LOG_INFO("Leader elected before migration (PrerequisiteId: %v)", prerequisiteId);

    MigrateLeaseManager();

    Sleep(TDuration::Seconds(5));

    EXPECT_TRUE(manager->IsLeader());
    EXPECT_EQ(manager->GetPrerequisiteId(), prerequisiteId);
    EXPECT_EQ(StartCount_.load(), 1);
    EXPECT_EQ(EndCount_.load(), 0);

    YT_LOG_INFO("Leader stable after migration (PrerequisiteId: %v)", prerequisiteId);

    WaitFor(manager->Stop())
        .ThrowOnError();
}

TEST_F(TChaosElectionTest, TwoManagersMigrationFailover)
{
    auto queue1 = New<TActionQueue>();
    auto queue2 = New<TActionQueue>();

    auto manager1 = CreateElectionManager("member1", queue1);
    auto manager2 = CreateElectionManager("member2", queue2);

    manager1->Start();
    manager2->Start();

    WaitForPredicate([&] {
        return manager1->IsLeader() != manager2->IsLeader();
    }, /*iterationCount*/ 600, /*period*/ TDuration::MilliSeconds(200));

    YT_LOG_INFO("Leader elected before migration (Manager1IsLeader: %v, Manager2IsLeader: %v)",
        manager1->IsLeader(), manager2->IsLeader());

    MigrateLeaseManager();

    WaitForPredicate([&] {
        return manager1->IsLeader() || manager2->IsLeader();
    }, /*iterationCount*/ 600, /*period*/ TDuration::MilliSeconds(200));
    WaitForPredicate([&] {
        return manager1->IsLeader() != manager2->IsLeader();
    }, "expected exactly one leader after migration");

    YT_LOG_INFO("Leader after migration (Manager1IsLeader: %v, Manager2IsLeader: %v)",
        manager1->IsLeader(), manager2->IsLeader());

    WaitFor(manager1->Stop())
        .ThrowOnError();
    WaitFor(manager2->Stop())
        .ThrowOnError();
}

TEST_F(TChaosElectionTest, ElectionFindsEnabledCellAfterMigration)
{
    MigrateLeaseManager();

    auto manager = CreateElectionManager("member1");
    manager->Start();

    WaitForPredicate([&] {
        return manager->IsLeader();
    });

    EXPECT_TRUE(manager->IsLeader());

    YT_LOG_INFO("Manager elected after migration (PrerequisiteId: %v)",
        manager->GetPrerequisiteId());

    WaitFor(manager->Stop())
        .ThrowOnError();
}

TEST_F(TChaosElectionTest, ElectionWorksOnNonEmptyTable)
{
    auto manager = CreateElectionManager("member1");
    manager->Start();

    WaitForPredicate([&] {
        return manager->IsLeader();
    });

    auto leaderLeaseId = manager->GetPrerequisiteId();

    WaitFor(manager->Stop())
        .ThrowOnError();

    auto query = Format("* FROM [%v]", LockTablePath);
    auto selectResult = WaitFor(Client_->SelectRows(query))
        .ValueOrThrow();

    auto rows = selectResult.Rowset->GetRows();

    YT_LOG_INFO("Started and stopped leader (ElectionTableRowsCount: %v)", rows.size());
    EXPECT_EQ(std::ssize(rows), 1);

    manager->Start();
    WaitForPredicate([&] {
        return manager->IsLeader();
    });

    EXPECT_NE(leaderLeaseId, manager->GetPrerequisiteId());

    auto newSelectResult = WaitFor(Client_->SelectRows(query))
        .ValueOrThrow();

    auto newRows = newSelectResult.Rowset->GetRows();

    YT_LOG_INFO("Selected rows after new election started (ElectionTableRowsCount: %v)", newRows.size());
    EXPECT_EQ(std::ssize(newRows), 1);

    EXPECT_NE(rows[0], newRows[0]);

    WaitFor(manager->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
