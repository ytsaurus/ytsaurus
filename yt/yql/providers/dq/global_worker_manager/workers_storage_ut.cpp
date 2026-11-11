#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/dq/global_worker_manager/workers_storage.h>
#include <yt/yql/providers/dq/global_worker_manager/worker_filter.h>
#include <yt/yql/providers/dq/global_worker_manager/literal_requests_hard_reserve.h>

#include <algorithm>
#include <limits>

using namespace NYql;
using namespace NYql::NDq;

namespace {

TResourceManagerOptions MakeBackend(
    const TString& clusterName,
    ui32 percent = 0,
    ui64 minSlots = 0,
    const TMaybe<TString>& announcedClusterName = {})
{
    TResourceManagerOptions options;
    options.YtBackend.SetClusterName(clusterName);
    options.YtBackend.SetDqLiteralReservePercent(percent);
    options.YtBackend.SetDqLiteralReserveMinSlots(minSlots);
    options.AnnounceClusterName = announcedClusterName;
    return options;
}

NDqProto::TAllocateWorkersRequest MakeClusterRequest(
    std::initializer_list<TString> clusters,
    ui32 workersCount = 0)
{
    NDqProto::TAllocateWorkersRequest request;
    request.SetCount(clusters.size());
    request.SetWorkersCount(workersCount);
    for (const auto& cluster : clusters) {
        request.AddWorkerFilterPerTask()->SetClusterName(cluster);
    }
    return request;
}

struct TStorageScenario {
    TWorkersStorage Storage{1, new TSensorsGroup, new TSensorsGroup, /*trackSlotsSnapshot=*/true};
    ui32 NextNodeId = 100;

    TStorageScenario() {
        Storage.Clear();
    }

    std::pair<TGUID, NDqs::TWorkerInfo::TPtr> AddWorker(
        const TString& clusterName,
        ui32 capacity,
        bool withSharedFile = false,
        ui32 runningWorkerActors = 0)
    {
        TGUID guid;
        CreateGuid(&guid);
        Yql::DqsProto::RegisterNodeRequest request;
        request.SetCapacity(capacity);
        request.SetClusterName(clusterName);
        request.SetRunningWorkers(runningWorkerActors);
        request.AddKnownNodes(1);
        if (withSharedFile) {
            request.AddFilesOnNode()->SetObjectId("shared-file");
        }
        auto [worker, _] = Storage.CreateOrUpdate(NextNodeId++, guid, request);
        return {guid, worker};
    }

    void AddFile(
        const TGUID& guid,
        const NDqs::TWorkerInfo::TPtr& worker,
        const TString& fileId)
    {
        Yql::DqsProto::RegisterNodeRequest request;
        request.SetCapacity(worker->Capacity);
        request.SetClusterName(worker->ClusterName);
        request.AddKnownNodes(1);
        request.AddFilesOnNode()->SetObjectId(fileId);
        Storage.CreateOrUpdate(worker->NodeId, guid, request);
    }
};

void AssertClusters(
    const TVector<NDqs::TWorkerInfo::TPtr>& workers,
    std::initializer_list<TString> expected)
{
    UNIT_ASSERT_VALUES_EQUAL(workers.size(), expected.size());
    auto worker = workers.begin();
    for (const auto& cluster : expected) {
        UNIT_ASSERT_VALUES_EQUAL((*worker++)->ClusterName, cluster);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TLiteralRequestsHardReserveSettingsTest) {
    Y_UNIT_TEST(AbsentAndAllZeroDisableReserve) {
        const TLiteralRequestsHardReserveSettings absent({});
        UNIT_ASSERT(!absent.IsEnabled());

        TResourceManagerOptions unsetBackend;
        unsetBackend.YtBackend.SetClusterName("plato");
        const TLiteralRequestsHardReserveSettings unset({unsetBackend});
        UNIT_ASSERT(!unset.IsEnabled());

        const TLiteralRequestsHardReserveSettings zero({MakeBackend("plato")});
        UNIT_ASSERT(!zero.IsEnabled());
    }

    Y_UNIT_TEST(CalculatesMinOnlyReserve) {
        const TLiteralRequestsHardReserveSettings settings({MakeBackend("plato", 0, 3)});
        UNIT_ASSERT(settings.IsEnabled());
        const auto* cluster = settings.Find("plato");
        UNIT_ASSERT(cluster);
        UNIT_ASSERT_VALUES_EQUAL(cluster->CalculateReserve(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(cluster->CalculateReserve(2), 2);
        UNIT_ASSERT_VALUES_EQUAL(cluster->CalculateReserve(3), 3);
        UNIT_ASSERT_VALUES_EQUAL(cluster->CalculateReserve(10), 3);
    }

    Y_UNIT_TEST(CalculatesReserveAtUint64Limit) {
        constexpr ui64 maxSlots = std::numeric_limits<ui64>::max();
        const TLiteralRequestsHardReserveSettings settings({
            MakeBackend("percent", 1),
            MakeBackend("full", 100),
            MakeBackend("minimum", 0, maxSlots),
        });
        UNIT_ASSERT_VALUES_EQUAL(settings.Find("percent")->CalculateReserve(maxSlots), 184'467'440'737'095'517ULL);
        UNIT_ASSERT_VALUES_EQUAL(settings.Find("full")->CalculateReserve(maxSlots), maxSlots);
        UNIT_ASSERT_VALUES_EQUAL(settings.Find("minimum")->CalculateReserve(maxSlots), maxSlots);
        UNIT_ASSERT_VALUES_EQUAL(settings.Find("minimum")->CalculateReserve(10), 10);
    }

    Y_UNIT_TEST(RejectsInvalidConfigurations) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TLiteralRequestsHardReserveSettings({MakeBackend("plato", 101)}),
            yexception,
            "must be in range [0, 100]");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TLiteralRequestsHardReserveSettings({MakeBackend("", 1)}),
            yexception,
            "requires a non-empty backend cluster name");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TLiteralRequestsHardReserveSettings({
                MakeBackend("plato", 1),
                MakeBackend("plato"),
            }),
            yexception,
            "multiple YT backends announce the same name");
    }

    Y_UNIT_TEST(CalculatesIndependentClusterReserves) {
        const TLiteralRequestsHardReserveSettings settings({
            MakeBackend("plato-proxy", 1, 3, "plato"),
            MakeBackend("arnold", 100),
        });

        const auto* platoSettings = settings.Find("plato");
        UNIT_ASSERT(platoSettings);
        UNIT_ASSERT_VALUES_EQUAL(platoSettings->CalculateReserve(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(platoSettings->CalculateReserve(2), 2);
        UNIT_ASSERT_VALUES_EQUAL(platoSettings->CalculateReserve(100), 3);
        UNIT_ASSERT_VALUES_EQUAL(platoSettings->CalculateReserve(301), 4);

        const auto* arnoldSettings = settings.Find("arnold");
        UNIT_ASSERT(arnoldSettings);
        UNIT_ASSERT_VALUES_EQUAL(arnoldSettings->CalculateReserve(123), 123);
    }
}

Y_UNIT_TEST_SUITE(TWorkersStorageSlotsSnapshotTest) {
    Y_UNIT_TEST(DisabledSnapshotRemainsEmpty) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        Yql::DqsProto::RegisterNodeRequest request;
        request.SetCapacity(5);
        request.SetClusterName("plato");
        request.AddKnownNodes(1);
        storage.CreateOrUpdate(100, guid, request);

        const auto& snapshot = storage.GetSlotsSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Capacity, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Free, 0);
        UNIT_ASSERT(snapshot.Clusters.empty());
    }

    Y_UNIT_TEST(CachesAndInvalidatesSchedulableSlots) {
        TStorageScenario scenario;
        auto [guid, worker] = scenario.AddWorker("plato", 5);
        auto allocated = scenario.Storage.TryAllocate({MakeClusterRequest({"plato", "plato"}), {}});
        UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 2);

        auto snapshot = scenario.Storage.GetSlotsSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Capacity, 5);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Free, 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Clusters.at("plato").Free, 3);

        scenario.Storage.SetStopping(worker);
        snapshot = scenario.Storage.GetSlotsSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Capacity, 0);
        UNIT_ASSERT(snapshot.Clusters.empty());

        scenario.Storage.SetStopping(worker, false);
        scenario.Storage.FreeWorker(TInstant::Now(), allocated.front());
        snapshot = scenario.Storage.GetSlotsSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Free, 4);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Clusters.at("plato").Free, 4);

        scenario.Storage.DropWorker(TInstant::Now(), guid);
        snapshot = scenario.Storage.GetSlotsSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Capacity, 0);
    }

    Y_UNIT_TEST(CleanUpRemovesExpiredWorkerSlots) {
        TStorageScenario scenario;
        const auto expired = scenario.AddWorker("plato", 5).second;
        const auto live = scenario.AddWorker("arnold", 3).second;
        const auto allocated = scenario.Storage.TryAllocate({MakeClusterRequest({"plato", "plato"}), {}});
        UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 2);

        const auto now = TInstant::Now();
        expired->LastPingTime = now - TDuration::Minutes(1);
        live->LastPingTime = now;
        scenario.Storage.CleanUp(now, TDuration::Seconds(15));

        const auto& snapshot = scenario.Storage.GetSlotsSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Capacity, 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Free, 3);
        UNIT_ASSERT(!snapshot.Clusters.contains("plato"));
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Clusters.at("arnold").Capacity, 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Clusters.at("arnold").Free, 3);
    }

    Y_UNIT_TEST(ClearResetsSnapshotBeforeNewRegistration) {
        TStorageScenario scenario;
        scenario.AddWorker("plato", 5);
        scenario.AddWorker("arnold", 3);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.TryAllocate({MakeClusterRequest({"plato", "plato"}), {}}).size(), 2);

        scenario.Storage.Clear();
        auto snapshot = scenario.Storage.GetSlotsSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Capacity, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Free, 0);
        UNIT_ASSERT(snapshot.Clusters.empty());

        scenario.AddWorker("plato", 2);
        snapshot = scenario.Storage.GetSlotsSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Capacity, 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Free, 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Clusters.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Clusters.at("plato").Free, 2);
    }

    Y_UNIT_TEST(EnforcesClusterBudgetAndRollsItBackOnFailure) {
        TStorageScenario scenario;
        scenario.AddWorker("plato", 3);
        scenario.AddWorker("arnold", 3);
        scenario.AddWorker("unconfigured", 3);

        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 1}, {"arnold", 2}};
        auto failed = scenario.Storage.TryAllocate({MakeClusterRequest({"plato", "plato"}), {}}, &budgets);
        UNIT_ASSERT(failed.empty());
        UNIT_ASSERT_VALUES_EQUAL(budgets.at("plato"), 1);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 9);

        auto mixed = scenario.Storage.TryAllocate({MakeClusterRequest({"plato", "arnold"}), {}}, &budgets);
        AssertClusters(mixed, {"plato", "arnold"});
        UNIT_ASSERT_VALUES_EQUAL(budgets.at("plato"), 0);
        UNIT_ASSERT_VALUES_EQUAL(budgets.at("arnold"), 1);

        auto unconfigured = scenario.Storage.TryAllocate({MakeClusterRequest({"unconfigured", "unconfigured"}, 1), {}}, &budgets);
        AssertClusters(unconfigured, {"unconfigured", "unconfigured"});
        UNIT_ASSERT_VALUES_EQUAL(budgets.at("arnold"), 1);
    }

    Y_UNIT_TEST(ExhaustedClusterDoesNotConsumeLinearSearchLimit) {
        TStorageScenario scenario;
        constexpr ui32 taskCount = 1000;
        for (ui32 i = 0; i <= taskCount; ++i) {
            scenario.AddWorker("plato", 1);
        }
        const auto arnold = scenario.AddWorker(
            "arnold",
            taskCount,
            /*withSharedFile*/ false,
            /*runningWorkerActors*/ 1).second;

        NDqProto::TAllocateWorkersRequest request;
        request.SetCount(taskCount);
        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 0}};
        const auto initialBudgets = budgets;
        const auto allocated = scenario.Storage.TryAllocate({request, {}}, &budgets);

        UNIT_ASSERT_VALUES_EQUAL(allocated.size(), taskCount);
        for (const auto& worker : allocated) {
            UNIT_ASSERT(worker == arnold);
        }
        UNIT_ASSERT(budgets == initialBudgets);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), taskCount + 1);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, taskCount + 1);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Clusters.at("plato").Free, taskCount + 1);

        for (const auto& worker : allocated) {
            scenario.Storage.FreeWorker(TInstant::Now(), worker);
        }
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 2 * taskCount + 1);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 2 * taskCount + 1);
    }

    Y_UNIT_TEST(EnforcesClusterBudgetInIndexedAndBoundedSearch) {
        TStorageScenario indexedScenario;
        indexedScenario.AddWorker("plato", 1, true);
        indexedScenario.AddWorker("arnold", 1, true);

        NDqProto::TAllocateWorkersRequest indexedRequest;
        indexedRequest.SetCount(1);
        indexedRequest.AddWorkerFilterPerTask()->AddFile()->SetObjectId("shared-file");
        TWorkersStorage::TLargeSlotsBudgetByCluster indexedBudgets{{"plato", 0}, {"arnold", 1}};
        auto indexed = indexedScenario.Storage.TryAllocate({indexedRequest, {}}, &indexedBudgets);
        AssertClusters(indexed, {"arnold"});
        UNIT_ASSERT_VALUES_EQUAL(indexedBudgets.at("arnold"), 0);

        TStorageScenario unevenScenario;
        unevenScenario.AddWorker("plato", 2);
        unevenScenario.AddWorker("arnold", 2);

        NDqProto::TAllocateWorkersRequest unevenRequest;
        unevenRequest.SetCount(3);
        unevenRequest.SetWorkersCount(2);
        TWorkersStorage::TLargeSlotsBudgetByCluster unevenBudgets{{"plato", 1}, {"arnold", 2}};
        auto uneven = unevenScenario.Storage.TryAllocate({unevenRequest, {}}, &unevenBudgets);
        UNIT_ASSERT_VALUES_EQUAL(uneven.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(unevenBudgets.at("plato"), 0);
        UNIT_ASSERT_VALUES_EQUAL(unevenBudgets.at("arnold"), 0);

        TStorageScenario legacyScenario;
        legacyScenario.AddWorker("plato", 1);
        legacyScenario.AddWorker("arnold", 2);

        // The legacy bounded selector requires every selected worker to have at least
        // ceil(Count / WorkersCount) slots. Keep this behavior when no hard-reserve
        // budget is supplied, even though the budget-aware selector can use 1 + 2 slots.
        auto legacy = legacyScenario.Storage.TryAllocate({unevenRequest, {}});
        UNIT_ASSERT(legacy.empty());

        TStorageScenario sharedBudgetScenario;
        sharedBudgetScenario.AddWorker("plato", 2);
        sharedBudgetScenario.AddWorker("plato", 2);
        sharedBudgetScenario.AddWorker("arnold", 2);
        TWorkersStorage::TLargeSlotsBudgetByCluster sharedBudgets{{"plato", 3}, {"arnold", 0}};
        auto shared = sharedBudgetScenario.Storage.TryAllocate({unevenRequest, {}}, &sharedBudgets);
        AssertClusters(shared, {"plato", "plato", "plato"});
        UNIT_ASSERT_VALUES_EQUAL(sharedBudgets.at("plato"), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedBudgets.at("arnold"), 0);
    }

    Y_UNIT_TEST(PreservesMandatoryClusterBudgetForFlexibleTask) {
        TStorageScenario scenario;
        const auto plato = scenario.AddWorker("plato", 2).second;
        const auto arnold = scenario.AddWorker(
            "arnold",
            2,
            /*withSharedFile*/ false,
            /*runningWorkerActors*/ 1).second;

        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 1}, {"arnold", 1}};
        const auto initialBudgets = budgets;
        TWorkersStorage::TLargeSlotsBudgetByCluster unavailableBudgets{{"plato", 0}, {"missing", 2}};
        const auto initialUnavailableBudgets = unavailableBudgets;
        for (const auto* clusterName : {"plato", "missing"}) {
            const auto unavailable = scenario.Storage.TryAllocate(
                {MakeClusterRequest({clusterName, clusterName}, 2), {}},
                &unavailableBudgets);

            UNIT_ASSERT(unavailable.empty());
            UNIT_ASSERT(unavailableBudgets == initialUnavailableBudgets);
            UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 4);
            UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 4);
        }

        const auto failed = scenario.Storage.TryAllocate(
            {MakeClusterRequest({"", "plato", "plato"}), {}},
            &budgets);

        UNIT_ASSERT(failed.empty());
        UNIT_ASSERT(budgets == initialBudgets);
        UNIT_ASSERT_VALUES_EQUAL(plato->RunningRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(arnold->RunningRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 4);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 4);

        const auto allocated = scenario.Storage.TryAllocate(
            {MakeClusterRequest({"", "plato"}), {}},
            &budgets);

        AssertClusters(allocated, {"arnold", "plato"});
        const TWorkersStorage::TLargeSlotsBudgetByCluster expectedBudgets{
            {"plato", 0},
            {"arnold", 0},
        };
        UNIT_ASSERT(budgets == expectedBudgets);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 2);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 2);
    }

    Y_UNIT_TEST(IndexedFlexibleTaskPreservesMandatoryClusterBudget) {
        TStorageScenario scenario;
        scenario.AddWorker("plato", 2, /*withSharedFile*/ true);
        scenario.AddWorker("arnold", 2, /*withSharedFile*/ true, /*runningWorkerActors*/ 1);

        for (const auto& mandatoryCluster : {TString("plato"), TString("arnold")}) {
            auto request = MakeClusterRequest({"", mandatoryCluster});
            request.MutableWorkerFilterPerTask(0)->AddFile()->SetObjectId("shared-file");
            TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 1}, {"arnold", 1}};
            const auto allocated = scenario.Storage.TryAllocate({request, {}}, &budgets);

            AssertClusters(allocated, {
                mandatoryCluster == "plato" ? "arnold" : "plato",
                mandatoryCluster,
            });
            const TWorkersStorage::TLargeSlotsBudgetByCluster expectedBudgets{
                {"plato", 0},
                {"arnold", 0},
            };
            UNIT_ASSERT(budgets == expectedBudgets);
            for (const auto& worker : allocated) {
                scenario.Storage.FreeWorker(TInstant::Now(), worker);
            }
        }
    }

    Y_UNIT_TEST(BoundedSelectionIncludesDifferentMandatoryClusters) {
        TStorageScenario scenario;
        scenario.AddWorker("plato", 2);
        scenario.AddWorker("arnold", 2, /*withSharedFile*/ false, /*runningWorkerActors*/ 1);
        scenario.AddWorker("plato", 2, /*withSharedFile*/ false, /*runningWorkerActors*/ 2);

        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"arnold", 1}};
        const auto allocated = scenario.Storage.TryAllocate(
            {MakeClusterRequest({"plato", "plato", "arnold"}, 2), {}},
            &budgets);

        AssertClusters(allocated, {"plato", "plato", "arnold"});
        UNIT_ASSERT(allocated[0] == allocated[1]);
        const TWorkersStorage::TLargeSlotsBudgetByCluster expectedBudgets{{"arnold", 0}};
        UNIT_ASSERT(budgets == expectedBudgets);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 3);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 3);
    }

    Y_UNIT_TEST(BoundedSelectionKeepsMandatoryClusterForFlexibleTasks) {
        TStorageScenario scenario;
        scenario.AddWorker("plato", 2);
        scenario.AddWorker("arnold", 2, /*withSharedFile*/ false, /*runningWorkerActors*/ 1);
        scenario.AddWorker("hahn", 2, /*withSharedFile*/ false, /*runningWorkerActors*/ 2);

        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 1}};
        const auto allocated = scenario.Storage.TryAllocate(
            {MakeClusterRequest({"plato", "", ""}, 2), {}},
            &budgets);

        UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(allocated[0]->ClusterName, "plato");
        UNIT_ASSERT(allocated[1]->ClusterName != "plato");
        UNIT_ASSERT(allocated[1] == allocated[2]);
        const TWorkersStorage::TLargeSlotsBudgetByCluster expectedBudgets{{"plato", 0}};
        UNIT_ASSERT(budgets == expectedBudgets);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 3);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 3);
    }

    Y_UNIT_TEST(BoundedSelectionPreservesUnselectedClusterBudget) {
        TStorageScenario scenario;
        scenario.AddWorker("plato", 1);
        scenario.AddWorker("arnold", 2, /*withSharedFile*/ false, /*runningWorkerActors*/ 1);
        scenario.AddWorker("plato", 1, /*withSharedFile*/ false, /*runningWorkerActors*/ 2);

        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 2}, {"arnold", 2}};
        const auto allocated = scenario.Storage.TryAllocate(
            {MakeClusterRequest({"", "plato", ""}, 2), {}},
            &budgets);

        AssertClusters(allocated, {"arnold", "plato", "arnold"});
        UNIT_ASSERT(allocated[0] == allocated[2]);
        const TWorkersStorage::TLargeSlotsBudgetByCluster expectedBudgets{
            {"plato", 1},
            {"arnold", 0},
        };
        UNIT_ASSERT(budgets == expectedBudgets);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 1);

        const auto unselected = scenario.Storage.TryAllocate(
            {MakeClusterRequest({"plato"}), {}},
            &budgets);

        AssertClusters(unselected, {"plato"});
        UNIT_ASSERT(unselected[0] != allocated[1]);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 0);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 0);
    }

    Y_UNIT_TEST(BudgetedSelectionHandlesMandatoryFilters) {
        TStorageScenario scenario;
        const auto [_, platoTwoSlotWorker] = scenario.AddWorker("plato", 2);
        scenario.AddWorker("plato", 1);
        scenario.AddWorker("arnold", 2);
        scenario.AddWorker("arnold", 2);

        NDqProto::TAllocateWorkersRequest request;
        request.SetCount(3);
        request.SetWorkersCount(2);
        for (ui32 i = 0; i < request.GetCount(); ++i) {
            request.AddWorkerFilterPerTask()->SetClusterName("plato");
        }

        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 3}, {"arnold", 4}};
        const auto allocated = scenario.Storage.TryAllocate({request, {}}, &budgets);

        AssertClusters(allocated, {"plato", "plato", "plato"});
        UNIT_ASSERT_VALUES_EQUAL(budgets.at("plato"), 0);
        UNIT_ASSERT_VALUES_EQUAL(budgets.at("arnold"), 4);

        for (const auto& worker : allocated) {
            scenario.Storage.FreeWorker(TInstant::Now(), worker);
        }
        request.ClearWorkerFilterPerTask();
        request.AddWorkerFilterPerTask()->SetClusterName("plato");
        request.AddWorkerFilterPerTask()->SetClusterName("plato");
        request.AddWorkerFilterPerTask()->SetClusterName("arnold");

        TWorkersStorage::TLargeSlotsBudgetByCluster heterogeneousBudgets{{"plato", 2}, {"arnold", 1}};
        const auto heterogeneous = scenario.Storage.TryAllocate({request, {}}, &heterogeneousBudgets);

        AssertClusters(heterogeneous, {"plato", "plato", "arnold"});
        UNIT_ASSERT_VALUES_EQUAL(heterogeneousBudgets.at("plato"), 0);
        UNIT_ASSERT_VALUES_EQUAL(heterogeneousBudgets.at("arnold"), 0);

        for (const auto& worker : heterogeneous) {
            scenario.Storage.FreeWorker(TInstant::Now(), worker);
        }
        scenario.Storage.SetStopping(platoTwoSlotWorker);
        scenario.AddWorker("plato", 1);

        heterogeneousBudgets = {{"plato", 2}, {"arnold", 1}};
        const auto overBound = scenario.Storage.TryAllocate({request, {}}, &heterogeneousBudgets);

        UNIT_ASSERT(overBound.empty());
        UNIT_ASSERT_VALUES_EQUAL(heterogeneousBudgets.at("plato"), 2);
        UNIT_ASSERT_VALUES_EQUAL(heterogeneousBudgets.at("arnold"), 1);

        TStorageScenario unconfiguredScenario;
        unconfiguredScenario.AddWorker("alpha", 1);
        unconfiguredScenario.AddWorker("beta", 1);
        auto unconfiguredRequest = MakeClusterRequest({"alpha", "beta"}, 1);
        TWorkersStorage::TLargeSlotsBudgetByCluster unrelatedBudget{{"plato", 0}};

        const auto unconfiguredOverBound = unconfiguredScenario.Storage.TryAllocate(
            {unconfiguredRequest, {}},
            &unrelatedBudget);

        UNIT_ASSERT(unconfiguredOverBound.empty());
        UNIT_ASSERT_VALUES_EQUAL(unrelatedBudget.at("plato"), 0);

        TStorageScenario explicitUnconfiguredScenario;
        explicitUnconfiguredScenario.AddWorker("alpha", 2);
        explicitUnconfiguredScenario.AddWorker("beta", 1);
        auto explicitUnconfiguredRequest = MakeClusterRequest({"alpha", "alpha", "beta"}, 2);

        const auto explicitUnconfiguredOverBound = explicitUnconfiguredScenario.Storage.TryAllocate(
            {explicitUnconfiguredRequest, {}},
            &unrelatedBudget);

        // A reserve on an unrelated cluster must not change legacy bounded selection.
        UNIT_ASSERT(explicitUnconfiguredOverBound.empty());
        UNIT_ASSERT_VALUES_EQUAL(unrelatedBudget.at("plato"), 0);
    }

    Y_UNIT_TEST(BoundedSelectionKeepsWorkersForMandatoryNodes) {
        TStorageScenario scenario;
        const auto platoFirst = scenario.AddWorker("plato", 2).second;
        const auto platoSecond = scenario.AddWorker(
            "plato",
            2,
            /*withSharedFile*/ false,
            /*runningWorkerActors*/ 1).second;
        const auto arnold = scenario.AddWorker(
            "arnold",
            2,
            /*withSharedFile*/ false,
            /*runningWorkerActors*/ 2).second;
        scenario.AddWorker("unconfigured", 2, /*withSharedFile*/ false, /*runningWorkerActors*/ 3);

        auto request = MakeClusterRequest({"plato", "plato", "arnold", "arnold"}, 3);
        request.MutableWorkerFilterPerTask(0)->AddNodeId(platoFirst->NodeId);
        request.MutableWorkerFilterPerTask(1)->AddNodeId(platoSecond->NodeId);

        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 1}, {"arnold", 2}};
        const auto insufficientBudgets = budgets;
        const auto failed = scenario.Storage.TryAllocate({request, {}}, &budgets);

        UNIT_ASSERT(failed.empty());
        UNIT_ASSERT(budgets == insufficientBudgets);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 8);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 8);

        budgets["plato"] = 2;
        const auto allocated = scenario.Storage.TryAllocate({request, {}}, &budgets);

        UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 4);
        UNIT_ASSERT(allocated[0] == platoFirst);
        UNIT_ASSERT(allocated[1] == platoSecond);
        UNIT_ASSERT(allocated[2] == arnold);
        UNIT_ASSERT(allocated[3] == arnold);
        const TWorkersStorage::TLargeSlotsBudgetByCluster expectedBudgets{
            {"plato", 0},
            {"arnold", 0},
        };
        UNIT_ASSERT(budgets == expectedBudgets);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 4);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 4);
    }

    Y_UNIT_TEST(BoundedSelectionPreservesLegacyWorkersForFileFilters) {
        TStorageScenario scenario;
        const auto [firstGuid, first] = scenario.AddWorker("plato", 2);
        const auto [secondGuid, second] = scenario.AddWorker("plato", 2);
        scenario.AddFile(firstGuid, first, "first-file");
        scenario.AddFile(secondGuid, second, "second-file");
        const auto arnold = scenario.AddWorker(
            "arnold", 2, /*withSharedFile*/ false, /*runningWorkerActors*/ 1).second;
        scenario.AddWorker("unconfigured", 2, /*withSharedFile*/ false, /*runningWorkerActors*/ 2);

        auto request = MakeClusterRequest({"plato", "plato", "arnold", "arnold"}, 3);
        request.MutableWorkerFilterPerTask(0)->AddFile()->SetObjectId("first-file");
        request.MutableWorkerFilterPerTask(1)->AddFile()->SetObjectId("second-file");
        IScheduler::TWaitInfo waitInfo(request, {});
        waitInfo.ResLeft["previous-attempt"] = 1;
        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 2}, {"arnold", 2}};
        const auto allocated = scenario.Storage.TryAllocate(waitInfo, &budgets);

        UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 4);
        UNIT_ASSERT(allocated[0] == first);
        UNIT_ASSERT(allocated[1] == second);
        UNIT_ASSERT(allocated[2] == arnold);
        UNIT_ASSERT(allocated[3] == arnold);
        const TWorkersStorage::TLargeSlotsBudgetByCluster expectedBudgets{
            {"plato", 0},
            {"arnold", 0},
        };
        UNIT_ASSERT(budgets == expectedBudgets);
        UNIT_ASSERT(waitInfo.ResLeft.empty());
        UNIT_ASSERT(first->GetDownloadList().empty());
        UNIT_ASSERT(second->GetDownloadList().empty());
        UNIT_ASSERT_VALUES_EQUAL(first->UseCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(second->UseCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(arnold->UseCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 4);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 4);

        for (const auto& worker : allocated) {
            scenario.Storage.FreeWorker(TInstant::Now(), worker);
        }
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 8);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 8);
    }

    Y_UNIT_TEST(BoundedSelectionDownloadsFilesToBudgetedWorkersAfterFailure) {
        TStorageScenario scenario;
        const auto plato = scenario.AddWorker("plato", 2).second;
        const auto [arnoldGuid, arnold] = scenario.AddWorker(
            "arnold", 2, /*withSharedFile*/ false, /*runningWorkerActors*/ 1);

        NDqProto::TAllocateWorkersRequest request;
        request.SetCount(2);
        request.SetWorkersCount(1);
        request.AddFiles()->SetObjectId("missing-file");
        IScheduler::TWaitInfo waitInfo(request, {});
        waitInfo.ResLeft["previous-attempt"] = 1;
        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 0}, {"arnold", 2}};
        const auto initialBudgets = budgets;
        const auto failed = scenario.Storage.TryAllocate(waitInfo, &budgets);

        UNIT_ASSERT(failed.empty());
        UNIT_ASSERT(budgets == initialBudgets);
        UNIT_ASSERT(plato->GetDownloadList().empty());
        UNIT_ASSERT(arnold->GetDownloadList().contains(NDqs::TDownloadKey{.ObjectId = "missing-file"}));
        UNIT_ASSERT_VALUES_EQUAL(waitInfo.ResLeft.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(waitInfo.ResLeft.at("missing-file"), 2);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 4);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 4);

        scenario.AddFile(arnoldGuid, arnold, "missing-file");
        const auto allocated = scenario.Storage.TryAllocate(waitInfo, &budgets);
        UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 2);
        UNIT_ASSERT(allocated[0] == arnold);
        UNIT_ASSERT(allocated[1] == arnold);
        UNIT_ASSERT_VALUES_EQUAL(budgets.at("plato"), 0);
        UNIT_ASSERT_VALUES_EQUAL(budgets.at("arnold"), 0);
        UNIT_ASSERT(waitInfo.ResLeft.empty());
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 2);
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 2);
    }

    Y_UNIT_TEST(UnboundedDownloadsSkipExhaustedCluster) {
        TStorageScenario scenario;
        const auto first = scenario.AddWorker("plato", 2).second;
        const auto second = scenario.AddWorker("plato", 2, false, 1).second;
        const auto [guid, usable] = scenario.AddWorker("arnold", 2, false, 2);
        NDqProto::TAllocateWorkersRequest request;
        request.SetCount(2);
        request.AddFiles()->SetObjectId("missing-file");
        IScheduler::TWaitInfo waitInfo(request, {});
        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 0}, {"arnold", 2}};
        const auto initialBudgets = budgets;
        UNIT_ASSERT(scenario.Storage.TryAllocate(waitInfo, &budgets).empty());
        UNIT_ASSERT(budgets == initialBudgets);
        UNIT_ASSERT(first->GetDownloadList().empty());
        UNIT_ASSERT(second->GetDownloadList().empty());
        UNIT_ASSERT(usable->GetDownloadList().contains(NDqs::TDownloadKey{.ObjectId = "missing-file"}));
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 6);
        scenario.AddFile(guid, usable, "missing-file");
        const auto allocated = scenario.Storage.TryAllocate(waitInfo, &budgets);
        UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 2);
        UNIT_ASSERT(allocated[0] == usable && allocated[1] == usable);
        UNIT_ASSERT_VALUES_EQUAL(budgets.at("arnold"), 0);
    }

    Y_UNIT_TEST(DownloadsSearchPastIncompatibleHosts) {
        TStorageScenario scenario;
        const auto wrong = scenario.AddWorker("plato", 2).second;
        const auto [guid, usable] = scenario.AddWorker("arnold", 2, false, 1);
        auto request = MakeClusterRequest({"arnold"});
        request.AddFiles()->SetObjectId("missing-file");
        auto* filter = request.MutableWorkerFilterPerTask(0);
        filter->AddNodeId(usable->NodeId);
        filter->AddFile()->SetObjectId("missing-file");
        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"arnold", 1}};
        UNIT_ASSERT(scenario.Storage.TryAllocate({request, {}}, &budgets).empty());
        UNIT_ASSERT(wrong->GetDownloadList().empty());
        UNIT_ASSERT(usable->GetDownloadList().contains(NDqs::TDownloadKey{.ObjectId = "missing-file"}));
        scenario.AddFile(guid, usable, "missing-file");
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.TryAllocate({request, {}}, &budgets).size(), 1);
        // No eligible host must terminate without changing the budget.
        filter->ClearNodeId();
        filter->AddNodeId(99999);
        budgets = {{"arnold", 1}};
        UNIT_ASSERT(scenario.Storage.TryAllocate({request, {}}, &budgets).empty());
        UNIT_ASSERT_VALUES_EQUAL(budgets.at("arnold"), 1);
    }

    Y_UNIT_TEST(DownloadsRespectBudgetOfPartiallyAllocatedRequest) {
        TStorageScenario scenario;
        const auto first = scenario.AddWorker("plato", 2, true).second;
        const auto [guid, usable] = scenario.AddWorker("arnold", 2, false, 1);
        auto request = MakeClusterRequest({"plato", ""});
        request.MutableWorkerFilterPerTask(0)->AddFile()->SetObjectId("shared-file");
        request.MutableWorkerFilterPerTask(1)->AddFile()->SetObjectId("missing-file");
        request.AddFiles()->SetObjectId("shared-file");
        request.AddFiles()->SetObjectId("missing-file");
        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 1}, {"arnold", 1}};
        const auto initialBudgets = budgets;
        UNIT_ASSERT(scenario.Storage.TryAllocate({request, {}}, &budgets).empty());
        UNIT_ASSERT(budgets == initialBudgets);
        UNIT_ASSERT(first->GetDownloadList().empty());
        UNIT_ASSERT(usable->GetDownloadList().contains(NDqs::TDownloadKey{.ObjectId = "missing-file"}));
        UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 4);
        scenario.AddFile(guid, usable, "missing-file");
        const auto allocated = scenario.Storage.TryAllocate({request, {}}, &budgets);
        UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 2);
        UNIT_ASSERT(allocated[0] == first && allocated[1] == usable);
    }

    Y_UNIT_TEST(BudgetedRetryKeepsIndispensableNodes) {
        for (bool repeatedNode : {false, true}) {
            TStorageScenario scenario;
            scenario.AddWorker("excluded", 2);
            const auto first = scenario.AddWorker("plato", 2, false, 1).second;
            const auto second = scenario.AddWorker("plato", 2, false, 2).second;
            scenario.AddWorker("arnold", 2, false, 3);
            scenario.AddWorker("arnold", 2, false, 4);
            auto request = MakeClusterRequest({"plato", "plato", "", ""}, repeatedNode ? 2 : 3);
            request.MutableWorkerFilterPerTask(0)->AddNodeId(first->NodeId);
            request.MutableWorkerFilterPerTask(1)->AddNodeId(repeatedNode ? first->NodeId : second->NodeId);
            TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"excluded", 0}, {"plato", 2}};
            const auto initialBudgets = budgets;
            const auto allocated = scenario.Storage.TryAllocate({request, {}}, &budgets);
            UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 4);
            UNIT_ASSERT(allocated[0] == first);
            UNIT_ASSERT(allocated[1] == (repeatedNode ? first : second));
            THashSet<NDqs::TWorkerInfo::TPtr> distinct(allocated.begin(), allocated.end());
            UNIT_ASSERT(distinct.size() <= request.GetWorkersCount());
            UNIT_ASSERT_VALUES_EQUAL(budgets.at("plato"), 0);
            UNIT_ASSERT_VALUES_EQUAL(budgets.size(), initialBudgets.size());
            for (const auto& worker : allocated) {
                scenario.Storage.FreeWorker(TInstant::Now(), worker);
            }
            UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 10);
            if (!repeatedNode) {
                request.SetWorkersCount(1);
                budgets = initialBudgets;
                UNIT_ASSERT(scenario.Storage.TryAllocate({request, {}}, &budgets).empty());
                UNIT_ASSERT(budgets == initialBudgets);
                UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 10);
            }
        }
    }

    Y_UNIT_TEST(DownloadsAllowReassignmentAfterRollback) {
        for (bool withBudget : {false, true}) {
            TStorageScenario scenario;
            const auto [guid, pinned] = scenario.AddWorker("plato", 1);
            const auto other = scenario.AddWorker("arnold", 1, false, 1).second;
            auto request = MakeClusterRequest({"", ""});
            request.MutableWorkerFilterPerTask(0)->AddNodeId(pinned->NodeId);
            request.MutableWorkerFilterPerTask(0)->AddFile()->SetObjectId("missing-file");
            request.AddFiles()->SetObjectId("missing-file");
            request.SetUploadPrefix("request-prefix");
            request.SetUploadClusterAddress("upload-cluster");
            TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", 1}, {"arnold", 1}};
            const auto initialBudgets = budgets;
            auto* budget = withBudget ? &budgets : nullptr;
            UNIT_ASSERT(scenario.Storage.TryAllocate({request, {}}, budget).empty());
            const NDqs::TDownloadKey key{
                .ObjectId = "missing-file",
                .UploadPrefix = "request-prefix",
                .UploadClusterAddress = "upload-cluster",
            };
            UNIT_ASSERT(pinned->GetDownloadList().contains(key));
            UNIT_ASSERT(other->GetDownloadList().empty());
            UNIT_ASSERT(budgets == initialBudgets);
            UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 2);
            scenario.AddFile(guid, pinned, "missing-file");
            const auto allocated = scenario.Storage.TryAllocate({request, {}}, budget);
            UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 2);
            UNIT_ASSERT(allocated[0] == pinned && allocated[1] == other);
        }
    }

    Y_UNIT_TEST(DownloadsRespectDifferentTaskHosts) {
        TStorageScenario scenario;
        const auto wrong = scenario.AddWorker("excluded", 1).second;
        const auto [firstGuid, first] = scenario.AddWorker("plato", 1, false, 1);
        const auto [secondGuid, second] = scenario.AddWorker("arnold", 1, false, 2);
        auto request = MakeClusterRequest({"plato", "arnold"});
        request.MutableWorkerFilterPerTask(0)->AddNodeId(first->NodeId);
        request.MutableWorkerFilterPerTask(0)->AddFile()->SetObjectId("first-file");
        request.MutableWorkerFilterPerTask(1)->AddNodeId(second->NodeId);
        request.MutableWorkerFilterPerTask(1)->AddFile()->SetObjectId("second-file");
        request.AddFiles()->SetObjectId("first-file");
        request.AddFiles()->SetObjectId("second-file");
        TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"excluded", 0}, {"plato", 1}, {"arnold", 1}};
        const auto initialBudgets = budgets;
        UNIT_ASSERT(scenario.Storage.TryAllocate({request, {}}, &budgets).empty());
        UNIT_ASSERT(budgets == initialBudgets);
        UNIT_ASSERT(wrong->GetDownloadList().empty());
        UNIT_ASSERT_VALUES_EQUAL(first->GetDownloadList().size(), 1);
        UNIT_ASSERT(first->GetDownloadList().contains(NDqs::TDownloadKey{.ObjectId = "first-file"}));
        UNIT_ASSERT_VALUES_EQUAL(second->GetDownloadList().size(), 1);
        UNIT_ASSERT(second->GetDownloadList().contains(NDqs::TDownloadKey{.ObjectId = "second-file"}));
        scenario.AddFile(firstGuid, first, "first-file");
        scenario.AddFile(secondGuid, second, "second-file");
        const auto allocated = scenario.Storage.TryAllocate({request, {}}, &budgets);
        UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 2);
        UNIT_ASSERT(allocated[0] == first && allocated[1] == second);
    }

    Y_UNIT_TEST(BudgetedBoundedSelectionUsesFullWorkersCount) {
        for (ui64 initialBudget : {5, 8}) {
            TStorageScenario scenario;
            TVector<std::pair<TGUID, NDqs::TWorkerInfo::TPtr>> workers;
            for (ui32 i = 0; i < 4; ++i) {
                workers.push_back(scenario.AddWorker("plato", 2));
            }
            std::sort(workers.begin(), workers.end(), [] (const auto& lhs, const auto& rhs) {
                return NDqs::TWorkerInfoPtrComparator{}(lhs.second, rhs.second);
            });
            scenario.AddFile(workers.back().first, workers.back().second, "unique-file");

            NDqProto::TAllocateWorkersRequest request;
            request.SetCount(5);
            request.SetWorkersCount(4);
            request.AddWorkerFilterPerTask()->AddFile()->SetObjectId("unique-file");
            for (ui32 i = 1; i < request.GetCount(); ++i) {
                request.AddWorkerFilterPerTask();
            }

            TWorkersStorage::TLargeSlotsBudgetByCluster budgets{{"plato", initialBudget}};
            const auto allocated = scenario.Storage.TryAllocate({request, {}}, &budgets);

            UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 5);
            UNIT_ASSERT(allocated[0] == workers.back().second);
            const TWorkersStorage::TLargeSlotsBudgetByCluster expectedBudgets{
                {"plato", initialBudget - request.GetCount()},
            };
            UNIT_ASSERT(budgets == expectedBudgets);
            UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.FreeSlots(), 3);
            UNIT_ASSERT_VALUES_EQUAL(scenario.Storage.GetSlotsSnapshot().Free, 3);
        }
    }
}

Y_UNIT_TEST_SUITE(WorkersBenchmark) {
    Y_UNIT_TEST(RequestUploadLocationsSurviveDownloadReassignment) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        Yql::DqsProto::RegisterNodeRequest workerRequest;
        workerRequest.SetCapacity(1);
        workerRequest.AddKnownNodes(1);
        TGUID workerId;
        CreateGuid(&workerId);
        auto [worker, _] = storage.CreateOrUpdate(100, workerId, workerRequest);

        const TVector<std::pair<TString, TString>> locations = {
            {"cluster-one.yt", "//client"},
            {"cluster-two.yt", "//client"},
            {"", "//client"},
            {"cluster-one.yt", ""},
            {"", ""},
        };
        THashSet<NDqs::TDownloadKey, NDqs::TDownloadKeyHash> expected;
        for (bool perTask : {false, true}) {
            for (const auto& location : locations) {
                NDqProto::TAllocateWorkersRequest request;
                request.SetCount(1);
                request.SetUploadClusterAddress(location.first);
                request.SetUploadPrefix(location.second);
                auto* filter = perTask ? request.AddWorkerFilterPerTask() : nullptr;
                for (int i = 0; i < 2; ++i) {
                    auto* file = filter ? filter->AddFile() : request.AddFiles();
                    file->SetObjectId("resource-" + ToString(i));
                    file->SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
                    expected.insert({
                        .ObjectId = file->GetObjectId(),
                        .UploadPrefix = location.second,
                        .UploadClusterAddress = location.first,
                    });
                }
                IScheduler::TWaitInfo waitInfo(request, NActors::TActorId());
                UNIT_ASSERT(storage.TryAllocate(waitInfo).empty());
            }
        }

        auto checkDownloads = [&] (const NDqs::TWorkerInfo::TPtr& node) {
            const auto downloads = node->GetResourcesForDownloading();
            UNIT_ASSERT_VALUES_EQUAL(downloads.size(), expected.size());
            for (const auto& key : expected) {
                UNIT_ASSERT_VALUES_EQUAL(downloads.at(key).UploadClusterAddress, key.UploadClusterAddress);
                UNIT_ASSERT_VALUES_EQUAL(downloads.at(key).UploadPrefix, key.UploadPrefix);
                UNIT_ASSERT_VALUES_EQUAL(downloads.at(key).File.GetObjectId(), key.ObjectId);
            }
        };
        checkDownloads(worker);

        TGUID replacementId;
        CreateGuid(&replacementId);
        auto [replacement, unused] = storage.CreateOrUpdate(101, replacementId, workerRequest);
        Y_UNUSED(unused);
        worker->LastPingTime = TInstant::Now() - TDuration::Minutes(1);
        storage.CleanUp(TInstant::Now(), TDuration::Seconds(15));
        checkDownloads(replacement);
    }

    Y_UNIT_TEST(AlternativeSourcesHaveIndependentLimitsAndFinishTogether) {
        using namespace NDqs;
        auto metrics = MakeIntrusive<TSensorsGroup>();
        TGlobalResources resources(metrics);
        auto limiter = MakeIntrusive<TInflightLimiter>(1);
        Yql::DqsProto::RegisterNodeRequest request;
        request.SetCapacity(1);
        request.AddKnownNodes(1);
        TGUID workerId;
        CreateGuid(&workerId);
        auto worker = MakeIntrusive<TWorkerInfo>("start", 100u, workerId, request, metrics, limiter, resources);
        TGUID otherId;
        CreateGuid(&otherId);
        auto other = MakeIntrusive<TWorkerInfo>("start", 101u, otherId, request, metrics, limiter, resources);

        TWorkerInfo::TDownloadResource first{
            .UploadPrefix = "//A",
            .UploadClusterAddress = "cluster-one.yt",
        };
        first.File.SetObjectId("shared-file");
        UNIT_ASSERT(worker->AddToDownloadList(first));
        UNIT_ASSERT_VALUES_EQUAL(worker->GetResourcesForDownloading().size(), 1);
        UNIT_ASSERT(other->AddToDownloadList(first));
        UNIT_ASSERT(other->GetResourcesForDownloading().empty());

        auto second = first;
        second.UploadPrefix = "//B";
        auto third = second;
        third.UploadClusterAddress = "cluster-two.yt";
        UNIT_ASSERT(worker->AddToDownloadList(second));
        UNIT_ASSERT(worker->AddToDownloadList(third));
        UNIT_ASSERT(!worker->AddToDownloadList(first));
        UNIT_ASSERT_VALUES_EQUAL(worker->GetResourcesForDownloading().size(), 3);

        // A cached object clears pending sources without releasing another worker's slot.
        request.AddFilesOnNode()->SetObjectId("shared-file");
        other->Update(request);
        UNIT_ASSERT(other->GetDownloadList().empty());
        UNIT_ASSERT(!limiter->CanDownload(first.GetKey()));

        worker->Update(request);
        UNIT_ASSERT(worker->GetDownloadList().empty());
        UNIT_ASSERT(worker->GetActiveDownloads().empty());
        for (const auto& source : {first, second, third}) {
            UNIT_ASSERT(limiter->CanDownload(source.GetKey()));
            UNIT_ASSERT(!worker->AddToDownloadList(source));
        }
    }

    Y_UNIT_TEST(RssIsReportedInClusterStatus) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        Yql::DqsProto::RegisterNodeRequest request;
        request.SetCapacity(1);
        request.AddKnownNodes(1);
        request.MutableRusage()->SetMaxRss(123456789);
        request.MutableRusage()->SetCurrentRss(98765432);
        storage.CreateOrUpdate(100, guid, request);

        Yql::DqsProto::ClusterStatusResponse response;
        storage.ClusterStatus(&response);

        UNIT_ASSERT_VALUES_EQUAL(response.WorkerSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetWorker(0).GetRusage().GetMaxRss(), 123456789);
        UNIT_ASSERT_VALUES_EQUAL(response.GetWorker(0).GetRusage().GetCurrentRss(), 98765432);
    }

    Y_UNIT_TEST(Basic) {
        int workers = 1000;
        int i;
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();
        for (i = 0; i < workers; i++) {
            TGUID guid;
            Yql::DqsProto::RegisterNodeRequest request;
            request.SetCapacity(100);
            request.AddKnownNodes(1);
            CreateGuid(&guid);
            storage.CreateOrUpdate(i+100, guid, request);
        }

        UNIT_ASSERT_VALUES_EQUAL(workers*100, storage.FreeSlots());

        TVector<NDqs::TWorkerInfo::TPtr> all;
        TInstant now = TInstant::Now();
        while (1) {
            NYql::NDqProto::TAllocateWorkersRequest request;
            request.SetCount(10);
            IScheduler::TWaitInfo waitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo);
            if (result.empty()) {
                break;
            }
            all.insert(all.end(), result.begin(), result.end());
        }

        Cerr << (TInstant::Now() - now) << Endl;

        UNIT_ASSERT_VALUES_EQUAL(0, storage.FreeSlots());


        now = TInstant::Now();
        for (auto& node : all) {
            storage.FreeWorker(now, node);
        }
        Cerr << (TInstant::Now() - now) << Endl;

        UNIT_ASSERT_VALUES_EQUAL(workers*100, storage.FreeSlots());
    }

    Y_UNIT_TEST(AcquireAll) {
        int workers = 1;
        int i;
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();
        for (i = 0; i < workers; i++) {
            TGUID guid;
            Yql::DqsProto::RegisterNodeRequest request;
            request.SetCapacity(100);
            request.AddKnownNodes(1);
            CreateGuid(&guid);
            storage.CreateOrUpdate(i+100, guid, request);
        }

        UNIT_ASSERT_VALUES_EQUAL(workers*100, storage.FreeSlots());

        TVector<NDqs::TWorkerInfo::TPtr> all;
        while (1) {
            NYql::NDqProto::TAllocateWorkersRequest request;
            request.SetCount(10);
            IScheduler::TWaitInfo waitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo);
            if (result.empty()) {
                break;
            }
            all.insert(all.end(), result.begin(), result.end());
        }
        UNIT_ASSERT_VALUES_EQUAL(all.size(), 100);
        UNIT_ASSERT_VALUES_EQUAL(0, storage.FreeSlots());
    }

    // Bounded mode must distribute tasks evenly across exactly workersCount workers.
    // Regresses the bug where tasksPerWorker was passed by value to ProcessMatched,
    // causing the per-worker cap to never trigger: all tasks landed on the first worker.
    Y_UNIT_TEST(BoundedModeDistribution) {
        const int Workers = 5;
        const int TasksPerWorker = 10;
        const int TotalTasks = Workers * TasksPerWorker;

        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        for (int i = 0; i < Workers; i++) {
            TGUID guid;
            Yql::DqsProto::RegisterNodeRequest req;
            req.SetCapacity(1000); // high capacity — old bug: first worker would absorb all tasks
            req.AddKnownNodes(1);
            CreateGuid(&guid);
            storage.CreateOrUpdate(100 + i, guid, req);
        }

        NYql::NDqProto::TAllocateWorkersRequest allocReq;
        allocReq.SetCount(TotalTasks);
        allocReq.SetWorkersCount(Workers);
        IScheduler::TWaitInfo waitInfo(allocReq, NActors::TActorId());
        auto result = storage.TryAllocate(waitInfo);

        UNIT_ASSERT_VALUES_EQUAL(result.size(), (size_t)TotalTasks);

        THashMap<TGUID, int> tasksPerWorker;
        for (const auto& w : result) {
            tasksPerWorker[w->WorkerId]++;
        }

        UNIT_ASSERT_VALUES_EQUAL_C(
            tasksPerWorker.size(), (size_t)Workers,
            "All workers must be used in bounded mode");
        for (const auto& [_, count] : tasksPerWorker) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                count, TasksPerWorker,
                "Each worker must receive exactly TasksPerWorker tasks");
        }
    }

    Y_UNIT_TEST(ScheduleDownload) {
        int workers = 10;
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();
        for (int i = 0; i < workers; i++) {
            TGUID guid;
            Yql::DqsProto::RegisterNodeRequest request;
            request.SetCapacity(100);
            request.AddKnownNodes(1);
            CreateGuid(&guid);
            storage.CreateOrUpdate(100+i, guid, request);
        }

        {
            auto request = NDqProto::TAllocateWorkersRequest();
            request.SetCount(10);

            auto waitInfo1 = IScheduler::TWaitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo1);

            UNIT_ASSERT_VALUES_EQUAL(result.size(), 10);
        }

        {
            auto request = NDqProto::TAllocateWorkersRequest();
            auto workerFilter = Yql::DqsProto::TWorkerFilter();
            workerFilter.AddNodeId(102);

            request.SetCount(10);
            for (ui32 i = 0; i < request.GetCount(); i++) {
                *request.AddWorkerFilterPerTask() = workerFilter;
            }
            auto waitInfo2 = IScheduler::TWaitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo2);
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 10);
        }

        {
            auto request = NDqProto::TAllocateWorkersRequest();
            auto workerFilter = Yql::DqsProto::TWorkerFilter();
            workerFilter.AddNodeId(102);
            Yql::DqsProto::TFile file;
            file.SetObjectId("fileId");
            file.SetLocalPath("/tmp/test");
            *workerFilter.AddFile() = file;
            request.SetCount(10);
            for (ui32 i = 0; i < request.GetCount(); i++) {
                *request.AddWorkerFilterPerTask() = workerFilter;
            }

            auto waitInfo3 = IScheduler::TWaitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo3);
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 0);

            storage.Visit([](const NDqs::TWorkerInfo::TPtr& workerInfo) {
                UNIT_ASSERT(workerInfo->GetDownloadList().size() == 0 || workerInfo->NodeId == 102);
            });
        }
    }
}

Y_UNIT_TEST_SUITE(TWorkersStorageCheckZombieTest) {
    Y_UNIT_TEST(NewerWorkerMarksOldWorkerDead) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup, /*trackSlotsSnapshot=*/true);
        storage.Clear();

        const ui32 nodeId = 100;
        TGUID oldWorkerId;
        TGUID newWorkerId;
        CreateGuid(&oldWorkerId);
        CreateGuid(&newWorkerId);

        Yql::DqsProto::RegisterNodeRequest oldRequest;
        oldRequest.SetCapacity(5);
        oldRequest.SetClusterName("plato");
        oldRequest.AddKnownNodes(1);
        oldRequest.SetStartTime("2020-01-01T00:00:00.000000Z");
        storage.CreateOrUpdate(nodeId, oldWorkerId, oldRequest);
        const auto allocated = storage.TryAllocate({MakeClusterRequest({"plato", "plato"}), {}});
        UNIT_ASSERT_VALUES_EQUAL(allocated.size(), 2);

        Yql::DqsProto::RegisterNodeRequest newRequest;
        newRequest.SetCapacity(3);
        newRequest.SetClusterName("plato");
        newRequest.AddKnownNodes(1);
        newRequest.SetStartTime("2021-01-01T00:00:00.000000Z");
        storage.CreateOrUpdate(nodeId, newWorkerId, newRequest);

        bool oldWorkerDead = false;
        bool newWorkerAlive = false;
        storage.Visit([&](const NDqs::TWorkerInfo::TPtr& workerInfo) {
            if (workerInfo->WorkerId == oldWorkerId) {
                oldWorkerDead = workerInfo->IsDead;
            }
            if (workerInfo->WorkerId == newWorkerId) {
                newWorkerAlive = !workerInfo->IsDead;
            }
        });

        UNIT_ASSERT(oldWorkerDead);
        UNIT_ASSERT(newWorkerAlive);

        const auto& snapshot = storage.GetSlotsSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Capacity, 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Free, 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Clusters.at("plato").Capacity, 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Clusters.at("plato").Free, 3);
    }
}

Y_UNIT_TEST_SUITE(FileIndex) {
    // Build a 1-task allocation request requiring the given file IDs.
    static IScheduler::TWaitInfo MakeFileWaitInfo(int taskCount, TVector<TString> fileIds) {
        NYql::NDqProto::TAllocateWorkersRequest req;
        req.SetCount(taskCount);
        Yql::DqsProto::TWorkerFilter filter;
        for (const auto& id : fileIds) {
            auto* f = filter.AddFile();
            f->SetObjectId(id);
            f->SetLocalPath("/tmp/" + id);
        }
        for (int i = 0; i < taskCount; i++) {
            *req.AddWorkerFilterPerTask() = filter;
        }
        return IScheduler::TWaitInfo(req, NActors::TActorId());
    }

    // Build a RegisterNodeRequest reporting the given files as present on the node.
    static Yql::DqsProto::RegisterNodeRequest MakeWorkerReq(TVector<TString> fileIds, int capacity = 100) {
        Yql::DqsProto::RegisterNodeRequest req;
        req.SetCapacity(capacity);
        req.AddKnownNodes(1);
        for (const auto& id : fileIds) {
            req.AddFilesOnNode()->SetObjectId(id);
        }
        return req;
    }

    // CreateOrUpdate requires a non-const lvalue reference; this wrapper avoids temporaries.
    static void AddWorker(TWorkersStorage& storage, ui32 nodeId, TGUID guid,
                          TVector<TString> fileIds, int capacity = 100) {
        auto req = MakeWorkerReq(std::move(fileIds), capacity);
        storage.CreateOrUpdate(nodeId, guid, req);
    }

    // Index is populated when a worker is created with files.
    Y_UNIT_TEST(PopulatedOnCreation) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        AddWorker(storage, 101, guid, {"f1", "f2"});

        auto result = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f2"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result.size(), 1u, "worker with f1+f2 must be found via index");
    }

    // Index is updated when a worker gains new files (download completed).
    Y_UNIT_TEST(UpdatedOnFileAddition) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        // Worker starts with no files.
        AddWorker(storage, 101, guid, {});

        auto result1 = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result1.size(), 0u, "no worker has f1 yet");

        // Worker reports f1 on next ping.
        AddWorker(storage, 101, guid, {"f1"});

        auto result2 = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result2.size(), 1u, "worker now has f1, must be found");
    }

    // Index is updated when a worker loses files (eviction/restart).
    Y_UNIT_TEST(UpdatedOnFileRemoval) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        AddWorker(storage, 101, guid, {"f1", "f2"});

        // Initial allocation for f1+f2 works.
        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f2"}));
            UNIT_ASSERT_VALUES_EQUAL(r.size(), 1u);
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        // Worker loses f2 on next ping.
        AddWorker(storage, 101, guid, {"f1"});

        // f1+f2 must no longer be satisfiable.
        auto result2 = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f2"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result2.size(), 0u, "f2 removed from worker, must not allocate");

        // f1 alone must still work.
        auto result3 = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result3.size(), 1u, "f1 still present, must allocate");
    }

    // Index is cleaned up when a worker is explicitly dropped.
    Y_UNIT_TEST(ClearedOnDropWorker) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        AddWorker(storage, 101, guid, {"f1"});

        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
            UNIT_ASSERT_VALUES_EQUAL(r.size(), 1u);
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        storage.DropWorker(TInstant::Now(), guid);

        auto result = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result.size(), 0u, "worker dropped, index entry must be gone");
    }

    // Index is cleaned up when a worker expires during CleanUp.
    Y_UNIT_TEST(ClearedOnCleanUp) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        AddWorker(storage, 101, guid, {"f1"});

        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
            UNIT_ASSERT_VALUES_EQUAL(r.size(), 1u);
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        // Expire the worker: pass a far-future "now" with zero tolerance.
        storage.CleanUp(TInstant::Now() + TDuration::Days(1), TDuration::Zero());

        auto result = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result.size(), 0u, "worker expired, index entry must be gone");
    }

    // Index is fully reset by Clear(), with no stale entries for new workers.
    Y_UNIT_TEST(ResetOnClear) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);

        // First population: worker with f1.
        {
            TGUID guid;
            CreateGuid(&guid);
            AddWorker(storage, 101, guid, {"f1"});
        }

        storage.Clear();

        // After Clear(), add a new worker without f1.
        {
            TGUID guid;
            CreateGuid(&guid);
            AddWorker(storage, 101, guid, {});
        }

        auto result = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result.size(), 0u, "stale index entry must be gone after Clear()");
    }

    // Index intersection selects only the worker that has ALL required files.
    Y_UNIT_TEST(IntersectionSelectsCorrectWorker) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID gA, gB, gC;
        CreateGuid(&gA); CreateGuid(&gB); CreateGuid(&gC);

        AddWorker(storage, 101, gA, {"f1", "f2"});       // A: f1, f2
        AddWorker(storage, 102, gB, {"f1", "f3"});       // B: f1, f3
        AddWorker(storage, 103, gC, {"f1", "f2", "f3"}); // C: f1, f2, f3

        // f1+f2: A or C only (not B — B lacks f2).
        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f2"}));
            UNIT_ASSERT_VALUES_EQUAL_C(r.size(), 1u, "f1+f2 must match exactly one worker");
            UNIT_ASSERT_C(r[0]->WorkerId == gA || r[0]->WorkerId == gC,
                          "only A or C have both f1 and f2");
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        // f1+f3: B or C only (not A — A lacks f3).
        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f3"}));
            UNIT_ASSERT_VALUES_EQUAL_C(r.size(), 1u, "f1+f3 must match exactly one worker");
            UNIT_ASSERT_C(r[0]->WorkerId == gB || r[0]->WorkerId == gC,
                          "only B or C have both f1 and f3");
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        // f1+f2+f3: only C has all three.
        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f2", "f3"}));
            UNIT_ASSERT_VALUES_EQUAL_C(r.size(), 1u, "f1+f2+f3 must match only C");
            UNIT_ASSERT_C(r[0]->WorkerId == gC, "only C has f1+f2+f3");
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        // Allocate 3 tasks requiring only f1: all three workers qualify.
        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(3, {"f1"}));
            UNIT_ASSERT_VALUES_EQUAL_C(r.size(), 3u, "all three workers have f1");
        }
    }

    // Index handles multiple workers sharing the same file correctly:
    // removing one worker must not evict others from the index entry.
    Y_UNIT_TEST(DropOneWorkerKeepsOthers) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID g1, g2;
        CreateGuid(&g1); CreateGuid(&g2);
        AddWorker(storage, 101, g1, {"shared"});
        AddWorker(storage, 102, g2, {"shared"});

        storage.DropWorker(TInstant::Now(), g1);

        // g2 still has the file and must be allocatable.
        auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"shared"}));
        UNIT_ASSERT_VALUES_EQUAL_C(r.size(), 1u, "g2 still has 'shared', must be found");
        UNIT_ASSERT_C(r[0]->WorkerId == g2, "allocated worker must be g2");
    }
}

// Validate that TWorkerFilter::Match populates WaitingResources with the
// correct "task is waiting for a missing resource" semantics:
//   - worker LACKS the file  → taskId inserted (task is waiting)
//   - worker HAS   the file  → taskId erased   (task is no longer waiting)
// This regresses the bug where insert/erase were inverted, causing
// ResourceWaitTime stats to measure the opposite of waiting time.
Y_UNIT_TEST_SUITE(TWorkerFilterWaitingResources) {
    // Build a RegisterNodeRequest reporting the given files as present on the node.
    static Yql::DqsProto::RegisterNodeRequest MakeWorkerReq(TVector<TString> fileIds, int capacity = 100) {
        Yql::DqsProto::RegisterNodeRequest req;
        req.SetCapacity(capacity);
        req.AddKnownNodes(1);
        for (const auto& id : fileIds) {
            req.AddFilesOnNode()->SetObjectId(id);
        }
        return req;
    }

    // Retrieve the single worker from storage via Visit.
    static NDqs::TWorkerInfo::TPtr GetSingleWorker(TWorkersStorage& storage) {
        NDqs::TWorkerInfo::TPtr result;
        storage.Visit([&](const NDqs::TWorkerInfo::TPtr& w) {
            if (!w->IsDead) {
                result = w;
            }
        });
        return result;
    }

    // Build a TWorkerFilter requiring the given file IDs.
    static TWorkerFilter MakeFilter(TVector<TString> fileIds) {
        Yql::DqsProto::TWorkerFilter proto;
        for (const auto& id : fileIds) {
            auto* f = proto.AddFile();
            f->SetObjectId(id);
            f->SetLocalPath("/tmp/" + id);
        }
        return TWorkerFilter(proto);
    }

    // Worker lacks the required file → taskId must be in WaitingResources.
    Y_UNIT_TEST(MissingFileInsertsTaskIntoWaitingResources) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        auto req = MakeWorkerReq({}); // worker has NO files
        storage.CreateOrUpdate(101, guid, req);

        auto worker = GetSingleWorker(storage);
        UNIT_ASSERT_C(worker, "worker must exist");

        auto filter = MakeFilter({"missing_file"});

        THashMap<TString, THashSet<int>> waitingResources;
        THashMap<TString, TResourceStat> uploaded;
        TWorkerFilter::TStats stats;
        stats.WaitingResources = &waitingResources;
        stats.Uploaded = &uploaded;

        const int taskId = 42;
        auto status = filter.Match(worker, taskId, &stats);

        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(status), static_cast<int>(TWorkerFilter::EFAIL),
            "worker lacks the file, Match must return EFAIL");
        UNIT_ASSERT_C(
            waitingResources["missing_file"].contains(taskId),
            "taskId must be in WaitingResources when worker LACKS the file (task is waiting)");
    }

    // Worker has the required file → taskId must NOT be in WaitingResources.
    Y_UNIT_TEST(PresentFileErasesTaskFromWaitingResources) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        auto req = MakeWorkerReq({"present_file"}); // worker HAS the file
        storage.CreateOrUpdate(101, guid, req);

        auto worker = GetSingleWorker(storage);
        UNIT_ASSERT_C(worker, "worker must exist");

        auto filter = MakeFilter({"present_file"});

        THashMap<TString, THashSet<int>> waitingResources;
        THashMap<TString, TResourceStat> uploaded;
        TWorkerFilter::TStats stats;
        stats.WaitingResources = &waitingResources;
        stats.Uploaded = &uploaded;

        const int taskId = 42;
        // Pre-insert taskId to simulate a previous "waiting" state.
        waitingResources["present_file"].insert(taskId);

        auto status = filter.Match(worker, taskId, &stats);

        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(status), static_cast<int>(TWorkerFilter::EOK),
            "worker has the file, Match must return EOK");
        UNIT_ASSERT_C(
            !waitingResources["present_file"].contains(taskId),
            "taskId must NOT be in WaitingResources when worker HAS the file (task is no longer waiting)");
    }

    // Mixed: worker has one file but lacks another.
    // The missing file must have taskId in WaitingResources; the present one must not.
    Y_UNIT_TEST(MixedFilesWaitingResources) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        auto req = MakeWorkerReq({"have_it"}); // worker has "have_it" but not "need_it"
        storage.CreateOrUpdate(101, guid, req);

        auto worker = GetSingleWorker(storage);
        UNIT_ASSERT_C(worker, "worker must exist");

        auto filter = MakeFilter({"have_it", "need_it"});

        THashMap<TString, THashSet<int>> waitingResources;
        THashMap<TString, TResourceStat> uploaded;
        TWorkerFilter::TStats stats;
        stats.WaitingResources = &waitingResources;
        stats.Uploaded = &uploaded;

        const int taskId = 7;
        // Pre-insert taskId for "have_it" to verify it gets erased.
        waitingResources["have_it"].insert(taskId);

        auto status = filter.Match(worker, taskId, &stats);

        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(status), static_cast<int>(TWorkerFilter::EFAIL),
            "worker lacks 'need_it', Match must return EFAIL");
        UNIT_ASSERT_C(
            !waitingResources["have_it"].contains(taskId),
            "taskId must NOT be in WaitingResources for 'have_it' (worker has it)");
        UNIT_ASSERT_C(
            waitingResources["need_it"].contains(taskId),
            "taskId must be in WaitingResources for 'need_it' (worker lacks it)");
    }
}
