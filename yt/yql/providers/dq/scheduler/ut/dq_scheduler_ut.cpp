#include "../dq_scheduler.h"

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>

using namespace NYql::NDq;

namespace {
NYql::NDqProto::TAllocateWorkersRequest MakeRequest(ui32 count, const TString& user) {
    NYql::NDqProto::TAllocateWorkersRequest request;
    request.SetCount(count);
    request.SetUser(user);
    return request;
}

}

Y_UNIT_TEST_SUITE(TSchedulerTest) {
    Y_UNIT_TEST(SimpleFifo) {
        const auto scheduler = IScheduler::Make();
        UNIT_ASSERT(scheduler);

        scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        scheduler->Suspend({MakeRequest(7U, "user2"), {}});
        scheduler->Suspend({MakeRequest(5U, "user3"), {}});

        size_t workers = 30U;
        std::vector<size_t> counts;
        const auto processor = [&](const IScheduler::TWaitInfo& wait) {
            counts.emplace_back(wait.Request.GetCount());
            return true;
        };

        scheduler->Process(workers, workers, processor);

        const std::vector<size_t> expected = {3U, 7U, 5U};
        UNIT_ASSERT_VALUES_EQUAL(counts, expected);
    }

    Y_UNIT_TEST(ReserveForSmall) {
        const auto scheduler = IScheduler::Make();
        UNIT_ASSERT(scheduler);

        scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        scheduler->Suspend({MakeRequest(7U, "user2"), {}});
        scheduler->Suspend({MakeRequest(5U, "user3"), {}});

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 3U);

        size_t workers = 5U;
        std::vector<size_t> counts;
        const auto processor = [&](const IScheduler::TWaitInfo& wait) {
            counts.emplace_back(wait.Request.GetCount());
            return true;
        };

        scheduler->Process(15U, workers, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 2U);

        scheduler->Process(15U, workers = 8U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);

        scheduler->Process(15U, workers = 8U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);

        const std::vector<size_t> expected = {3U, 5U};
        UNIT_ASSERT_VALUES_EQUAL(counts, expected);
    }

    Y_UNIT_TEST(OneUserForCluster) {
        NYql::NProto::TDqConfig::TScheduler cfg;
        cfg.SetHistoryKeepingTime(1);
        cfg.SetLimitTasksPerWindow(true);

        const auto scheduler = IScheduler::Make(cfg);
        UNIT_ASSERT(scheduler);

        scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        scheduler->Suspend({MakeRequest(3U, "user1"), {}});

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 2U);

        std::vector<size_t> counts;
        const auto processor = [&](const IScheduler::TWaitInfo& wait) {
            counts.emplace_back(wait.Request.GetCount());
            return true;
        };

        scheduler->Process(11U, 7U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);

        const std::vector<size_t> expected = {3U};
        UNIT_ASSERT_VALUES_EQUAL(counts, expected);
    }

    Y_UNIT_TEST(DoNotReserveForSmall) {
        NYql::NProto::TDqConfig::TScheduler cfg;
        cfg.SetKeepReserveForLiteralRequests(false);

        const auto scheduler = IScheduler::Make(cfg);
        UNIT_ASSERT(scheduler);

        scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        scheduler->Suspend({MakeRequest(7U, "user2"), {}});
        scheduler->Suspend({MakeRequest(5U, "user3"), {}});

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 3U);

        size_t workers = 5U;
        std::vector<size_t> counts;
        const auto processor = [&](const IScheduler::TWaitInfo& wait) {
            counts.emplace_back(wait.Request.GetCount());
            return true;
        };

        scheduler->Process(30U, workers, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 2U);

        scheduler->Process(30U, workers = 8U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);

        scheduler->Process(30U, workers = 8U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);

        const std::vector<size_t> expected = {3U, 7U, 5U};
        UNIT_ASSERT_VALUES_EQUAL(counts, expected);
    }

    Y_UNIT_TEST(NewbieFirst) {
        const auto scheduler = IScheduler::Make();
        UNIT_ASSERT(scheduler);

        scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        scheduler->Suspend({MakeRequest(7U, "user1"), {}});
        scheduler->Suspend({MakeRequest(5U, "user1"), {}});

        size_t workers = 15U;
        std::vector<size_t> counts;
        const auto processor = [&](const IScheduler::TWaitInfo& wait) {
            counts.emplace_back(wait.Request.GetCount());
            return true;
        };

        scheduler->Process(33U, workers, processor);

        scheduler->Suspend({MakeRequest(3U, "user2"), {}});
        scheduler->Suspend({MakeRequest(7U, "user2"), {}});
        scheduler->Suspend({MakeRequest(5U, "user2"), {}});

        workers += 10U;

        scheduler->Process(33U, workers, processor);
        workers += 10U;

        scheduler->Process(33U, workers, processor);

        const std::vector<size_t> expected = {3U, 7U, 3U, 7U, 5U, 5U};
        UNIT_ASSERT_VALUES_EQUAL(counts, expected);
    }

    Y_UNIT_TEST(FifoAfterOneHour) {
        const auto scheduler = IScheduler::Make();
        UNIT_ASSERT(scheduler);

        scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        scheduler->Suspend({MakeRequest(7U, "user1"), {}});
        scheduler->Suspend({MakeRequest(5U, "user1"), {}});

        size_t workers = 15U;
        std::vector<size_t> counts;
        const auto processor = [&](const IScheduler::TWaitInfo& wait) {
            counts.emplace_back(wait.Request.GetCount());
            return true;
        };

        scheduler->Process(40U, workers, processor);

        scheduler->Suspend({MakeRequest(3U, "user2"), {}});
        scheduler->Suspend({MakeRequest(7U, "user2"), {}});
        scheduler->Suspend({MakeRequest(5U, "user2"), {}});

        workers += 10U;

        const auto skipHour = TInstant::Now() + TDuration::Hours(1U);

        scheduler->Process(40U, workers, processor, skipHour);
        workers += 10U;

        scheduler->Process(40U, workers, processor, skipHour);

        const std::vector<size_t> expected = {3U, 7U, 5U, 3U, 7U, 5U};
        UNIT_ASSERT_VALUES_EQUAL(counts, expected);
    }

    Y_UNIT_TEST(HalfWorkersForSmall) {
        const auto scheduler = IScheduler::Make();
        UNIT_ASSERT(scheduler);

        scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        scheduler->Suspend({MakeRequest(7U, "user1"), {}});
        scheduler->Suspend({MakeRequest(5U, "user1"), {}});
        scheduler->Suspend({MakeRequest(1U, "user1"), {}});
        scheduler->Suspend({MakeRequest(1U, "user1"), {}});
        scheduler->Suspend({MakeRequest(1U, "user1"), {}});
        scheduler->Suspend({MakeRequest(1U, "user1"), {}});

        size_t workers = 6U;
        std::vector<size_t> counts;
        const auto processor = [&](const IScheduler::TWaitInfo& wait) {
            counts.emplace_back(wait.Request.GetCount());
            return true;
        };

        scheduler->Process(30U, workers, processor);

        workers += 15U;

        scheduler->Process(30U, workers, processor);

        const std::vector<size_t> expected = {1U, 1U, 1U, 3U, 1U, 7U, 5U};
        UNIT_ASSERT_VALUES_EQUAL(counts, expected);
    }

    Y_UNIT_TEST(Use75PercentForLargeInNonOverload) {
        const auto scheduler = IScheduler::Make();
        UNIT_ASSERT(scheduler);

        scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        scheduler->Suspend({MakeRequest(3U, "user2"), {}});
        scheduler->Suspend({MakeRequest(3U, "user3"), {}});
        scheduler->Suspend({MakeRequest(3U, "user4"), {}});
        scheduler->Suspend({MakeRequest(3U, "user5"), {}});
        scheduler->Suspend({MakeRequest(3U, "user6"), {}});

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 6U);

        size_t workers = 4U;
        std::vector<size_t> counts;
        const auto processor = [&](const IScheduler::TWaitInfo& wait) {
            counts.emplace_back(wait.Request.GetCount());
            return true;
        };

        scheduler->Process(16U, workers, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 5U);

        scheduler->Process(16U, workers = 4U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 4U);

        scheduler->Process(16U, workers = 4U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 3U);

        scheduler->Process(16U, workers = 4U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 2U);

        scheduler->Process(16U, workers = 4U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);

        scheduler->Process(16U, workers = 4U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
    }

    Y_UNIT_TEST(ProcessAllFreesPerUserOperationSlot) {
        // Regression test: ProcessAll is used on node disconnect (DropActorOrNode) and when all
        // workers shut down (TryResume). It must decrement AwaitOperations the same way Process()
        // does, otherwise per-user limits leak and new requests get OVERLOADED.
        NYql::NProto::TDqConfig::TScheduler cfg;
        cfg.SetMaxOperationsPerUser(5);
        cfg.SetMaxOperations(1000);
        const auto scheduler = IScheduler::Make(cfg);
        UNIT_ASSERT(scheduler);
        const TString user = "user1";
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, user), {}}));
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
        scheduler->ProcessAll([](const IScheduler::TWaitInfo&) { return true; });
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
        for (int i = 0; i < 5; ++i) {
            UNIT_ASSERT_C(
                scheduler->Suspend({MakeRequest(3U, user), {}}),
                TStringBuilder() << "Failed on iteration " << i);
        }
        UNIT_ASSERT(!scheduler->Suspend({MakeRequest(3U, user), {}}));
    }

    Y_UNIT_TEST(UpdateMetricsAfterRejectedLargeRequest) {
        // A rejected user remains in AllocationsHistory and must not break metric updates.
        // Also pins incremental AllocatedTotal across allocation, history expiry and Cleanup().
        NYql::NProto::TDqConfig::TScheduler cfg;
        cfg.SetMaxOperations(1);
        cfg.SetHistoryKeepingTime(1);

        NYql::TSensorsGroupPtr sensorsPtr = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = IScheduler::Make(cfg, NYql::CreateMetricsRegistry(sensorsPtr));
        UNIT_ASSERT(scheduler);

        // First large request fills the LargeWaitList (size becomes 1 == MaxOperations).
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user1"), {}}));

        // Second large request from a brand-new user: rejected because LargeWaitList is full.
        UNIT_ASSERT(!scheduler->Suspend({MakeRequest(3U, "user2"), {}}));

        scheduler->UpdateMetrics();

        const auto schedulerCounters = sensorsPtr->FindSubgroup("component", "scheduler");
        UNIT_ASSERT(schedulerCounters);
        UNIT_ASSERT(!schedulerCounters->FindSubgroup("user", "user1"));
        UNIT_ASSERT(!schedulerCounters->FindSubgroup("user", "user2"));

        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("KnownUsers")->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 0);

        const auto now = TInstant::Now();
        const auto process = [] (const IScheduler::TWaitInfo&) { return true; };
        scheduler->Process(3U, 3U, process, now);
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 3);

        // The queue is already drained; this call only runs the history expiry sweep.
        scheduler->Process(3U, 0U, process, now + TDuration::Minutes(1));
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 0);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user1"), {}}));
        scheduler->Process(3U, 3U, process, now + TDuration::Minutes(1));
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 3);

        scheduler->Cleanup();
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("KnownUsers")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 0);
    }

    Y_UNIT_TEST(UseOnlyHalfForLargeInOverload) {
        const auto scheduler = IScheduler::Make();
        UNIT_ASSERT(scheduler);

        scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        scheduler->Suspend({MakeRequest(3U, "user2"), {}});
        scheduler->Suspend({MakeRequest(3U, "user3"), {}});
        scheduler->Suspend({MakeRequest(3U, "user4"), {}});
        scheduler->Suspend({MakeRequest(3U, "user5"), {}});
        scheduler->Suspend({MakeRequest(3U, "user6"), {}});

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 6U);

        size_t workers = 4U;
        std::vector<size_t> counts;
        const auto processor = [&](const IScheduler::TWaitInfo& wait) {
            counts.emplace_back(wait.Request.GetCount());
            return true;
        };

        scheduler->Process(20U, workers, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 6U);

        scheduler->Process(20U, workers = 5U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 5U);

        scheduler->Process(20U, workers = 4U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 5U);

        scheduler->Process(20U, workers = 5U, processor);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 4U);
    }
}
