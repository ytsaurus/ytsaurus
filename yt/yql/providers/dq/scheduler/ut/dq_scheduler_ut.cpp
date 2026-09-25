#include "../dq_scheduler.h"

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>

using namespace NYql::NDq;
using ESuspendStatus = IScheduler::ESuspendStatus;

namespace {
NYql::NDqProto::TAllocateWorkersRequest MakeRequest(ui32 count, const TString& user) {
    NYql::NDqProto::TAllocateWorkersRequest request;
    request.SetCount(count);
    request.SetUser(user);
    return request;
}

NYql::NProto::TDqConfig::TScheduler MakeRunningLimiterConfig() {
    NYql::NProto::TDqConfig::TScheduler config;
    config.SetLimitRunningTasksPerUserPercent(50);
    return config;
}

IScheduler::TPtr MakeRunningLimiterScheduler(
    size_t targetCapacity,
    NYql::IMetricsRegistryPtr metricsRegistry = {})
{
    return IScheduler::Make(
        MakeRunningLimiterConfig(),
        std::move(metricsRegistry),
        targetCapacity);
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

        NYql::TSensorsGroupPtr sensors = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = IScheduler::Make(cfg, NYql::CreateMetricsRegistry(sensors));
        UNIT_ASSERT(scheduler);
        const auto userLimited = sensors->GetSubgroup("component", "scheduler")
            ->FindCounter("UserLimited");
        UNIT_ASSERT(userLimited);
        UNIT_ASSERT(userLimited->ForDerivative());
        UNIT_ASSERT_VALUES_EQUAL(userLimited->Val(), 0);

        scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        scheduler->Suspend({MakeRequest(3U, "user1"), {}});

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 2U);

        std::vector<size_t> counts;
        const auto processor = [&](const IScheduler::TWaitInfo& wait) {
            counts.emplace_back(wait.Request.GetCount());
            return true;
        };

        const auto now = TInstant::Now();
        scheduler->Process(11U, 7U, processor, now);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(userLimited->Val(), 1);

        scheduler->Process(11U, 7U, processor, now);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(userLimited->Val(), 2);

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
        // Regression test: ProcessAll must decrement AwaitOperations the way Process() does,
        // otherwise per-user limits leak and new requests get OVERLOADED.
        NYql::NProto::TDqConfig::TScheduler cfg;
        cfg.SetMaxOperationsPerUser(5);
        cfg.SetMaxOperations(1000);
        const auto scheduler = IScheduler::Make(cfg);
        UNIT_ASSERT(scheduler);
        const TString user = "user1";
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, user), {}}).Status == ESuspendStatus::Accepted);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
        scheduler->ProcessAll([](const IScheduler::TWaitInfo&) { return true; });
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
        for (int i = 0; i < 5; ++i) {
            UNIT_ASSERT_C(
                scheduler->Suspend({MakeRequest(3U, user), {}}).Status == ESuspendStatus::Accepted,
                TStringBuilder() << "Failed on iteration " << i);
        }
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, user), {}}).Status == ESuspendStatus::PerUserLimit);
    }

    Y_UNIT_TEST(SuspendReportsLimitAndCounts) {
        NYql::NProto::TDqConfig::TScheduler cfg;
        cfg.SetMaxOperations(2);
        cfg.SetMaxOperationsPerUser(2);

        NYql::TSensorsGroupPtr sensors = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = IScheduler::Make(cfg, NYql::CreateMetricsRegistry(sensors));

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user1"), {}}).Status == ESuspendStatus::Accepted);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user1"), {}}).Status == ESuspendStatus::Accepted);

        const auto perUserLimit = scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        UNIT_ASSERT(perUserLimit.Status == ESuspendStatus::PerUserLimit);
        UNIT_ASSERT_VALUES_EQUAL(perUserLimit.WaitingOperations, 2);
        UNIT_ASSERT_VALUES_EQUAL(perUserLimit.Limit, 2);

        const auto globalLimit = scheduler->Suspend({MakeRequest(3U, "user2"), {}});
        UNIT_ASSERT(globalLimit.Status == ESuspendStatus::GlobalLimit);
        UNIT_ASSERT_VALUES_EQUAL(globalLimit.WaitingOperations, 2);
        UNIT_ASSERT_VALUES_EQUAL(globalLimit.Limit, 2);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(1U, "user1"), {}}).Status == ESuspendStatus::Accepted);
        const auto perUserLimitAfterSmall = scheduler->Suspend({MakeRequest(3U, "user1"), {}});
        UNIT_ASSERT(perUserLimitAfterSmall.Status == ESuspendStatus::PerUserLimit);
        UNIT_ASSERT_VALUES_EQUAL(perUserLimitAfterSmall.WaitingOperations, 3);

        const auto counters = sensors->FindSubgroup("component", "scheduler");
        UNIT_ASSERT(counters);
        UNIT_ASSERT_VALUES_EQUAL(counters->FindCounter("PerUserQueueLimitRejections")->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters->FindCounter("GlobalQueueLimitRejections")->Val(), 1);
    }

    Y_UNIT_TEST(ZeroPerUserOperationLimitRejectsNewUser) {
        NYql::NProto::TDqConfig::TScheduler cfg;
        cfg.SetMaxOperationsPerUser(0);

        NYql::TSensorsGroupPtr sensors = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = IScheduler::Make(cfg, NYql::CreateMetricsRegistry(sensors));
        const auto rejectionCounter = sensors->GetSubgroup("component", "scheduler")
            ->GetCounter("PerUserQueueLimitRejections");

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user"), {}}).Status == ESuspendStatus::PerUserLimit);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
        UNIT_ASSERT(rejectionCounter->ForDerivative());
        UNIT_ASSERT_VALUES_EQUAL(rejectionCounter->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            sensors->GetSubgroup("component", "scheduler")->GetCounter("KnownUsers")->Val(),
            0);
    }

    // A large request rejected because LargeWaitList is full must not leave user state behind.
    Y_UNIT_TEST(RejectedLargeRequestDoesNotCreateUserState) {
        // Also pins aggregate metrics across allocation, history expiry and Cleanup().
        NYql::NProto::TDqConfig::TScheduler cfg;
        cfg.SetMaxOperations(1);
        cfg.SetHistoryKeepingTime(1);

        NYql::TSensorsGroupPtr sensorsPtr = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = IScheduler::Make(cfg, NYql::CreateMetricsRegistry(sensorsPtr));
        UNIT_ASSERT(scheduler);
        const auto rejectionCounter = sensorsPtr->GetSubgroup("component", "scheduler")
            ->GetCounter("GlobalQueueLimitRejections");

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user1"), {}}).Status == ESuspendStatus::Accepted);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user2"), {}}).Status == ESuspendStatus::GlobalLimit);
        UNIT_ASSERT(rejectionCounter->ForDerivative());
        UNIT_ASSERT_VALUES_EQUAL(rejectionCounter->Val(), 1);

        scheduler->UpdateMetrics();

        const auto schedulerCounters = sensorsPtr->FindSubgroup("component", "scheduler");
        UNIT_ASSERT(schedulerCounters);
        UNIT_ASSERT(!schedulerCounters->FindSubgroup("user", "user1"));
        UNIT_ASSERT(!schedulerCounters->FindSubgroup("user", "user2"));

        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("KnownUsers")->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 0);

        const auto now = TInstant::Now();
        const auto process = [] (const IScheduler::TWaitInfo&) { return true; };
        scheduler->Process(3U, 3U, process, now);
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 3);

        scheduler->ReleaseRunningTasks("user1", 3U);
        // The queue is already drained; this call only runs the history expiry sweep.
        scheduler->Process(3U, 0U, process, now + TDuration::Minutes(1));
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("KnownUsers")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 0);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user1"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(3U, 3U, process, now + TDuration::Minutes(1));
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 3);

        scheduler->Cleanup();
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("KnownUsers")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 0);
    }

    Y_UNIT_TEST(PerUserMetrics) {
        NYql::NProto::TDqConfig::TScheduler cfg;
        cfg.SetHistoryKeepingTime(1);
        cfg.SetMaxOperations(1);
        cfg.SetEnablePerUserMetrics(true);

        NYql::TSensorsGroupPtr sensorsPtr = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = IScheduler::Make(cfg, NYql::CreateMetricsRegistry(sensorsPtr));
        const auto schedulerCounters = sensorsPtr->FindSubgroup("component", "scheduler");
        UNIT_ASSERT(scheduler);
        UNIT_ASSERT(schedulerCounters);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user1"), {}}).Status == ESuspendStatus::Accepted);
        const auto user1Counters = schedulerCounters->FindSubgroup("user", "user1");
        UNIT_ASSERT(user1Counters);
        UNIT_ASSERT_VALUES_EQUAL(user1Counters->FindCounter("Await")->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(user1Counters->FindCounter("AwaitOperations")->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(user1Counters->FindCounter("Allocated")->Val(), 0);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user2"), {}}).Status == ESuspendStatus::GlobalLimit);
        const auto user2Counters = schedulerCounters->FindSubgroup("user", "user2");
        UNIT_ASSERT(!user2Counters);

        const auto now = TInstant::Now();
        const auto process = [] (const IScheduler::TWaitInfo&) { return true; };
        scheduler->Process(3U, 3U, process, now);
        UNIT_ASSERT_VALUES_EQUAL(user1Counters->FindCounter("Await")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(user1Counters->FindCounter("AwaitOperations")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(user1Counters->FindCounter("Allocated")->Val(), 3);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(4U, "user1"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->ProcessAll(process);
        UNIT_ASSERT_VALUES_EQUAL(user1Counters->FindCounter("Await")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(user1Counters->FindCounter("AwaitOperations")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(user1Counters->FindCounter("Allocated")->Val(), 3);

        scheduler->ReleaseRunningTasks("user1", 3U);
        scheduler->Process(3U, 0U, process, now + TDuration::Minutes(1));
        UNIT_ASSERT_VALUES_EQUAL(user1Counters->FindCounter("Allocated")->Val(), 0);
        scheduler->UpdateMetrics();
        UNIT_ASSERT(!schedulerCounters->FindSubgroup("user", "user1"));
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 0);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user1"), {}}).Status == ESuspendStatus::Accepted);
        const auto recreatedUser1Counters = schedulerCounters->FindSubgroup("user", "user1");
        UNIT_ASSERT(recreatedUser1Counters);
        scheduler->Process(3U, 3U, process, now + TDuration::Minutes(1));
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(recreatedUser1Counters->FindCounter("Allocated")->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 3);

        scheduler->Cleanup();
        UNIT_ASSERT_VALUES_EQUAL(recreatedUser1Counters->FindCounter("Await")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(recreatedUser1Counters->FindCounter("AwaitOperations")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(recreatedUser1Counters->FindCounter("Allocated")->Val(), 0);
        UNIT_ASSERT(!schedulerCounters->FindSubgroup("user", "user1"));
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

    Y_UNIT_TEST(RunningLimiterDisabledByDefault) {
        const auto scheduler = IScheduler::Make();

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(12U, "user"), {}}).Status == ESuspendStatus::Accepted);

        ui32 allocatedCount = 0;
        scheduler->Process(20U, 20U, [&] (const IScheduler::TWaitInfo& info) {
            allocatedCount += info.Request.GetCount();
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(allocatedCount, 12U);
    }

    Y_UNIT_TEST(RunningLimiterDisabledByZeroPercent) {
        NYql::NProto::TDqConfig::TScheduler config;
        config.SetLimitRunningTasksPerUserPercent(0);
        NYql::TSensorsGroupPtr sensors = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = IScheduler::Make(
            config,
            NYql::CreateMetricsRegistry(sensors),
            /*targetCapacity*/ 42U);
        const auto runningLimitedQueueSize = sensors->GetSubgroup("component", "scheduler")
            ->GetCounter("RunningLimitedQueueSize");

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(43U, "user"), {}}).Status == ESuspendStatus::Accepted);

        ui32 allocatedCount = 0;
        scheduler->Process(100U, 100U, [&] (const IScheduler::TWaitInfo& info) {
            allocatedCount += info.Request.GetCount();
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(allocatedCount, 43U);

        *runningLimitedQueueSize = 1;
        scheduler->UpdateMetrics(/*updateRunningLimitedQueueSize*/ false);
        UNIT_ASSERT_VALUES_EQUAL(runningLimitedQueueSize->Val(), 0);
    }

    Y_UNIT_TEST(RunningLimiterRoundsUp) {
        const auto scheduler = MakeRunningLimiterScheduler(21U);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(11U, "user"), {}}).Status == ESuspendStatus::Accepted);

        ui32 allocatedCount = 0;
        scheduler->Process(21U, 21U, [&] (const IScheduler::TWaitInfo& info) {
            allocatedCount += info.Request.GetCount();
            return true;
        });

        UNIT_ASSERT_VALUES_EQUAL(allocatedCount, 11U);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(1U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(21U, 21U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
    }

    Y_UNIT_TEST(RunningLimiterRoundsArbitraryPercentUp) {
        auto config = MakeRunningLimiterConfig();
        config.SetLimitRunningTasksPerUserPercent(20);
        const auto scheduler = IScheduler::Make(
            config,
            /*metricsRegistry*/ {},
            /*targetCapacity*/ 7U);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(2U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(7U, 7U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(1U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(7U, 7U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
    }

    Y_UNIT_TEST(RunningLimiterAllowsExactLimitAndQueuesAboveIt) {
        const auto scheduler = MakeRunningLimiterScheduler(20U);
        const auto allocate = [] (const IScheduler::TWaitInfo&) {
            return true;
        };

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(6U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, allocate);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(4U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, allocate);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(1U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, allocate);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
    }

    Y_UNIT_TEST(RunningLimiterQueuesUserAtLimitAndAllowsAnotherUser) {
        NYql::TSensorsGroupPtr sensors = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = MakeRunningLimiterScheduler(
            20U,
            NYql::CreateMetricsRegistry(sensors));
        const auto runningLimitedQueueSize = sensors->GetSubgroup("component", "scheduler")
            ->GetCounter("RunningLimitedQueueSize");
        const auto allocate = [] (const IScheduler::TWaitInfo&) {
            return true;
        };

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(10U, "user1"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, allocate);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user1"), {}}).Status == ESuspendStatus::Accepted);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(3U, "user2"), {}}).Status == ESuspendStatus::Accepted);

        TVector<TString> allocatedUsers;
        scheduler->Process(20U, 6U, [&] (const IScheduler::TWaitInfo& info) {
            allocatedUsers.push_back(info.Request.GetUser());
            return allocate(info);
        });
        UNIT_ASSERT_VALUES_EQUAL(allocatedUsers, TVector<TString>({"user2"}));

        UNIT_ASSERT_VALUES_EQUAL(
            scheduler->UpdateMetrics(/*updateRunningLimitedQueueSize*/ false),
            1U);
        UNIT_ASSERT_VALUES_EQUAL(runningLimitedQueueSize->Val(), 0);
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(runningLimitedQueueSize->Val(), 1);

        scheduler->ReleaseRunningTasks("user1", 10U);
        scheduler->Process(20U, 20U, allocate);
        scheduler->UpdateMetrics(/*updateRunningLimitedQueueSize*/ false);
        UNIT_ASSERT_VALUES_EQUAL(runningLimitedQueueSize->Val(), 1);
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(runningLimitedQueueSize->Val(), 0);
    }

    Y_UNIT_TEST(RunningLimiterReleasesUserQuota) {
        const auto scheduler = MakeRunningLimiterScheduler(20U);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(10U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(4U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);

        scheduler->ReleaseRunningTasks("user", 4U);
        scheduler->Process(20U, 20U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
    }

    Y_UNIT_TEST(RunningLimiterPreservesRunningAfterHistoryExpires) {
        auto config = MakeRunningLimiterConfig();
        config.SetHistoryKeepingTime(1);
        config.SetEnablePerUserMetrics(true);

        NYql::TSensorsGroupPtr sensors = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = IScheduler::Make(config, NYql::CreateMetricsRegistry(sensors), /*targetCapacity*/ 20U);
        const auto schedulerCounters = sensors->FindSubgroup("component", "scheduler");
        UNIT_ASSERT(schedulerCounters);

        const auto now = TInstant::Now();
        const auto afterHistoryExpires = now + TDuration::Minutes(1);
        const auto allocate = [] (const IScheduler::TWaitInfo&) { return true; };

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(10U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, allocate, now);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 10);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("RunningTotal")->Val(), 10);

        scheduler->Process(20U, 10U, allocate, afterHistoryExpires);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("RunningTotal")->Val(), 10);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("KnownUsers")->Val(), 1);
        const auto userCounters = schedulerCounters->FindSubgroup("user", "user");
        UNIT_ASSERT(userCounters);
        UNIT_ASSERT_VALUES_EQUAL(userCounters->FindCounter("Allocated")->Val(), 0);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(1U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 10U, allocate, afterHistoryExpires);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("RunningTotal")->Val(), 10);

        scheduler->ReleaseRunningTasks("user", 10U);
        scheduler->Process(20U, 20U, allocate, afterHistoryExpires);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("RunningTotal")->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(userCounters->FindCounter("Allocated")->Val(), 1);

        scheduler->ReleaseRunningTasks("user", 1U);
        scheduler->Process(20U, 20U, allocate, afterHistoryExpires + TDuration::Minutes(1));
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("AllocatedTotal")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("RunningTotal")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->FindCounter("KnownUsers")->Val(), 0);
        UNIT_ASSERT(!schedulerCounters->FindSubgroup("user", "user"));
    }

    Y_UNIT_TEST(RunningLimiterIgnoresReleaseForUnknownUser) {
        NYql::TSensorsGroupPtr sensors = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = MakeRunningLimiterScheduler(
            20U,
            NYql::CreateMetricsRegistry(sensors));
        const auto runningTotal = sensors->GetSubgroup("component", "scheduler")
            ->GetCounter("RunningTotal");

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(4U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });

        scheduler->ReleaseRunningTasks("unknown", 2U);
        scheduler->UpdateMetrics();

        UNIT_ASSERT_VALUES_EQUAL(runningTotal->Val(), 4);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(10U, "other-user"), {}}).Status == ESuspendStatus::Accepted);
    }

    Y_UNIT_TEST(RunningLimiterSaturatesUnaccountedRelease) {
        NYql::TSensorsGroupPtr sensors = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = MakeRunningLimiterScheduler(
            20U,
            NYql::CreateMetricsRegistry(sensors));
        const auto runningTotal = sensors->GetSubgroup("component", "scheduler")
            ->GetCounter("RunningTotal");

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(10U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });

        scheduler->ReleaseRunningTasks("user", 11U);
        scheduler->UpdateMetrics();

        UNIT_ASSERT_VALUES_EQUAL(runningTotal->Val(), 0);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(10U, "user"), {}}).Status == ESuspendStatus::Accepted);

        ui32 allocatedCount = 0;
        scheduler->Process(20U, 20U, [&] (const IScheduler::TWaitInfo& info) {
            allocatedCount += info.Request.GetCount();
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(allocatedCount, 10U);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
        UNIT_ASSERT_VALUES_EQUAL(runningTotal->Val(), 10);
    }

    Y_UNIT_TEST(RunningLimiterDoesNotReserveQueuedCapacity) {
        const auto scheduler = MakeRunningLimiterScheduler(20U);

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(6U, "user1"), {}}).Status == ESuspendStatus::Accepted);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(6U, "user1"), {}}).Status == ESuspendStatus::Accepted);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 2U);

        ui32 allocatedCount = 0;
        scheduler->Process(20U, 20U, [&] (const IScheduler::TWaitInfo& info) {
            allocatedCount += info.Request.GetCount();
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(allocatedCount, 6U);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
    }

    Y_UNIT_TEST(RunningLimiterUsesConfiguredTargetCapacity) {
        const auto scheduler = MakeRunningLimiterScheduler(20U);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(6U, "user"), {}}).Status == ESuspendStatus::Accepted);

        TVector<ui32> allocatedCounts;
        const auto allocate = [&] (const IScheduler::TWaitInfo& info) {
            allocatedCounts.push_back(info.Request.GetCount());
            return true;
        };

        scheduler->Process(5U, 5U, allocate);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
        UNIT_ASSERT(allocatedCounts.empty());

        scheduler->Process(8U, 8U, allocate);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
        UNIT_ASSERT_VALUES_EQUAL(allocatedCounts, TVector<ui32>({6U}));

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(4U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(8U, 8U, allocate);
        UNIT_ASSERT_VALUES_EQUAL(allocatedCounts, TVector<ui32>({6U, 4U}));
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(1U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(8U, 8U, allocate);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
    }

    Y_UNIT_TEST(RunningLimiterQueuesOversizedRequestWithoutBlockingOtherUser) {
        const auto scheduler = MakeRunningLimiterScheduler(20U);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(11U, "user"), {}}).Status == ESuspendStatus::Accepted);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(10U, "other-user"), {}}).Status == ESuspendStatus::Accepted);

        TVector<TString> allocatedUsers;
        scheduler->Process(20U, 20U, [&] (const IScheduler::TWaitInfo& info) {
            allocatedUsers.push_back(info.Request.GetUser());
            return true;
        });

        UNIT_ASSERT_VALUES_EQUAL(allocatedUsers, TVector<TString>({"other-user"}));
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);
    }

    Y_UNIT_TEST(RunningLimitedSmallDoesNotReserveCapacityFromLarge) {
        const auto scheduler = MakeRunningLimiterScheduler(20U);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(10U, "limited-user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(1U, "limited-user"), {}}).Status == ESuspendStatus::Accepted);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(8U, "other-user"), {}}).Status == ESuspendStatus::Accepted);

        ui32 limitedUserAllocated = 0;
        ui32 otherUserAllocated = 0;
        const auto allocate = [&] (const IScheduler::TWaitInfo& info) {
            auto& allocated = info.Request.GetUser() == "limited-user"
                ? limitedUserAllocated
                : otherUserAllocated;
            allocated += info.Request.GetCount();
            return true;
        };

        scheduler->Process(20U, 10U, allocate);
        UNIT_ASSERT_VALUES_EQUAL(limitedUserAllocated, 0U);
        UNIT_ASSERT_VALUES_EQUAL(otherUserAllocated, 8U);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);

        scheduler->ReleaseRunningTasks("limited-user", 10U);
        scheduler->Process(20U, 12U, allocate);
        UNIT_ASSERT_VALUES_EQUAL(limitedUserAllocated, 1U);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
    }

    Y_UNIT_TEST(RunningLimitedLargeDoesNotReserveCapacityFromSmall) {
        const auto scheduler = MakeRunningLimiterScheduler(20U);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(10U, "limited-user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(2U, "limited-user"), {}}).Status == ESuspendStatus::Accepted);
        for (int index = 0; index < 3; ++index) {
            UNIT_ASSERT(scheduler->Suspend({MakeRequest(1U, "other-user"), {}}).Status == ESuspendStatus::Accepted);
        }

        ui32 limitedUserAllocated = 0;
        ui32 otherUserAllocated = 0;
        const auto allocate = [&] (const IScheduler::TWaitInfo& info) {
            auto& allocated = info.Request.GetUser() == "limited-user"
                ? limitedUserAllocated
                : otherUserAllocated;
            allocated += info.Request.GetCount();
            return true;
        };

        scheduler->Process(20U, 3U, allocate);
        UNIT_ASSERT_VALUES_EQUAL(limitedUserAllocated, 0U);
        UNIT_ASSERT_VALUES_EQUAL(otherUserAllocated, 3U);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 1U);

        scheduler->ReleaseRunningTasks("limited-user", 10U);
        scheduler->Process(20U, 10U, allocate);
        UNIT_ASSERT_VALUES_EQUAL(limitedUserAllocated, 2U);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->UpdateMetrics(), 0U);
    }

    Y_UNIT_TEST(RunningMetricsAndCleanup) {
        NYql::TSensorsGroupPtr sensors = MakeIntrusive<NYql::TSensorsGroup>();
        const auto scheduler = MakeRunningLimiterScheduler(
            20U,
            NYql::CreateMetricsRegistry(sensors));
        const auto schedulerCounters = sensors->GetSubgroup("component", "scheduler");
        const auto runningTotal = schedulerCounters->GetCounter("RunningTotal");
        const auto runningLimitedQueueSize = schedulerCounters->GetCounter("RunningLimitedQueueSize");
        const auto integralQueueSizeForLarge = schedulerCounters->GetCounter("IntegralQueueSizeForLarge");
        UNIT_ASSERT(!schedulerCounters->FindSubgroup("user", "user"));

        UNIT_ASSERT(scheduler->Suspend({MakeRequest(10U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(1U, "user"), {}}).Status == ESuspendStatus::Accepted);
        UNIT_ASSERT(scheduler->Suspend({MakeRequest(2U, "user"), {}}).Status == ESuspendStatus::Accepted);
        scheduler->Process(20U, 20U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });
        scheduler->UpdateMetrics();
        UNIT_ASSERT_VALUES_EQUAL(runningTotal->Val(), 10);
        UNIT_ASSERT_VALUES_EQUAL(runningLimitedQueueSize->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(integralQueueSizeForLarge->Val(), 2);

        scheduler->Cleanup();
        UNIT_ASSERT_VALUES_EQUAL(runningLimitedQueueSize->Val(), 0);
        scheduler->UpdateMetrics(/*updateRunningLimitedQueueSize*/ false);
        UNIT_ASSERT_VALUES_EQUAL(runningTotal->Val(), 0);
    }

    Y_UNIT_TEST(RunningLimiterNormalizesInvalidInputs) {
        auto config = MakeRunningLimiterConfig();
        config.SetLimitRunningTasksPerUserPercent(101);
        const auto clampedScheduler = IScheduler::Make(
            config,
            /*metricsRegistry*/ {},
            /*targetCapacity*/ 42U);
        UNIT_ASSERT(clampedScheduler->Suspend({MakeRequest(42U, "user"), {}}).Status == ESuspendStatus::Accepted);
        clampedScheduler->Process(42U, 42U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });
        UNIT_ASSERT(clampedScheduler->Suspend({MakeRequest(1U, "user"), {}}).Status == ESuspendStatus::Accepted);
        clampedScheduler->Process(42U, 42U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(clampedScheduler->UpdateMetrics(), 1U);

        config.SetLimitRunningTasksPerUserPercent(50);
        const auto disabledScheduler = IScheduler::Make(config);
        UNIT_ASSERT(disabledScheduler->Suspend({MakeRequest(1U, "user"), {}}).Status == ESuspendStatus::Accepted);
        ui32 allocatedCount = 0;
        disabledScheduler->Process(1U, 1U, [&] (const IScheduler::TWaitInfo& info) {
            allocatedCount += info.Request.GetCount();
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(allocatedCount, 1U);

        config.SetLimitRunningTasksPerUserPercent(100);
        const auto fullCapacityScheduler = IScheduler::Make(
            config,
            /*metricsRegistry*/ {},
            /*targetCapacity*/ 42U);
        UNIT_ASSERT(fullCapacityScheduler->Suspend({MakeRequest(42U, "user"), {}}).Status == ESuspendStatus::Accepted);
        fullCapacityScheduler->Process(42U, 42U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });
        UNIT_ASSERT(fullCapacityScheduler->Suspend({MakeRequest(1U, "user"), {}}).Status == ESuspendStatus::Accepted);
        fullCapacityScheduler->Process(42U, 42U, [] (const IScheduler::TWaitInfo&) {
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(fullCapacityScheduler->UpdateMetrics(), 1U);
    }
}
