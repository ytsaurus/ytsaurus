#pragma once

#include <yt/yt/flow/library/cpp/controller/worker.h>
#include <yt/yt/flow/library/cpp/controller/worker_tracker.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TMockWorkerTracker);

struct TMockWorkerTracker
    : public IWorkerTracker
{
    // clang-format off

    MOCK_METHOD(void, Initialize, (), (override));

    MOCK_METHOD(std::vector<TWorkerInfo>, GetWorkers, (), (const, override));
    MOCK_METHOD(TWorkerInfo, RegisterWorker, (
        const TNodeInfoBase& address,
        std::vector<TWorkerGroupId> groups,
        (THashMap<std::string, ssize_t>) capabilities),
        (override));
    MOCK_METHOD(TWorkerInfo, HandleWorkerHeartbeat, (
        const std::string& address,
        NWorker::TIncarnationId connectionIncarnationId,
        ui64 heartbeatSeqNo),
        (override));
    MOCK_METHOD(void, Reconfigure, (
        const TDynamicPipelineSpecPtr& dynamicSpec),
        (override));

    // clang-format on
};

DEFINE_REFCOUNTED_TYPE(TMockWorkerTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
