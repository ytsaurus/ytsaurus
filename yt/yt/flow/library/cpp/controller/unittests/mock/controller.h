#pragma once

#include <yt/yt/flow/library/cpp/controller/controller.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

struct TMockController
    : public IController
{
    // clang-format off

    MOCK_METHOD(void, Initialize, (), (override));
    MOCK_METHOD(void, EnsureIsLeader, (), (const, override));
    MOCK_METHOD(TFlowViewKeeperPtr, GetFlowViewKeeper, (), (const, override));
    MOCK_METHOD(TNodeInfoPtr, GetNodeInfo, (), (const, override));
    MOCK_METHOD(IVersionProviderPtr, GetVersionProvider, (), (const, override));
    MOCK_METHOD(void, RegisterJobStatus, (const TJobId& jobId, TJobStatusPtr status), (override));
    MOCK_METHOD(void, RegisterWorkerStatus, (TStringBuf workerAddress, TWorkerStatusPtr status), (override));

    // clang-format on
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
