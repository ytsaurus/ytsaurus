#include <yt/yt/flow/library/cpp/connectors/queue/queue_info.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NYPath;
using namespace NYTree;

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

TEST(TQueueInfoControllerTest, RetriesFailedLookupBeforeNormalPeriod)
{
    const auto normalPeriod = TDuration::Minutes(10);
    const auto retryMinBackoff = TDuration::MilliSeconds(10);
    const auto retryTimeout = TDuration::MilliSeconds(500);

    auto spec = New<TQueueInfoSpec>();
    spec->QueuePath = TRichYPath("//queue");
    spec->UpdatePartitionCountPeriod = normalPeriod;
    spec->UpdatePartitionCountRetryMinBackoff = retryMinBackoff;

    auto node = BuildYsonNodeFluently()
        .BeginAttributes()
        .Item("tablet_count")
        .Value(3)
        .EndAttributes()
        .Entity();

    auto retryStarted = NewPromise<void>();
    auto retryResult = NewPromise<NYson::TYsonString>();
    auto client = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*client, GetNode(TYPath("//queue"), _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Transient failure"))))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Transient failure"))))
        .WillOnce(Invoke([&] (const TYPath&, const TGetNodeOptions&) {
            retryStarted.Set();
            return retryResult.ToFuture();
        }));

    auto actionQueue = New<TActionQueue>();
    auto controller = New<TQueueInfoController>(
        spec,
        client,
        actionQueue->GetInvoker(),
        TLogger("test"),
        CreateSyncStatusProfiler());
    auto stateManager = New<TStateManagerMock>();

    controller->Init(stateManager->CreateContext());
    WaitFor(retryStarted.ToFuture().WithTimeout(retryTimeout)).ThrowOnError();

    retryResult.Set(NYson::ConvertToYsonString(node));
    auto partitionCount = WaitFor(BIND([controller] {
        return controller->GetPartitionCount();
    }).AsyncVia(actionQueue->GetInvoker())
            .Run())
        .ValueOrThrow();
    EXPECT_EQ(partitionCount, 3);

    controller.Reset();
    actionQueue->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
