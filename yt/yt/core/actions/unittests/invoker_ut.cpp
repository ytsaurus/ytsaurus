#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TTraverser final
{
    int MaxDepth = 0;
    int CurrentDepth = 0;

    std::vector<int> VisitedNodes;

    bool Binary = false;

    //! If binary is true, Call(x) leads to Call(2 * x) and Call(2 * x + 1);
    //! otherwise Call(x) leads to Call(x + 1).
    explicit TTraverser(bool binary)
        : Binary(binary)
    { }

    void Call(int node, int limit)
    {
        ++CurrentDepth;
        auto finally = Finally([&] { --CurrentDepth; });
        if (MaxDepth < CurrentDepth) {
            MaxDepth = CurrentDepth;
        }

        if (node > limit) {
            return;
        }

        VisitedNodes.push_back(node);

        if (Binary) {
            GetSyncInvoker()->Invoke(BIND(&TTraverser::Call, MakeStrong(this), 2 * node, limit));
            GetSyncInvoker()->Invoke(BIND(&TTraverser::Call, MakeStrong(this), 2 * node + 1, limit));
        } else {
            GetSyncInvoker()->Invoke(BIND(&TTraverser::Call, MakeStrong(this), node + 1, limit));
        }
    }
};

TEST(TestSyncInvoker, TraverseLinear)
{
    auto traverser = New<TTraverser>(/*binary*/ false);

    GetSyncInvoker()->Invoke(BIND(&TTraverser::Call, traverser, 1, 1000));
    EXPECT_EQ(1, traverser->MaxDepth);

    std::vector<int> expectedNodes(1000);
    std::iota(expectedNodes.begin(), expectedNodes.end(), 1);
    EXPECT_EQ(expectedNodes, traverser->VisitedNodes);
}

TEST(TestSyncInvoker, TraverseBinary)
{
    auto traverser = New<TTraverser>(/*binary*/ true);

    GetSyncInvoker()->Invoke(BIND(&TTraverser::Call, traverser, 1, 1000));
    EXPECT_EQ(1, traverser->MaxDepth);

    std::vector<int> expectedNodes(1000);
    std::iota(expectedNodes.begin(), expectedNodes.end(), 1);
    EXPECT_EQ(expectedNodes, traverser->VisitedNodes);
}

TEST(TestSyncInvoker, SleepyFiber)
{
    // This test ensures that deferred callbacks in the sync invoker
    // are tracked per-fiber, which allows them to sleep without
    // breaking the synchronous manner of execution for other fibers.

    auto queue = New<TActionQueue>("Test");
    auto invoker = queue->GetInvoker();

    auto completionFlag = NewPromise<void>();

    std::vector<TString> events;

    auto actionA = [&] {
        events.push_back("A started");

        auto actionB = [&] {
            events.push_back("B started");
            events.push_back("B out");
            WaitFor(completionFlag.ToFuture())
                .ThrowOnError();
            events.push_back("B in");
            events.push_back("B finished");
        };

        GetSyncInvoker()->Invoke(BIND(actionB));

        events.push_back("A finished");
    };

    auto actionC = [&] {
        events.push_back("C started");

        auto actionD = [&] {
            events.push_back("D started");
            completionFlag.Set();
            events.push_back("D finished");
        };

        GetSyncInvoker()->Invoke(BIND(actionD));

        events.push_back("C finished");
    };

    auto asyncA = BIND(actionA)
        .AsyncVia(invoker)
        .Run();
    auto asyncC = BIND(actionC)
        .AsyncVia(invoker)
        .Run();

    AllSucceeded(std::vector<TFuture<void>>{asyncA, asyncC})
        .Get()
        .ThrowOnError();

    std::vector<TString> expectedEvents{
        "A started", "B started", "B out", "C started", "D started", "D finished", "C finished", "B in", "B finished", "A finished",
    };

    EXPECT_EQ(expectedEvents, events);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
