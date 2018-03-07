#include <yt/core/test_framework/framework.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/mpsc_queue.h>

#include <thread>
#include <array>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TIntNode
{
    int Value;
    TMpscQueueHook Hook;

    TIntNode(int value)
        : Value(value)
    { }
};

TEST(TMpscQueueTest, SimpleSingleThreaded)
{
    TMpscQueue<TIntNode, &TIntNode::Hook> queue;

    queue.Push(std::make_unique<TIntNode>(1));
    queue.Push(std::make_unique<TIntNode>(2));
    queue.Push(std::make_unique<TIntNode>(3));

    auto n1 = queue.Pop();
    EXPECT_EQ(1, n1->Value);
    auto n2 = queue.Pop();
    EXPECT_EQ(2, n2->Value);
    auto n3 = queue.Pop();
    EXPECT_EQ(3, n3->Value);

    EXPECT_FALSE(static_cast<bool>(queue.Pop()));
};

TEST(TMpscQueueTest, SimpleMultiThreaded)
{
    TMpscQueue<TIntNode, &TIntNode::Hook> queue;

    constexpr int N = 10000;
    constexpr int T = 4;

    auto barrier = NewPromise<void>();

    auto producer = [&] () {
        barrier.ToFuture().Get();
        for (int i = 0; i < N; ++i) {
            queue.Push(std::make_unique<TIntNode>(i));
        }
    };

    auto consumer = [&] () {
        std::array<int, N> counts;
        counts.fill(0);
        barrier.ToFuture().Get();
        for (int i = 0; i < N * T; ++i) {
            while (true) {
                if (auto item = queue.Pop()) {
                    counts[item->Value]++;
                    break;
                }
            }
        }
        for (int i = 0; i < N; ++i) {
            EXPECT_EQ(counts[i], T);
        }
    };

    std::vector<std::thread> threads;

    threads.reserve(T + 1);
    for (int i = 0; i < T; ++i) {
        threads.emplace_back(producer);
    }
    threads.emplace_back(consumer);

    barrier.Set();

    for (int i = 0; i < T + 1; ++i) {
        threads[i].join();
    }

    SUCCEED();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
