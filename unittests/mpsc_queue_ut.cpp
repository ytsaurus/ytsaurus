#include "framework.h"
#include "probe.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/mpsc_queue.h>

#include <thread>
#include <array>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TMPSCQueueTest, SimpleSingleThreaded)
{
    TMPSCQueue<int> queue;

    queue.Push(1);
    queue.Push(2);
    queue.Push(3);

    auto n1 = queue.Pop();
    EXPECT_EQ(1, *n1);
    auto n2 = queue.Pop();
    EXPECT_EQ(2, *n2);
    auto n3 = queue.Pop();
    EXPECT_EQ(3, *n3);

    EXPECT_FALSE(static_cast<bool>(queue.Pop()));
};

TEST(TMPSCQueueTest, SimpleMultiThreaded)
{
    TMPSCQueue<int> queue;

    constexpr int N = 10000;
    constexpr int T = 4;

    auto barrier = NewPromise<void>();

    auto producer = [&] () {
        barrier.ToFuture().Get();
        for (int i = 0; i < N; ++i) {
            queue.Push(i);
        }
    };

    auto consumer = [&] () {
        std::array<int, N> counts;
        counts.fill(0);
        barrier.ToFuture().Get();
        for (int i = 0; i < N * T; ++i) {
            while (true) {
                if (auto item = queue.Pop()) {
                    counts[*item]++;
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

TEST(TMPSCQueueTest, NoCopies)
{
    TProbeState state;

    TMPSCQueue<TProbe> queue;

    auto p = TProbe(&state);
    queue.Push(std::move(p));
    auto q = *queue.Pop(); // Look, ma, no copiez!

    EXPECT_EQ(2, state.MoveAssignments + state.MoveConstructors);
    EXPECT_EQ(0, state.CopyAssignments + state.CopyConstructors);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
