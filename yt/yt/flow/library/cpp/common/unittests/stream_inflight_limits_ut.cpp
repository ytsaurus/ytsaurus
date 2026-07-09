#include <yt/yt/flow/library/cpp/common/stream_inflight_limits.h>

#include <yt/yt/core/test_framework/framework.h>

#include <thread>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TStreamLimitUsageState, ConcurrentWriterReaderSeesConsistentSnapshots)
{
    auto state = New<TStreamLimitUsageState>(/*inflation*/ 0);

    std::atomic<bool> stop{false};
    constexpr int kIterations = 100'000;

    // Distinct multipliers per cumulative field so any torn snapshot violates the
    // ratio invariants checked below.
    std::thread writer([&] {
        for (int i = 1; i <= kIterations; ++i) {
            state->Update(TStreamUsage{
                .CumulativeByteIn = i,
                .CumulativeByteOut = 2 * i,
                .CumulativeCountIn = 3 * i,
                .CumulativeCountOut = 4 * i,
                .PendingInflatedBytes = i % 10,
            });
        }
        stop.store(true, std::memory_order_relaxed);
    });

    int observedReads = 0;
    while (!stop.load(std::memory_order_relaxed)) {
        auto u = state->Read();
        EXPECT_EQ(u.CumulativeByteOut, 2 * u.CumulativeByteIn);
        EXPECT_EQ(u.CumulativeCountIn, 3 * u.CumulativeByteIn);
        EXPECT_EQ(u.CumulativeCountOut, 4 * u.CumulativeByteIn);
        ++observedReads;
    }
    writer.join();

    EXPECT_GT(observedReads, 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
