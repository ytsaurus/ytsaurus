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

TEST(TStreamLimitUsageState, MaxInflatedInflightHighWatermark)
{
    auto state = New<TStreamLimitUsageState>(/*inflation*/ 10);

    EXPECT_EQ(state->ReadAndResetMaxInflatedInflightBytes(), 0);

    // Inflight: 100 bytes + 2 messages * 10 inflation = 120.
    state->Update(TStreamUsage{
        .CumulativeByteIn = 100,
        .CumulativeByteOut = 0,
        .CumulativeCountIn = 2,
        .CumulativeCountOut = 0,
    });
    // Drained down to 50 bytes + 1 message = 60; the peak of 120 must survive.
    state->Update(TStreamUsage{
        .CumulativeByteIn = 100,
        .CumulativeByteOut = 50,
        .CumulativeCountIn = 2,
        .CumulativeCountOut = 1,
    });

    EXPECT_EQ(state->ReadAndResetMaxInflatedInflightBytes(), 120);
    // Reset: no updates since, so the watermark is back to zero.
    EXPECT_EQ(state->ReadAndResetMaxInflatedInflightBytes(), 0);

    // Next window sees only the current (lower) inflight.
    state->Update(TStreamUsage{
        .CumulativeByteIn = 100,
        .CumulativeByteOut = 50,
        .CumulativeCountIn = 2,
        .CumulativeCountOut = 1,
    });
    EXPECT_EQ(state->ReadAndResetMaxInflatedInflightBytes(), 60);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
