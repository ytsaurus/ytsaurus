#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/prefetch.h>

#include <numeric>
#include <unordered_set>
#include <vector>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

// Per-stage recorder: how many elements the stage's Fn touched, the set of touched values, and the
// running frontier (max value seen). Element values equal their index (iota), so the frontier lets
// us assert the exact prefetch distance.
struct TStageTracker
{
    int Count = 0;
    int Frontier = -1;
    std::unordered_set<int> Seen;
};

auto MakeStage(std::vector<TStageTracker>& trackers, int stage)
{
    return [&trackers, stage] (int value) {
        auto& tracker = trackers[stage];
        EXPECT_TRUE(tracker.Seen.insert(value).second)
            << "stage " << stage << " prefetched " << value << " twice";
        tracker.Frontier = std::max(tracker.Frontier, value);
        ++tracker.Count;
    };
}

// Body asserts the lead invariant: when the body reaches element |i|, stage |k| must sit exactly at
// |i + offsets[k]| (its whole prologue+main history collapses to that frontier), which pins the
// offset resolved from the deltas. Targets past the end are skipped (the stage doesn't run there).
auto MakeBody(
    const std::vector<TStageTracker>& trackers,
    const std::vector<int>& offsets,
    int size,
    std::vector<int>& order)
{
    return [&trackers, &offsets, size, &order] (int value) {
        int i = std::ssize(order);
        for (int k = 0; k < std::ssize(offsets); ++k) {
            if (i + offsets[k] < size) {
                EXPECT_EQ(trackers[k].Frontier, i + offsets[k])
                    << "stage " << k << " frontier at body " << i;
            }
        }
        order.push_back(value);
    };
}

void VerifyBodyAndCoverage(const std::vector<TStageTracker>& trackers, const std::vector<int>& order, int size)
{
    ASSERT_EQ(std::ssize(order), size);
    for (int i = 0; i < size; ++i) {
        EXPECT_EQ(order[i], i) << "body visited elements out of order at " << i;
    }
    for (int k = 0; k < std::ssize(trackers); ++k) {
        EXPECT_EQ(trackers[k].Count, size) << "stage " << k << " Fn call count";
        EXPECT_EQ(std::ssize(trackers[k].Seen), size) << "stage " << k << " distinct elements";
    }
}

std::vector<int> Iota(int size)
{
    std::vector<int> items(size);
    std::iota(items.begin(), items.end(), 0);
    return items;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPrefetcherTest, EmptyContainer)
{
    auto items = Iota(0);
    int bodyCalls = 0;
    MakePrefetcher()
        .Add([] (int) {
            ADD_FAILURE() << "stage must not run on empty input";
        })
        .ForEach(items, [&] (int) {
            ++bodyCalls;
        });
    EXPECT_EQ(bodyCalls, 0);
}

TEST(TPrefetcherTest, NoStagesVisitsAllInOrder)
{
    auto items = Iota(100);
    std::vector<int> order;
    MakePrefetcher().ForEach(items, [&] (int value) {
        order.push_back(value);
    });
    ASSERT_EQ(std::ssize(order), 100);
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(order[i], i);
    }
}

TEST(TPrefetcherTest, SingleStageDefaultStride)
{
    constexpr int Size = 100;
    auto items = Iota(Size);
    std::vector<TStageTracker> trackers(1);
    std::vector<int> offsets = {static_cast<int>(DefaultPrefetchStride)}; // 16
    std::vector<int> order;

    MakePrefetcher()
        .Add(MakeStage(trackers, 0))
        .ForEach(items, MakeBody(trackers, offsets, Size, order));

    VerifyBodyAndCoverage(trackers, order, Size);
}

TEST(TPrefetcherTest, TwoStageExplicitDeltas)
{
    constexpr int Size = 100;
    auto items = Iota(Size);
    std::vector<TStageTracker> trackers(2);
    // Add(10) then Add(5): the last-added (inner) sits at 5, the earlier (outer) 10 further = 15.
    std::vector<int> offsets = {15, 5};
    std::vector<int> order;

    MakePrefetcher()
        .Add(10, MakeStage(trackers, 0))
        .Add(5, MakeStage(trackers, 1))
        .ForEach(items, MakeBody(trackers, offsets, Size, order));

    VerifyBodyAndCoverage(trackers, order, Size);
}

TEST(TPrefetcherTest, ThreeStageDefaultStrideOffsets)
{
    constexpr int Size = 200;
    auto items = Iota(Size);
    std::vector<TStageTracker> trackers(3);
    // Three default strides → reverse-cumsum 48, 32, 16.
    std::vector<int> offsets = {48, 32, 16};
    std::vector<int> order;

    MakePrefetcher()
        .Add(MakeStage(trackers, 0))
        .Add(MakeStage(trackers, 1))
        .Add(MakeStage(trackers, 2))
        .ForEach(items, MakeBody(trackers, offsets, Size, order));

    VerifyBodyAndCoverage(trackers, order, Size);
}

TEST(TPrefetcherTest, ContainerSmallerThanMaxOffset)
{
    // Size below every offset: no main loop, everything runs in prologue/tail. Still visits each
    // element once and keeps body order; the frontier checks self-skip when the target is past end.
    constexpr int Size = 8;
    auto items = Iota(Size);
    std::vector<TStageTracker> trackers(3);
    std::vector<int> offsets = {48, 32, 16};
    std::vector<int> order;

    MakePrefetcher()
        .Add(MakeStage(trackers, 0))
        .Add(MakeStage(trackers, 1))
        .Add(MakeStage(trackers, 2))
        .ForEach(items, MakeBody(trackers, offsets, Size, order));

    VerifyBodyAndCoverage(trackers, order, Size);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
