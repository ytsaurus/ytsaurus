#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/controller/job_balancer_common.h>
#include <yt/yt/flow/library/cpp/controller/job_balancer_result.h>

namespace NYT::NFlow::NBalancer {
namespace {

////////////////////////////////////////////////////////////////////////////////

// Helpers to build actions concisely.

static TPartitionId P(int n)
{
    return TPartitionId(TGuid::FromString(Format("%08x-%08x-%08x-%08x", 0, 0, 0, n)));
}

static TRebalanceResultAction Add(TPartitionId partitionId, std::string worker)
{
    return TRebalanceResultAction{
        .Type = ERebalanceActionType::Add,
        .PartitionId = partitionId,
        .WorkerAddress = std::move(worker),
    };
}

static TRebalanceResultAction Del(TPartitionId partitionId, std::string worker)
{
    return TRebalanceResultAction{
        .Type = ERebalanceActionType::Del,
        .PartitionId = partitionId,
        .WorkerAddress = std::move(worker),
    };
}

// Compare two action vectors element-by-element.
static void ExpectActions(
    const std::vector<TRebalanceResultAction>& actual,
    const std::vector<TRebalanceResultAction>& expected)
{
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i].Type, expected[i].Type) << "at index " << i;
        EXPECT_EQ(actual[i].PartitionId, expected[i].PartitionId) << "at index " << i;
        EXPECT_EQ(actual[i].WorkerAddress, expected[i].WorkerAddress) << "at index " << i;
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Empty input — no crash, empty output.
TEST(TCompactRebalanceActionsTest, EmptyInput)
{
    std::vector<TRebalanceResultAction> actions;
    CompactRebalanceActions(actions);
    EXPECT_TRUE(actions.empty());
}

//! Single Add — unchanged.
TEST(TCompactRebalanceActionsTest, SingleAdd)
{
    std::vector<TRebalanceResultAction> actions = {Add(P(1), "w1")};
    CompactRebalanceActions(actions);
    ExpectActions(actions, {Add(P(1), "w1")});
}

//! Single Del — unchanged (no preceding Add to cancel).
TEST(TCompactRebalanceActionsTest, SingleDel)
{
    std::vector<TRebalanceResultAction> actions = {Del(P(1), "w1")};
    CompactRebalanceActions(actions);
    ExpectActions(actions, {Del(P(1), "w1")});
}

//! Add then Del for the same partition on the same worker — both removed.
TEST(TCompactRebalanceActionsTest, AddThenDelSameWorkerCancelled)
{
    std::vector<TRebalanceResultAction> actions = {
        Add(P(1), "w1"),
        Del(P(1), "w1"),
    };
    CompactRebalanceActions(actions);
    EXPECT_TRUE(actions.empty());
}

//! Add to wA, Del from wA, Add to wB — collapses to just Add to wB.
TEST(TCompactRebalanceActionsTest, AddDelAddCollapsesToFinalAdd)
{
    std::vector<TRebalanceResultAction> actions = {
        Add(P(1), "w1"),
        Del(P(1), "w1"),
        Add(P(1), "w2"),
    };
    CompactRebalanceActions(actions);
    ExpectActions(actions, {Add(P(1), "w2")});
}

//! Del from wA (no preceding Add) followed by Add to wB — Del is kept.
TEST(TCompactRebalanceActionsTest, DelWithNoPrecedingAddIsKept)
{
    std::vector<TRebalanceResultAction> actions = {
        Del(P(1), "w1"),
        Add(P(1), "w2"),
    };
    CompactRebalanceActions(actions);
    ExpectActions(actions, {
            Del(P(1), "w1"),
            Add(P(1), "w2"),
                           });
}

//! Del from wB does NOT cancel an Add to wA (different workers).
TEST(TCompactRebalanceActionsTest, DelDifferentWorkerDoesNotCancelAdd)
{
    std::vector<TRebalanceResultAction> actions = {
        Add(P(1), "w1"),
        Del(P(1), "w2"),
    };
    CompactRebalanceActions(actions);
    ExpectActions(actions, {
            Add(P(1), "w1"),
            Del(P(1), "w2"),
                           });
}

//! Multiple independent partitions — each pair cancelled independently.
TEST(TCompactRebalanceActionsTest, MultiplePartitionsEachCancelledIndependently)
{
    std::vector<TRebalanceResultAction> actions = {
        Add(P(1), "w1"),
        Add(P(2), "w1"),
        Del(P(1), "w1"),
        Del(P(2), "w1"),
    };
    CompactRebalanceActions(actions);
    EXPECT_TRUE(actions.empty());
}

//! Only one of two partitions is cancelled; the other survives.
TEST(TCompactRebalanceActionsTest, OnlyMatchingPairCancelled)
{
    std::vector<TRebalanceResultAction> actions = {
        Add(P(1), "w1"),
        Add(P(2), "w1"),
        Del(P(1), "w1"),
        // No Del for P(2).
    };
    CompactRebalanceActions(actions);
    ExpectActions(actions, {Add(P(2), "w1")});
}

//! Typical equalization scenario:
//! Step 6 assigns P(1) to wA (Add), Step 8 moves it to wB (Del wA + Add wB).
//! Result: only Add(P(1), wB) survives.
TEST(TCompactRebalanceActionsTest, TypicalEqualizationScenario)
{
    std::vector<TRebalanceResultAction> actions = {
        // Step 6: assign stray partitions.
        Add(P(1), "w1"),
        Add(P(2), "w2"),
        // Step 8: move P(1) from w1 to w2.
        Del(P(1), "w1"),
        Add(P(1), "w2"),
    };
    CompactRebalanceActions(actions);
    ExpectActions(actions, {
            Add(P(2), "w2"),
            Add(P(1), "w2"),
                           });
}

//! Two partitions swapped between two workers.
//! P(1): Add(w1) → Del(w1) + Add(w2)  →  Add(w2)
//! P(2): Add(w2) → Del(w2) + Add(w1)  →  Add(w1)
TEST(TCompactRebalanceActionsTest, TwoPartitionsSwapped)
{
    std::vector<TRebalanceResultAction> actions = {
        Add(P(1), "w1"),
        Add(P(2), "w2"),
        Del(P(1), "w1"),
        Add(P(1), "w2"),
        Del(P(2), "w2"),
        Add(P(2), "w1"),
    };
    CompactRebalanceActions(actions);
    ExpectActions(actions, {
            Add(P(1), "w2"),
            Add(P(2), "w1"),
                           });
}

//! Actions for unrelated partitions are not disturbed.
TEST(TCompactRebalanceActionsTest, UnrelatedActionsUntouched)
{
    std::vector<TRebalanceResultAction> actions = {
        Del(P(10), "w1"), // Pre-existing Del (no Add before it).
        Add(P(1), "w1"),
        Add(P(2), "w2"),
        Del(P(1), "w1"), // cancels Add(P(1), w1)
        Add(P(3), "w3"),
    };
    CompactRebalanceActions(actions);
    ExpectActions(actions, {
            Del(P(10), "w1"),
            Add(P(2), "w2"),
            Add(P(3), "w3"),
                           });
}

//! Add then Del then Add again for the same partition on the same worker.
//! First Add+Del cancel; second Add survives.
TEST(TCompactRebalanceActionsTest, AddDelAddSameWorkerSecondAddSurvives)
{
    std::vector<TRebalanceResultAction> actions = {
        Add(P(1), "w1"),
        Del(P(1), "w1"),
        Add(P(1), "w1"),
    };
    CompactRebalanceActions(actions);
    ExpectActions(actions, {Add(P(1), "w1")});
}

//! All actions are Dels with no preceding Adds — all kept as-is.
TEST(TCompactRebalanceActionsTest, AllDelsNoAdds)
{
    std::vector<TRebalanceResultAction> actions = {
        Del(P(1), "w1"),
        Del(P(2), "w2"),
        Del(P(3), "w3"),
    };
    CompactRebalanceActions(actions);
    ExpectActions(actions, {
            Del(P(1), "w1"),
            Del(P(2), "w2"),
            Del(P(3), "w3"),
                           });
}

//! All actions are Adds with no Dels — all kept as-is.
TEST(TCompactRebalanceActionsTest, AllAddsNoDels)
{
    std::vector<TRebalanceResultAction> actions = {
        Add(P(1), "w1"),
        Add(P(2), "w2"),
        Add(P(3), "w3"),
    };
    CompactRebalanceActions(actions);
    ExpectActions(actions, {
            Add(P(1), "w1"),
            Add(P(2), "w2"),
            Add(P(3), "w3"),
                           });
}

//! A Del cancels the most recent Add for that partition, not an earlier one.
//! Add(P1, w1), Add(P1, w2), Del(P1, w2) → Add(P1, w1) survives.
//! (The second Add overwrites the tracked index; Del cancels the second Add.)
TEST(TCompactRebalanceActionsTest, DelCancelsMostRecentAdd)
{
    std::vector<TRebalanceResultAction> actions = {
        Add(P(1), "w1"),
        Add(P(1), "w2"), // Overwrites tracked index for P(1).
        Del(P(1), "w2"), // Cancels the second Add.
    };
    CompactRebalanceActions(actions);
    ExpectActions(actions, {Add(P(1), "w1")});
}

//! Large sequence: many partitions, some cancelled, some not.
TEST(TCompactRebalanceActionsTest, LargeSequenceMixedCancellations)
{
    // P(1..5) added to w1, then P(2) and P(4) moved to w2.
    std::vector<TRebalanceResultAction> actions;
    for (int i = 1; i <= 5; ++i) {
        actions.push_back(Add(P(i), "w1"));
    }
    actions.push_back(Del(P(2), "w1"));
    actions.push_back(Add(P(2), "w2"));
    actions.push_back(Del(P(4), "w1"));
    actions.push_back(Add(P(4), "w2"));

    CompactRebalanceActions(actions);

    // Expected: P(1), P(3), P(5) on w1; P(2), P(4) on w2.
    // Order: Add(P1,w1), Add(P3,w1), Add(P5,w1), Add(P2,w2), Add(P4,w2).
    ASSERT_EQ(actions.size(), 5u);

    THashMap<TPartitionId, std::string> addMap;
    for (const auto& a : actions) {
        EXPECT_EQ(a.Type, ERebalanceActionType::Add);
        addMap[a.PartitionId] = a.WorkerAddress;
    }
    EXPECT_EQ(addMap[P(1)], "w1");
    EXPECT_EQ(addMap[P(2)], "w2");
    EXPECT_EQ(addMap[P(3)], "w1");
    EXPECT_EQ(addMap[P(4)], "w2");
    EXPECT_EQ(addMap[P(5)], "w1");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NBalancer
