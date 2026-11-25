#include <yt/yt/flow/library/cpp/misc/cow_tree.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/misc/guid.h>

#include <util/str_stl.h>

#include <iterator>
#include <map>
#include <memory>
#include <random>
#include <ranges>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace NYT;
using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////

using TTestTree = TCowTree<TCowTreeDefaultConfig<ui64, 16>>;
using TTestMap = TCowTree<TCowTreeDefaultConfig<ui64, 16, std::less<ui64>, ui64>>;

// MaxHeight is derived at compile time from MaxSize and the minimal node fill.
static_assert(TCowTree<TCowTreeDefaultConfig<ui64, 256>>::MaxHeight == 6);
static_assert(TCowTree<TCowTreeDefaultConfig<ui64, 128>>::MaxHeight == 6);
static_assert(TCowTree<TCowTreeDefaultConfig<ui64, 16>>::MaxHeight == 11);

// The facades take a behavioural config; the default is concurrent with B = 64.
static_assert(TCowSet<ui64>::MaxNodeSize == 64);
static_assert(TCowSet<ui64, std::less<ui64>, TCowTreeStConfig<128>>::MaxNodeSize == 128);

// The linear-tail policy follows the absl-style default: cheap arithmetic
// comparisons get the tail, everything else gets pure binary search — unless the
// comparator or the key opts in, or the config pins an explicit value.
struct TExpensiveLess
{
    bool operator()(ui64 lhs, ui64 rhs) const
    {
        return lhs < rhs;
    }
};

struct TOptInLess
    : TExpensiveLess
{
    static constexpr bool PrefersLinearSearch = true;
};

struct TSpeculativeLess
    : TExpensiveLess
{
    static constexpr ECowTreeSearch ReadSearch = ECowTreeSearch::BranchyBinary;
};

static_assert(TCowSet<ui64>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowSet<ui64, std::greater<ui64>>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowSet<ui64, TExpensiveLess>::WriteSearch == ECowTreeSearch::BranchyBinary);
static_assert(TCowSet<ui64, TOptInLess>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowSet<ui64>::ReadSearch == ECowTreeSearch::BranchlessBinary);
static_assert(TCowSet<ui64, TSpeculativeLess>::ReadSearch == ECowTreeSearch::BranchyBinary);
static_assert(TCowSet<ui64, TSpeculativeLess>::WriteSearch == ECowTreeSearch::BranchyBinary);
static_assert(TCowTree<TCowTreeConfig<TCowTreeStConfig<128, 8>, ui64>>::LinearSearchLimit == 8);

// The default linear tail spans the whole node up to B = 64 and stays 32 beyond.
static_assert(TCowSet<ui64>::LinearSearchLimit == 64);
static_assert(TCowSet<ui64, std::less<ui64>, TCowTreeStConfig<32>>::LinearSearchLimit == 32);
static_assert(TCowSet<ui64, std::less<ui64>, TCowTreeStConfig<128>>::LinearSearchLimit == 32);

// The structural linear-friendly rule: small trivially copyable keys under
// std::less qualify (TGuid, raw pointers) with no per-type knowledge here;
// util's TLess/TGreater count as thin wrappers derived from the std functors.
static_assert(sizeof(TGuid) == 16 && std::is_trivially_copyable_v<TGuid>);
static_assert(TCowSet<TGuid>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowSet<ui64, ::TLess<ui64>>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowSet<ui64, ::TGreater<ui64>>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowSet<TGuid, ::TLess<TGuid>>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowSet<TString, ::TLess<TString>>::WriteSearch == ECowTreeSearch::BranchyBinary);
static_assert(TCowSet<ui64, std::less<>>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowSet<TGuid, std::less<>>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowSet<ui64, std::greater<>>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowSet<std::string, std::less<>>::WriteSearch == ECowTreeSearch::BranchyBinary);
static_assert(TCowSet<const void*>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowMap<ui64, ui64>::WriteSearch == ECowTreeSearch::HybridLinear);
static_assert(TCowSet<std::string>::WriteSearch == ECowTreeSearch::BranchyBinary);
static_assert(TCowSet<TGuid>::ReadSearch == ECowTreeSearch::BranchlessBinary);

// Opaque keys — a pointer or a handle compared through the pointee, a string —
// read through the branchy halving like they write; pointers ordered by address
// (std::less) are trivially comparable and keep the branchless read.
struct TPointee
{
    ui64 Value;
};

struct TPointeeLess
{
    bool operator()(const TPointee* lhs, const TPointee* rhs) const
    {
        return lhs->Value < rhs->Value;
    }

    std::weak_ordering Compare(const TPointee* lhs, const TPointee* rhs) const
    {
        return lhs->Value <=> rhs->Value;
    }
};

struct THandle
{
    const TPointee* Ptr;

    const TPointee* Get() const
    {
        return Ptr;
    }
};

struct THandleLess
{
    bool operator()(const THandle& lhs, const THandle& rhs) const
    {
        return lhs.Get()->Value < rhs.Get()->Value;
    }
};

static_assert(TCowSet<const TPointee*, TPointeeLess>::ReadSearch == ECowTreeSearch::BranchyBinary);
static_assert(TCowSet<const TPointee*, TPointeeLess>::WriteSearch == ECowTreeSearch::BranchyBinary);
static_assert(TCowSet<THandle, THandleLess>::ReadSearch == ECowTreeSearch::BranchyBinary);
static_assert(TCowSet<std::string>::ReadSearch == ECowTreeSearch::BranchyBinary);
static_assert(TCowSet<const TPointee*>::ReadSearch == ECowTreeSearch::BranchlessBinary);
static_assert(TCowSet<const void*>::ReadSearch == ECowTreeSearch::BranchlessBinary);
static_assert(TCowSet<ui64, TExpensiveLess>::ReadSearch == ECowTreeSearch::BranchyBinary);
static_assert(TCowSet<ui64, TOptInLess>::ReadSearch == ECowTreeSearch::BranchlessBinary);

// The three-way lookup: spelled by the comparator, or implied by std::less over a
// key with <=>; never for trivially comparable keys.
static_assert(CowTreeComparesThreeWay<const TPointee*, TPointeeLess>());
static_assert(CowTreeComparesThreeWay<std::string, std::less<std::string>>());
static_assert(CowTreeComparesThreeWay<std::string, std::greater<std::string>>());
static_assert(CowTreeComparesThreeWay<std::string, std::less<>>());
static_assert(!CowTreeComparesThreeWay<THandle, THandleLess>());
static_assert(!CowTreeComparesThreeWay<ui64, std::less<ui64>>());
static_assert(!CowTreeComparesThreeWay<const TPointee*, std::less<const TPointee*>>());

////////////////////////////////////////////////////////////////////////////////

TEST(TCowTree, MapInputRangeAndMissingKey)
{
    TCowMap<int, std::string> map;
    auto entries = std::views::iota(1, 4) | std::views::transform([] (int key) {
        return std::pair(key, std::to_string(key));
    });
    map.insert(entries.begin(), entries.end());
    EXPECT_THROW(map.at(0), std::out_of_range);
    EXPECT_THROW(std::as_const(map).at(0), std::out_of_range);
    auto it = map.find(2);
    auto version = map.GetVersion();
    it->second = "two";
    EXPECT_GT(map.GetVersion(), version);
    EXPECT_EQ(std::as_const(map).at(2), "two");
}

TEST(TCowTree, DerivedComparatorControlsStringOrder)
{
    struct TReverse : std::less<std::string>
    {
        bool operator()(const std::string& lhs, const std::string& rhs) const
        {
            return lhs > rhs;
        }
    };

    TCowSet<std::string, TReverse> set;
    set.insert({"a", "c", "b"});
    EXPECT_EQ(*set.begin(), "c");
    EXPECT_TRUE(set.contains("b"));
    EXPECT_EQ(*set.lower_bound("bb"), "b");
}

TEST(TCowTree, Simple)
{
    {
        TTestTree testSet;
    }
    {
        TTestTree testSet;
        testSet.clear();
    }
    {
        TTestTree testSet;
        testSet.insert(0);
    }
    {
        TTestTree testSet;
        testSet.insert(0);
        testSet.erase(0);
    }
    {
        TTestTree testSet;
        testSet.insert(0);
        testSet.erase(0);
        testSet.insert(0);
    }
    {
        TTestTree testSet;
        testSet.insert(0);
        testSet.erase(0);
        testSet.insert(0);
        testSet.clear();
    }
}

TEST(TCowTree, Snapshot)
{
    constexpr int N = 1000;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i * 3);
    }

    auto snapshot = master.Snapshot();
    EXPECT_TRUE(master.IsMaster());
    EXPECT_FALSE(snapshot.IsMaster());
    EXPECT_EQ(snapshot.size(), master.size());
    for (int i = 0; i < N; ++i) {
        EXPECT_NE(snapshot.find(i * 3), snapshot.end());
        EXPECT_EQ(snapshot.find(i * 3 + 1), snapshot.end());
    }

    // A copy of a snapshot is another snapshot of the same version.
    // An intentional O(1) snapshot copy — the copy itself is under test.
    TTestTree::TSnapshot copy(snapshot); // NOLINT(performance-unnecessary-copy-initialization)
    EXPECT_FALSE(copy.IsMaster());
    EXPECT_EQ(copy.size(), N);
    EXPECT_NE(copy.find(0), copy.end());
    EXPECT_EQ(copy.find(1), copy.end());
}

TEST(TCowTree, SnapshotOfEmpty)
{
    TTestTree master;
    auto snapshot = master.Snapshot();
    EXPECT_TRUE(snapshot.empty());
    EXPECT_EQ(snapshot.find(42), snapshot.end());
}

TEST(TCowTree, SnapshotOutlivesMaster)
{
    constexpr int N = 1000;
    auto master = std::make_unique<TTestTree>();
    for (int i = 0; i < N; ++i) {
        master->insert(i);
    }
    auto snapshot = master->Snapshot();
    master.reset();
    EXPECT_EQ(snapshot.size(), N);
    for (int i = 0; i < N; ++i) {
        EXPECT_NE(snapshot.find(i), snapshot.end());
    }
    EXPECT_EQ(snapshot.find(N), snapshot.end());
}

TEST(TCowTree, MultipleSnapshots)
{
    TTestTree master;
    for (int i = 0; i < 100; ++i) {
        master.insert(i);
    }
    auto first = master.Snapshot();
    auto second = master.Snapshot();
    {
        auto third = master.Snapshot();
        EXPECT_EQ(third.size(), 100u);
    }
    EXPECT_EQ(first.size(), 100u);
    EXPECT_EQ(second.size(), 100u);
}

TEST(TCowTree, MutateAfterSnapshotDeath)
{
    TTestTree master;
    for (int i = 0; i < 100; ++i) {
        master.insert(i);
    }
    {
        auto snapshot = master.Snapshot();
        EXPECT_EQ(snapshot.size(), 100u);
    }
    // All snapshots are gone; the master may mutate in place again.
    for (int i = 100; i < 200; ++i) {
        master.insert(i);
    }
    for (int i = 0; i < 100; ++i) {
        master.erase(i);
    }
    master.SelfCheck();
    EXPECT_EQ(master.size(), 100u);
}

// A snapshot must keep its exact content while the master mutates through node
// splits: the master copies shared nodes instead of writing into them.
TEST(TCowTree, SnapshotSurvivesInserts)
{
    constexpr int N = 1000;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i * 2);
    }
    auto snapshot = master.Snapshot();

    for (int i = 0; i < N; ++i) {
        master.insert(i * 2 + 1);
    }
    master.SelfCheck();

    EXPECT_EQ(snapshot.size(), N);
    for (int i = 0; i < 2 * N; ++i) {
        EXPECT_EQ(snapshot.find(i) != snapshot.end(), i % 2 == 0);
        EXPECT_NE(master.find(i), master.end());
    }
}

// The same through erases: merges, redistributions and the root collapse must not
// disturb the snapshot either.
TEST(TCowTree, SnapshotSurvivesErases)
{
    constexpr int N = 1000;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i);
    }
    auto snapshot = master.Snapshot();

    for (int i = 0; i < N; ++i) {
        if (i % 10 != 0) {
            master.erase(i);
        }
    }
    master.SelfCheck();

    EXPECT_EQ(snapshot.size(), N);
    EXPECT_EQ(master.size(), N / 10);
    for (int i = 0; i < N; ++i) {
        EXPECT_NE(snapshot.find(i), snapshot.end());
        EXPECT_EQ(master.find(i) != master.end(), i % 10 == 0);
    }
}

TEST(TCowTree, SnapshotSurvivesClear)
{
    constexpr int N = 300;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i);
    }
    auto snapshot = master.Snapshot();

    master.clear();
    EXPECT_TRUE(master.empty());
    for (int i = 0; i < N; ++i) {
        master.insert(i * 3);
    }
    master.SelfCheck();

    EXPECT_EQ(snapshot.size(), N);
    for (int i = 0; i < N; ++i) {
        EXPECT_NE(snapshot.find(i), snapshot.end());
    }
}

TEST(TCowTree, SnapshotSurvivesMasterDrainToEmpty)
{
    constexpr int N = 500;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i);
    }
    auto snapshot = master.Snapshot();

    // Down to a single key and through EraseLast to an empty tree.
    for (int i = 0; i < N; ++i) {
        master.erase(i);
    }
    EXPECT_TRUE(master.empty());
    master.insert(N);
    master.SelfCheck();

    EXPECT_EQ(snapshot.size(), N);
    for (int i = 0; i < N; ++i) {
        EXPECT_NE(snapshot.find(i), snapshot.end());
    }
    EXPECT_EQ(snapshot.find(N), snapshot.end());
}

// Randomized interleaving of mutations and snapshots: every live snapshot is compared
// against the std::set state captured at its creation.
TEST(TCowTree, SnapshotStress)
{
    constexpr int KeyUniverse = 512;
    constexpr int Iterations = 20000;
    std::mt19937_64 rng(42);

    TTestTree master;
    std::set<ui64> reference;
    std::vector<std::pair<std::unique_ptr<TTestTree::TSnapshot>, std::set<ui64>>> snapshots;

    // The shared read-only base view serves masters and snapshots alike.
    auto checkSnapshot = [&] (const TTestTree::TBase& snapshot, const std::set<ui64>& expected) {
        ASSERT_EQ(snapshot.size(), expected.size());
        for (int key = 0; key < KeyUniverse; ++key) {
            ASSERT_EQ(snapshot.find(key) != snapshot.end(), expected.contains(key)) << "key " << key;
        }
        auto it = snapshot.begin();
        for (ui64 key : expected) {
            ASSERT_NE(it, snapshot.end());
            ASSERT_EQ(*it, key);
            ++it;
        }
        ASSERT_EQ(it, snapshot.end());
    };

    for (int iteration = 0; iteration < Iterations; ++iteration) {
        ui64 key = rng() % KeyUniverse;
        switch (rng() % 8) {
            case 0:
            case 1:
            case 2:
                EXPECT_EQ(master.insert(key).second, reference.insert(key).second);
                break;
            case 3:
            case 4:
            case 5:
                EXPECT_EQ(master.erase(key), reference.erase(key) > 0);
                break;
            case 6:
                if (snapshots.size() < 8) {
                    auto snapshot = master.Snapshot();
                    snapshots.emplace_back(std::make_unique<TTestTree::TSnapshot>(snapshot), reference);
                }
                break;
            case 7:
                if (!snapshots.empty()) {
                    size_t victim = rng() % snapshots.size();
                    checkSnapshot(*snapshots[victim].first, snapshots[victim].second);
                    snapshots.erase(snapshots.begin() + victim);
                }
                break;
        }
        if (iteration % 500 == 0) {
            master.SelfCheck();
            for (const auto& [snapshot, expected] : snapshots) {
                checkSnapshot(*snapshot, expected);
            }
        }
    }
    master.SelfCheck();
    for (const auto& [snapshot, expected] : snapshots) {
        checkSnapshot(*snapshot, expected);
    }
    checkSnapshot(master, reference);
}

// A master's iterator remembers its key and recovers when the leaf under it changes.
TEST(TCowTree, IteratorRecoversAfterMutations)
{
    TTestTree master;
    for (int i = 0; i < 100; ++i) {
        master.insert(i * 10);
    }
    auto it = master.find(500);
    EXPECT_EQ(*it, 500u);

    // Inserts all over the tree (with splits) do not disturb the iterator.
    for (int i = 0; i < 100; ++i) {
        master.insert(i * 10 + 1);
    }
    EXPECT_EQ(*it, 500u);
    ++it;
    EXPECT_EQ(*it, 501u);
    --it;
    EXPECT_EQ(*it, 500u);
}

TEST(TCowTree, IteratorRecoversAfterEraseOfCurrent)
{
    TTestTree master;
    for (int i = 0; i < 1000; ++i) {
        master.insert(i);
    }
    auto it = master.find(500);
    master.erase(500);

    // Erasing the current element leaves the iterator on the successor.
    EXPECT_EQ(*it, 501u);

    auto next = it;
    ++next;
    EXPECT_EQ(*next, 502u);

    auto prev = it;
    --prev;
    EXPECT_EQ(*prev, 499u);
}

// A copy-on-write eviction replaces the leaf; the master's iterators are repointed
// to the replacement and keep working on the current tree.
TEST(TCowTree, IteratorSurvivesCowEviction)
{
    TTestTree master;
    for (int i = 0; i < 100; ++i) {
        master.insert(i);
    }
    auto it = master.find(50);
    {
        auto snapshot = master.Snapshot();
        master.erase(51);
    }
    EXPECT_EQ(*it, 50u);
    ++it;
    EXPECT_EQ(*it, 52u);
}

TEST(TCowTree, IteratorAfterClear)
{
    TTestTree master;
    for (int i = 0; i < 100; ++i) {
        master.insert(i);
    }
    auto it = master.find(50);
    master.clear();

    // clear() evicts every element: the iterators become end() for good.
    EXPECT_EQ(it, master.end());

    for (int i = 0; i < 100; i += 2) {
        master.insert(i);
    }
    EXPECT_EQ(it, master.end());

    // Bidirectional decrement off end() still lands on the (new) last element.
    --it;
    EXPECT_EQ(*it, 98u);
}

// A forward sweep erasing every visited element behind the iterator.
TEST(TCowTree, IteratorSweepWithInterleavedErase)
{
    constexpr int N = 1000;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i);
    }
    ui64 expected = 0;
    auto it = master.begin();
    while (it != master.end()) {
        EXPECT_EQ(*it, expected);
        ++it;
        master.erase(expected);
        ++expected;
    }
    EXPECT_EQ(expected, N);
    EXPECT_TRUE(master.empty());
}

// Iterators keep denoting exactly their element through arbitrary mutations:
// splits, merges, copy-on-write evictions and clear-less churn all around them.
TEST(TCowTree, IteratorStabilityUnderChurn)
{
    constexpr int N = 3000;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i * 2);
    }

    // Park an iterator on every 100th element.
    std::vector<std::pair<ui64, TTestTree::iterator>> parked;
    for (int i = 0; i < N; i += 100) {
        parked.emplace_back(i * 2, master.find(i * 2));
    }

    std::mt19937_64 rng(42);
    std::optional<TTestTree::TSnapshot> snapshot;
    for (int round = 0; round < 20000; ++round) {
        ui64 v = rng() % (2 * N);
        if (v % 2 == 0 && v % 100 != 0) {
            // Churn non-parked even keys and odd fillers, forcing splits and merges.
            master.erase(v);
        } else {
            master.insert(v | 1);
        }
        if (round % 1000 == 0) {
            auto freshSnapshot = master.Snapshot();
            snapshot.emplace(freshSnapshot);
        }
        if (round % 500 == 0) {
            for (const auto& [key, it] : parked) {
                EXPECT_EQ(*it, key);
            }
        }
    }
    master.SelfCheck();
    for (auto& [key, it] : parked) {
        EXPECT_EQ(*it, key);
        // The neighborhood is walkable in both directions.
        auto next = it;
        ++next;
        if (next != master.end()) {
            EXPECT_GT(*next, key);
            --next;
            EXPECT_EQ(*next, key);
        }
    }
}

// erase(iterator) returns the successor, std-style.
TEST(TCowTree, EraseIterator)
{
    constexpr int N = 1000;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i);
    }

    auto it = master.erase(master.find(500));
    EXPECT_EQ(*it, 501u);
    EXPECT_FALSE(master.contains(500));

    // Mutations elsewhere do not disturb the iterator; erasing the current
    // element repositions it to the successor, which the next erase(it) takes.
    it = master.find(600);
    master.insert(N);
    it = master.erase(it);
    EXPECT_EQ(*it, 601u);
    it = master.find(700);
    master.erase(700);
    it = master.erase(it);
    EXPECT_EQ(*it, 702u);

    // Erasing the last element returns end().
    it = master.erase(std::prev(master.end()));
    EXPECT_EQ(it, master.end());
    master.SelfCheck();
}

// The by-reference erase flavours: EraseAdvance sweeps, EraseAt drops.
TEST(TCowTree, EraseByReference)
{
    constexpr int N = 5000;
    TTestTree master;
    std::set<ui64> reference;
    for (int i = 0; i < N; ++i) {
        master.insert(i);
        reference.insert(i);
    }

    // EraseAdvance leaves the iterator on the successor, so the filter loop
    // needs no assignment at all.
    auto it = master.begin();
    auto referenceIt = reference.begin();
    while (it != master.end()) {
        if (*it % 3 != 1) {
            master.EraseAdvance(it);
            referenceIt = reference.erase(referenceIt);
            if (it != master.end()) {
                EXPECT_EQ(*it, *referenceIt);
            }
        } else {
            ++it;
            ++referenceIt;
        }
    }
    EXPECT_EQ(referenceIt, reference.end());
    EXPECT_EQ(master.size(), reference.size());
    master.SelfCheck();

    // EraseAt drops the element and resets the iterator to end().
    ui64 victim = *master.begin();
    auto dropped = master.find(victim);
    master.EraseAt(dropped);
    EXPECT_EQ(dropped, master.end());
    EXPECT_FALSE(master.contains(victim));
    reference.erase(victim);
    master.SelfCheck();

    // Erasing the last element through either flavour ends at end().
    while (!master.empty()) {
        auto last = std::prev(master.end());
        master.EraseAdvance(last);
        EXPECT_EQ(last, master.end());
    }
    EXPECT_TRUE(master.empty());
    master.SelfCheck();
}

// Other iterators keep working across the by-reference erases.
TEST(TCowTree, EraseByReferenceKeepsOtherIterators)
{
    constexpr int N = 2000;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i);
    }
    auto watcher = master.find(1500);
    auto it = master.begin();
    while (it != master.end() && *it < 1000) {
        master.EraseAdvance(it);
    }
    EXPECT_EQ(*watcher, 1500u);
    auto victim = master.find(1499);
    master.EraseAt(victim);
    EXPECT_EQ(*watcher, 1500u);
    master.SelfCheck();
    EXPECT_EQ(master.size(), N - 1001);
}

// Hint inserts: the good hint takes the in-leaf fast path, any other hint
// falls back and stays correct; the iterator lands on the element either way.
TEST(TCowTree, InsertHint)
{
    constexpr int N = 2000;
    TTestTree master;
    std::set<ui64> reference;
    for (int i = 0; i < N; ++i) {
        master.insert(i * 4);
        reference.insert(i * 4);
    }

    // The merge pattern: ascending keys inserted off the advancing iterator.
    auto it = master.begin();
    for (int i = 0; i < N - 1; ++i) {
        ui64 key = i * 4 + 2;
        master.InsertHint(it, key);
        ASSERT_NE(it, master.end());
        EXPECT_EQ(*it, key);
        reference.insert(key);
    }
    master.SelfCheck();
    EXPECT_EQ(master.size(), reference.size());

    // A duplicate through the hint reparks the iterator, size intact.
    it = master.find(ui64{42});
    ASSERT_NE(it, master.end());
    auto hinted = master.insert(it, ui64{42});
    EXPECT_EQ(*hinted, 42u);
    EXPECT_EQ(master.size(), reference.size());

    // Wildly wrong hints and the end() hint stay correct via the fallback.
    it = master.begin();
    master.InsertHint(it, 3 * 4 * N);
    EXPECT_EQ(*it, 3 * 4 * N);
    reference.insert(3 * 4 * N);
    auto endHint = master.end();
    master.InsertHint(endHint, ui64{1});
    EXPECT_EQ(*endHint, 1u);
    reference.insert(1);
    master.SelfCheck();

    auto referenceIt = reference.begin();
    master.ForEach([&] (ui64 key) {
        EXPECT_EQ(key, *referenceIt++);
    });
}

TEST(TCowTree, EmplaceAndExtract)
{
    TTestTree set;
    EXPECT_TRUE(set.emplace(7).second);
    EXPECT_FALSE(set.emplace(7).second);
    for (int i = 0; i < 500; ++i) {
        set.insert(i * 2 + 100);
    }

    // ExtractAt takes the element out and resets the iterator to end().
    auto it = set.find(200);
    ui64 taken = set.ExtractAt(it);
    EXPECT_EQ(taken, 200u);
    EXPECT_EQ(it, set.end());
    EXPECT_FALSE(set.contains(200));

    // ExtractAdvance leaves the iterator on the successor; reinsertion undoes it.
    it = set.find(300);
    taken = set.ExtractAdvance(it);
    EXPECT_EQ(taken, 300u);
    EXPECT_EQ(*it, 302u);
    set.insert(taken);
    EXPECT_TRUE(set.contains(300));
    set.SelfCheck();

    // The map flavours move the value out.
    using TStringMap = TCowTree<TCowTreeDefaultConfig<ui64, 16, std::less<ui64>, std::string>>;
    TStringMap map;
    EXPECT_TRUE(map.emplace(1, "one").second);
    EXPECT_FALSE(map.emplace(1, "uno").second);
    map.emplace(2, "two");
    auto mapIt = map.find(1);
    auto extracted = map.ExtractAt(mapIt);
    EXPECT_EQ(extracted.first, 1u);
    EXPECT_EQ(extracted.second, "one");
    EXPECT_EQ(mapIt, map.end());
    EXPECT_FALSE(map.contains(1));
    EXPECT_EQ(map.at(2), "two");
    map.SelfCheck();
}

// std-style heterogeneous lookup through a transparent comparator: the stored
// key is a wrapper, the lookups go by the raw value.
struct TBoxed
{
    ui64 Value;
};

struct TBoxedLess
{
    using is_transparent = void;

    bool operator()(const TBoxed& lhs, const TBoxed& rhs) const
    {
        return lhs.Value < rhs.Value;
    }

    bool operator()(const TBoxed& lhs, ui64 rhs) const
    {
        return lhs.Value < rhs;
    }

    bool operator()(ui64 lhs, const TBoxed& rhs) const
    {
        return lhs < rhs.Value;
    }
};

TEST(TCowTree, HeterogeneousLookup)
{
    TCowSet<TBoxed, TBoxedLess, TCowTreeStConfig<16>> set;
    for (int i = 0; i < 1000; ++i) {
        set.insert(TBoxed{static_cast<ui64>(i * 2)});
    }

    EXPECT_TRUE(set.contains(ui64{200}));
    EXPECT_FALSE(set.contains(ui64{201}));
    EXPECT_EQ(set.count(ui64{200}), 1u);
    EXPECT_EQ(set.count(ui64{201}), 0u);

    auto it = set.find(ui64{200});
    ASSERT_NE(it, set.end());
    EXPECT_EQ(it->Value, 200u);
    EXPECT_EQ(set.find(ui64{201}), set.end());

    auto lowerBound = set.lower_bound(ui64{201});
    EXPECT_EQ(lowerBound->Value, 202u);
    auto upperBound = set.upper_bound(ui64{202});
    EXPECT_EQ(upperBound->Value, 204u);

    // The heterogeneous seeks produce full-fledged iterators.
    --lowerBound;
    EXPECT_EQ(lowerBound->Value, 200u);
    EXPECT_EQ(set.lower_bound(ui64{5000}), set.end());
}

// A comparator with state — a function pointer, a functor carrying data — goes
// into the master's constructor the std::set way; the snapshots inherit it.
struct TPair
{
    int Values[2];

    bool operator==(const TPair& other) const = default;
};

struct TPairFieldLess
{
    int Index;

    bool operator()(const TPair& lhs, const TPair& rhs) const
    {
        return lhs.Values[Index] < rhs.Values[Index];
    }
};

bool GreaterInt(int lhs, int rhs)
{
    return lhs > rhs;
}

TEST(TCowTree, StatefulComparator)
{
    constexpr int N = 1000;

    {
        TCowSet<int, bool (*)(int, int), TCowTreeStConfig<16>> set(&GreaterInt);
        for (int i = 0; i < N; ++i) {
            set.insert(i);
        }
        int expected = N - 1;
        for (int value : set) {
            EXPECT_EQ(value, expected--);
        }
        EXPECT_EQ(expected, -1);
        EXPECT_EQ(*set.lower_bound(500), 500);
        EXPECT_EQ(*set.upper_bound(500), 499);
        EXPECT_EQ(set.key_comp(), &GreaterInt);
    }

    {
        using TSet = TCowSet<TPair, TPairFieldLess, TCowTreeStConfig<16>>;
        TSet byFirst(TPairFieldLess{0});
        TSet bySecond(TPairFieldLess{1});
        std::vector<TPair> reference;
        for (int i = 0; i < N; ++i) {
            TPair pair{{i, N - 1 - i}};
            byFirst.insert(pair);
            bySecond.insert(pair);
            reference.push_back(pair);
        }
        auto snapshot = bySecond.Snapshot();

        auto expectOrder = [&] (const auto& tree, bool reversed) {
            size_t index = 0;
            for (const auto& pair : tree) {
                EXPECT_EQ(pair, reference[reversed ? N - 1 - index : index]);
                ++index;
            }
            EXPECT_EQ(index, reference.size());
        };
        expectOrder(byFirst, false);
        expectOrder(bySecond, true);
        expectOrder(snapshot, true);

        // Each side compares by its own field only.
        EXPECT_TRUE(byFirst.contains(TPair{{7, -1}}));
        EXPECT_FALSE(byFirst.contains(TPair{{N, 0}}));
        EXPECT_TRUE(bySecond.contains(TPair{{-1, 7}}));
        EXPECT_FALSE(bySecond.contains(TPair{{0, N}}));
        EXPECT_EQ(byFirst.find(TPair{{7, -1}})->Values[1], N - 8);
        EXPECT_EQ(snapshot.find(TPair{{-1, 7}})->Values[0], N - 8);
        EXPECT_EQ(byFirst.upper_bound(TPair{{7, 0}})->Values[0], 8);
        EXPECT_EQ(bySecond.upper_bound(TPair{{0, 7}})->Values[1], 8);

        // Mutations after the snapshot keep both comparators in force.
        EXPECT_EQ(bySecond.erase(TPair{{0, 500}}), 1u);
        EXPECT_FALSE(bySecond.contains(TPair{{0, 500}}));
        EXPECT_TRUE(snapshot.contains(TPair{{0, 500}}));
        EXPECT_EQ(bySecond.size(), reference.size() - 1);
        EXPECT_EQ(snapshot.size(), reference.size());
    }
}

// Pointer keys under a pointee comparator take the branchy halving on both
// descents and the three-way lookup on reads; the whole lookup API must agree
// with a std::set on every node size, separator hits included (B = 4 makes
// most keys separators at some level).
template <int MaxNodeSize>
void TestIndirectKeys()
{
    constexpr int N = 5000;
    std::vector<std::unique_ptr<TPointee>> storage;
    std::vector<const TPointee*> pointers;
    std::mt19937_64 rng(7);
    for (int i = 0; i < N; ++i) {
        storage.push_back(std::make_unique<TPointee>(TPointee{static_cast<ui64>(i * 3)}));
        pointers.push_back(storage.back().get());
    }
    std::shuffle(pointers.begin(), pointers.end(), rng);

    SCOPED_TRACE(MaxNodeSize);
    TCowSet<const TPointee*, TPointeeLess, TCowTreeStConfig<MaxNodeSize>> set;
    std::set<const TPointee*, TPointeeLess> reference;
    for (int i = 0; i < N; ++i) {
        // Keep the tree partially filled by erasing every third key as it goes.
        if (i % 3 == 2) {
            const auto* victim = pointers[i - 1];
            EXPECT_EQ(set.erase(victim), reference.erase(victim));
        }
        EXPECT_EQ(set.insert(pointers[i]).second, reference.insert(pointers[i]).second);
    }
    EXPECT_EQ(set.size(), reference.size());

    TPointee probe{0};
    for (int value = 0; value < N * 3 + 3; ++value) {
        SCOPED_TRACE(value);
        probe.Value = value;
        const auto* key = &probe;
        EXPECT_EQ(set.contains(key), reference.contains(key));
        auto it = set.find(key);
        auto referenceIt = reference.find(key);
        EXPECT_EQ(it == set.end(), referenceIt == reference.end());
        if (it != set.end()) {
            EXPECT_EQ((*it)->Value, (*referenceIt)->Value);
        }
        auto lower = set.lower_bound(key);
        auto referenceLower = reference.lower_bound(key);
        EXPECT_EQ(lower == set.end(), referenceLower == reference.end());
        if (lower != set.end()) {
            EXPECT_EQ((*lower)->Value, (*referenceLower)->Value);
        }
        auto upper = set.upper_bound(key);
        auto referenceUpper = reference.upper_bound(key);
        EXPECT_EQ(upper == set.end(), referenceUpper == reference.end());
        if (upper != set.end()) {
            EXPECT_EQ((*upper)->Value, (*referenceUpper)->Value);
        }
    }

    size_t index = 0;
    std::vector<const TPointee*> sorted(reference.begin(), reference.end());
    for (const auto* key : set) {
        EXPECT_EQ(key, sorted[index++]);
    }
    EXPECT_EQ(index, sorted.size());
}

TEST(TCowTree, IndirectKeys)
{
    TestIndirectKeys<4>();
    TestIndirectKeys<16>();
    TestIndirectKeys<64>();
    TestIndirectKeys<128>();
}

// String keys get the three-way lookup through <=>: every present key must be
// found (separator hits included), every absent one missed, and the map value
// reads must follow the same descent.
template <int MaxNodeSize>
void TestStringKeys()
{
    constexpr int N = 3000;
    std::vector<std::string> keys;
    for (int i = 0; i < N; ++i) {
        keys.push_back("key-with-a-long-common-prefix-" + std::to_string(i * 2));
    }
    std::mt19937_64 rng(11);
    std::shuffle(keys.begin(), keys.end(), rng);

    TCowMap<std::string, size_t, std::less<std::string>, TCowTreeStConfig<MaxNodeSize>> map;
    std::map<std::string, size_t> reference;
    for (int i = 0; i < N; ++i) {
        map.Insert(keys[i], i);
        reference.emplace(keys[i], i);
        if (i % 4 == 3) {
            EXPECT_EQ(map.erase(keys[i - 2]), reference.erase(keys[i - 2]));
        }
    }
    EXPECT_EQ(map.size(), reference.size());

    for (int i = 0; i < 2 * N; ++i) {
        const auto key = "key-with-a-long-common-prefix-" + std::to_string(i);
        const auto referenceIt = reference.find(key);
        EXPECT_EQ(map.contains(key), referenceIt != reference.end());
        auto it = std::as_const(map).find(key);
        EXPECT_EQ(it == map.end(), referenceIt == reference.end());
        if (it != map.end()) {
            EXPECT_EQ(it->first, referenceIt->first);
            EXPECT_EQ(it->second, referenceIt->second);
            EXPECT_EQ(std::as_const(map).at(key), referenceIt->second);
        }
        auto lower = std::as_const(map).lower_bound(key);
        auto referenceLower = reference.lower_bound(key);
        EXPECT_EQ(lower == map.end(), referenceLower == reference.end());
        if (lower != map.end()) {
            EXPECT_EQ(lower->first, referenceLower->first);
        }
    }
}

TEST(TCowTree, StringKeys)
{
    TestStringKeys<4>();
    TestStringKeys<16>();
    TestStringKeys<64>();
}

// Randomized by-reference erases against a std::set reference: hits leaf maxima,
// underfills and snapshot-shared leaves, with the separator invariants checked.
TEST(TCowTree, EraseByReferenceStress)
{
    constexpr int N = 4000;
    TCowTree<TCowTreeDefaultConfig<ui64, 16>> master;
    std::set<ui64> reference;
    std::mt19937_64 rng(42);
    std::optional<decltype(master.Snapshot())> snapshot;

    for (int i = 0; i < N; ++i) {
        ui64 key = rng() % (4 * N);
        master.insert(key);
        reference.insert(key);
    }

    for (int round = 0; round < 20000; ++round) {
        ui64 key = rng() % (4 * N);
        if (round % 3 == 0) {
            EXPECT_EQ(master.insert(key).second, reference.insert(key).second);
        } else {
            auto it = master.find(key);
            EXPECT_EQ(it == master.end(), reference.find(key) == reference.end());
            if (it != master.end()) {
                auto referenceIt = reference.erase(reference.find(key));
                if (round % 2 == 0) {
                    master.EraseAdvance(it);
                    if (referenceIt == reference.end()) {
                        EXPECT_EQ(it, master.end());
                    } else {
                        EXPECT_EQ(*it, *referenceIt);
                    }
                } else {
                    master.EraseAt(it);
                    EXPECT_EQ(it, master.end());
                }
            }
        }
        if (round % 2000 == 0) {
            auto freshSnapshot = master.Snapshot();
            snapshot.emplace(freshSnapshot);
            master.SelfCheck();
        }
    }
    master.SelfCheck();
    EXPECT_EQ(master.size(), reference.size());
    auto referenceIt = reference.begin();
    master.ForEach([&] (ui64 key) {
        EXPECT_EQ(key, *referenceIt++);
    });
}

// The `it = erase(it)` filter loop against the std::set reference: exercises the
// in-leaf fast path, the leaf-max and underflow fallbacks, and the merges they cause.
TEST(TCowTree, EraseIteratorFilterLoop)
{
    constexpr int N = 5000;
    TTestTree master;
    std::set<ui64> reference;
    for (int i = 0; i < N; ++i) {
        master.insert(i);
        reference.insert(i);
    }

    auto it = master.begin();
    auto referenceIt = reference.begin();
    while (it != master.end()) {
        if (*it % 3 != 1) {
            it = master.erase(it);
            referenceIt = reference.erase(referenceIt);
            if (it != master.end()) {
                EXPECT_EQ(*it, *referenceIt);
            }
        } else {
            ++it;
            ++referenceIt;
        }
    }
    EXPECT_EQ(referenceIt, reference.end());
    master.SelfCheck();

    EXPECT_EQ(master.size(), reference.size());
    auto checkIt = reference.begin();
    master.ForEach([&] (ui64 key) {
        EXPECT_EQ(key, *checkIt++);
    });
}

// erase(iterator) under a live snapshot goes through copy-on-write.
TEST(TCowTree, EraseIteratorCow)
{
    constexpr int N = 2000;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i);
    }
    auto snapshot = master.Snapshot();

    auto it = master.begin();
    while (it != master.end()) {
        it = master.erase(it);
        if (it != master.end()) {
            ++it;
        }
    }
    master.SelfCheck();

    EXPECT_EQ(master.size(), N / 2);
    EXPECT_EQ(snapshot.size(), N);
    for (int i = 0; i < N; ++i) {
        EXPECT_TRUE(snapshot.contains(i));
        EXPECT_EQ(master.contains(i), i % 2 == 1);
    }
}

// Draining the tree through `it = erase(it)` shrinks leaves through every
// slow-path flavour and ends at end().
TEST(TCowTree, EraseIteratorDrain)
{
    constexpr int N = 1500;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i * 7 % N);
    }
    auto it = master.begin();
    ui64 erased = 0;
    while (it != master.end()) {
        it = master.erase(it);
        ++erased;
    }
    EXPECT_EQ(erased, N);
    EXPECT_TRUE(master.empty());
    master.SelfCheck();
}

// Snapshots iterate their frozen state, in both directions, while the master mutates.
TEST(TCowTree, SnapshotIteration)
{
    constexpr int N = 1000;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i * 2);
    }
    auto snapshot = master.Snapshot();
    for (int i = 0; i < N; ++i) {
        master.insert(i * 2 + 1);
    }

    ui64 expected = 0;
    for (auto it = snapshot.begin(); it != snapshot.end(); ++it, expected += 2) {
        EXPECT_EQ(*it, expected);
    }
    EXPECT_EQ(expected, 2 * N);

    auto it = snapshot.end();
    for (int i = N; i-- > 0;) {
        --it;
        EXPECT_EQ(*it, i * 2);
    }
    EXPECT_EQ(it, snapshot.begin());
}

// The set iterator models the full C++20 concept; std algorithms apply.
static_assert(std::bidirectional_iterator<TTestTree::iterator>);

TEST(TCowTree, StdIteratorInterop)
{
    constexpr int N = 100;
    TTestTree set;
    for (int i = 0; i < N; ++i) {
        set.insert(i * 2);
    }

    EXPECT_EQ(*std::prev(set.end()), (N - 1) * 2);
    EXPECT_EQ(*std::prev(set.end(), 3), (N - 3) * 2);
    EXPECT_EQ(*std::next(set.begin()), 2u);
    EXPECT_EQ(std::distance(set.begin(), set.end()), static_cast<std::ptrdiff_t>(N));

    auto snapshot = set.Snapshot();
    EXPECT_EQ(*std::prev(snapshot.end()), (N - 1) * 2);
}

TEST(TCowTree, IteratorContract)
{
    constexpr int N = 1000;
    TTestTree testSet;
    for (int i = 0; i < N; ++i) {
        testSet.insert(i * 2);
    }

    // Pre-increment/decrement return a reference to the same iterator.
    auto it = testSet.begin();
    EXPECT_EQ(&(++it), &it);
    EXPECT_EQ(&(--it), &it);
    EXPECT_EQ(it, testSet.begin());

    // Post-increment returns the old position and advances.
    auto old = it++;
    EXPECT_EQ(*old, ui64{0});
    EXPECT_EQ(*it, ui64{2});
    auto same = it--;
    EXPECT_EQ(*same, ui64{2});
    EXPECT_EQ(*it, ui64{0});

    // Full backward pass from end() visits all elements in reverse order.
    auto backward = testSet.end();
    for (int i = N; i-- > 0;) {
        --backward;
        EXPECT_EQ(*backward, i * 2);
    }
    EXPECT_EQ(backward, testSet.begin());
}

TEST(TCowTree, StlLookupApi)
{
    TCowSet<ui64> set;
    for (int i = 0; i < 100; ++i) {
        set.insert(i * 10);
    }
    const auto& constSet = set;

    EXPECT_TRUE(constSet.contains(500));
    EXPECT_FALSE(constSet.contains(505));
    EXPECT_EQ(constSet.count(500), 1u);
    EXPECT_EQ(constSet.count(505), 0u);

    EXPECT_EQ(*constSet.lower_bound(495), 500u);
    EXPECT_EQ(*constSet.lower_bound(500), 500u);
    EXPECT_EQ(*constSet.upper_bound(500), 510u);
    EXPECT_EQ(constSet.lower_bound(991), constSet.end());
    EXPECT_EQ(constSet.upper_bound(990), constSet.end());
    EXPECT_EQ(*constSet.lower_bound(0), 0u);
    EXPECT_EQ(*constSet.upper_bound(0), 10u);

    EXPECT_NE(constSet.find(990), constSet.end());
    EXPECT_EQ(constSet.find(991), constSet.end());
    EXPECT_EQ(*constSet.begin(), 0u);
}

TEST(TCowTree, ForEach)
{
    constexpr int N = 1000;
    TTestTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i * 3);
    }
    auto snapshot = master.Snapshot();
    for (int i = 0; i < N; ++i) {
        master.erase(i * 3);
        master.insert(i * 3 + 1);
    }

    ui64 expected = 0;
    master.ForEach([&] (const ui64& key) {
        EXPECT_EQ(key, expected * 3 + 1);
        ++expected;
    });
    EXPECT_EQ(expected, N);

    expected = 0;
    snapshot.ForEach([&] (const ui64& key) {
        EXPECT_EQ(key, expected * 3);
        ++expected;
    });
    EXPECT_EQ(expected, N);
}

// Snapshots of the concurrent preset serve readers on other threads while the
// master keeps mutating.
TEST(TCowTree, ConcurrentSnapshotReaders)
{
    using TConcurrentTree = TCowTree<TCowTreeConcurrentConfig<ui64, 16>>;
    constexpr int N = 1000;

    TConcurrentTree master;
    for (int i = 0; i < N; ++i) {
        master.insert(i * 2);
    }

    std::vector<std::thread> readers;
    for (int reader = 0; reader < 4; ++reader) {
        auto snapshot = master.Snapshot();
        readers.emplace_back([tree = std::make_unique<TConcurrentTree::TSnapshot>(snapshot)] {
            for (int pass = 0; pass < 50; ++pass) {
                ui64 expected = 0;
                for (auto it = tree->begin(); it != tree->end(); ++it, expected += 2) {
                    ASSERT_EQ(*it, expected);
                }
                ASSERT_EQ(expected, 2 * N);
                ASSERT_TRUE(tree->contains(2 * (pass % N)));
                ASSERT_FALSE(tree->contains(2 * (pass % N) + 1));
            }
        });
    }

    for (int i = 0; i < N; ++i) {
        master.insert(i * 2 + 1);
        master.erase(i * 2);
    }
    for (auto& reader : readers) {
        reader.join();
    }

    master.SelfCheck();
    EXPECT_EQ(master.size(), N);
    for (int i = 0; i < N; ++i) {
        EXPECT_TRUE(master.contains(i * 2 + 1));
        EXPECT_FALSE(master.contains(i * 2));
    }
}

TEST(TCowTree, MapBasics)
{
    constexpr int N = 1000;
    TTestMap map;
    for (int i = 0; i < N; ++i) {
        EXPECT_TRUE(map.insert(i * 2, i * 10).second);
    }
    EXPECT_FALSE(map.insert(0, 555).second);
    EXPECT_EQ(map.at(0), 0u);
    EXPECT_EQ(map.size(), N);
    map.SelfCheck();

    EXPECT_EQ(map.at(10), 50u);
    EXPECT_EQ(std::as_const(map).at(10), 50u);
    EXPECT_TRUE(map.contains(10));
    EXPECT_FALSE(map.contains(11));

    auto it = map.find(10);
    EXPECT_NE(it, map.end());
    EXPECT_EQ((*it).first, 10u);
    EXPECT_EQ(it->second, 50u);
    ++it;
    EXPECT_EQ(it->first, 12u);
    EXPECT_EQ(it->second, 60u);
    --it;
    EXPECT_EQ(it->first, 10u);

    ui64 expected = 0;
    for (auto jt = map.begin(); jt != map.end(); ++jt, ++expected) {
        EXPECT_EQ(jt->first, expected * 2);
        EXPECT_EQ(jt->second, expected * 10);
    }
    EXPECT_EQ(expected, N);

    expected = 0;
    map.ForEach([&] (ui64 key, ui64 value) {
        EXPECT_EQ(key, expected * 2);
        EXPECT_EQ(value, expected * 10);
        ++expected;
    });
    EXPECT_EQ(expected, N);

    EXPECT_TRUE(map.erase(10));
    EXPECT_FALSE(map.erase(10));
    EXPECT_FALSE(map.contains(10));
    EXPECT_EQ(map.size(), N - 1);
    map.SelfCheck();
}

// Value writes go through copy-on-write: the master changes, snapshots do not.
TEST(TCowTree, MapProxyWriteCow)
{
    constexpr int N = 300;
    TTestMap map;
    for (int i = 0; i < N; ++i) {
        map.insert(i, i);
    }
    auto snapshot = map.Snapshot();

    auto it = map.find(100);
    it->second = ui64{1000};
    EXPECT_EQ(it->second, 1000u);
    map.at(101) = 1010;
    map[102] = ui64{1020};
    it = map.find(103);
    it->second += 900;
    map.SelfCheck();

    EXPECT_EQ(map.at(100), 1000u);
    EXPECT_EQ(map.at(101), 1010u);
    EXPECT_EQ(map.at(102), 1020u);
    EXPECT_EQ(map.at(103), 1003u);

    // The snapshot still sees the original values.
    for (int key = 100; key <= 103; ++key) {
        EXPECT_EQ(snapshot.at(key), key);
    }

    // Snapshot iterators read values but may not write.
    auto snapshotIt = snapshot.find(100);
    EXPECT_EQ(snapshotIt->second, 100u);
}

// operator[] follows the std::map contract: a missing key is inserted with a
// default-constructed value right at the call.
TEST(TCowTree, MapIndexOperator)
{
    TTestMap map;
    for (int i = 0; i < 100; ++i) {
        map.insert(i, i);
    }
    auto snapshot = map.Snapshot();

    map[50] = ui64{500};
    map[1000] = ui64{1};
    map[2000] += 7;
    map.SelfCheck();

    EXPECT_EQ(map[50], 500u);
    EXPECT_EQ(map[1000], 1u);
    EXPECT_EQ(map[2000], 7u);
    EXPECT_EQ(map.size(), 102u);

    // The bare call inserts the default value at once; an existing key is only read.
    map[3000];
    EXPECT_TRUE(map.contains(3000));
    EXPECT_EQ(map.at(3000), 0u);
    EXPECT_EQ(map[3000], 0u);
    EXPECT_EQ(map.size(), 103u);
    map.SelfCheck();

    // Snapshots read via at(); operator[] is master-only.
    EXPECT_EQ(snapshot.at(50), 50u);
    EXPECT_FALSE(snapshot.contains(1000));

    // operator[] hands out a plain mutable reference.
    ui64& reference = map[50];
    reference += 1;
    EXPECT_EQ(std::as_const(map).at(50), 501u);
}

// The master/snapshot split is enforced by the type system: the snapshot type
// carries no mutations, and const_iterator never converts back to iterator.
TEST(TCowTree, TypeSplitContract)
{
    using TSnapshot = TTestTree::TSnapshot;
    static_assert(!std::is_copy_constructible_v<TTestTree>);
    static_assert(!std::is_move_constructible_v<TTestTree>);
    static_assert(std::is_copy_constructible_v<TSnapshot>);
    static_assert(std::is_convertible_v<TTestTree::iterator, TTestTree::const_iterator>);
    static_assert(!std::is_constructible_v<TTestTree::iterator, TTestTree::const_iterator>);
    static_assert(std::is_same_v<decltype(std::declval<TTestTree&>().begin()), TTestTree::iterator>);
    static_assert(std::is_same_v<decltype(std::declval<const TTestTree&>().begin()), TTestTree::const_iterator>);
    static_assert(std::is_same_v<decltype(std::declval<TSnapshot&>().begin()), TTestTree::const_iterator>);

    // A mutable map iterator dereferences to a writable value; the write goes
    // copy-on-write against a live snapshot and the snapshot stays intact.
    TTestMap map;
    map.insert(1, ui64{10});
    auto it = map.find(1);
    it->second = 11;
    EXPECT_EQ(std::as_const(map).at(1), 11u);

    auto snapshot = map.Snapshot();
    map.find(1)->second += 5;
    EXPECT_EQ(std::as_const(map).at(1), 16u);
    EXPECT_EQ(snapshot.at(1), 11u);
    EXPECT_EQ(snapshot.find(1)->second, 11u);
    map.SelfCheck();

    // The registered iterator followed its element onto the private copy.
    EXPECT_EQ(it->second, 16u);
}

// emplace_hint, the proxy-to-value_type conversion of map iterators, and the
// half-open ForEachInRange.
TEST(TCowTree, EmplaceHintRangeAndProxyConversion)
{
    TTestTree set;
    auto it = set.end();
    for (int i = 0; i < 300; ++i) {
        it = set.emplace_hint(it, i * 2);
        ASSERT_EQ(*it, i * 2);
    }
    EXPECT_EQ(set.size(), 300u);
    EXPECT_EQ(*set.emplace_hint(set.find(100), 100), 100u);
    EXPECT_EQ(set.size(), 300u);
    set.SelfCheck();

    // ForEachInRange visits exactly [from, to), in order.
    std::vector<ui64> seen;
    set.ForEachInRange(101, 111, [&] (ui64 key) {
        seen.push_back(key);
    });
    EXPECT_EQ(seen, (std::vector<ui64>{102, 104, 106, 108, 110}));
    seen.clear();
    set.ForEachInRange(700, 700, [&] (ui64 key) {
        seen.push_back(key);
    });
    set.ForEachInRange(800, 100, [&] (ui64 key) {
        seen.push_back(key);
    });
    EXPECT_TRUE(seen.empty());
    ui64 total = 0;
    set.ForEachInRange(0, 1'000'000, [&] (ui64 key) {
        total += key;
    });
    EXPECT_EQ(total, 299u * 300u);

    TTestMap map;
    auto mapIt = map.end();
    for (int i = 0; i < 100; ++i) {
        mapIt = map.emplace_hint(mapIt, i, i * 3);
    }
    EXPECT_EQ(map.size(), 100u);
    EXPECT_EQ(std::as_const(map).at(42), 126u);

    // The dereference proxy converts to value_type: copying algorithms work.
    std::vector<std::pair<ui64, ui64>> copied;
    std::copy(map.begin(), map.end(), std::back_inserter(copied));
    ASSERT_EQ(copied.size(), 100u);
    EXPECT_EQ(copied[7], (std::pair<ui64, ui64>{7, 21}));

    // By-value structured bindings and auto&& over the proxy.
    ui64 keySum = 0;
    ui64 valueSum = 0;
    for (auto [key, value] : std::as_const(map)) {
        keySum += key;
        valueSum += value;
    }
    for (auto&& [key, value] : std::as_const(map)) {
        keySum += key;
    }
    EXPECT_EQ(keySum, 2u * 4950u);
    EXPECT_EQ(valueSum, 3u * 4950u);

    ui64 rangeValues = 0;
    map.ForEachInRange(10, 20, [&] (ui64 /*key*/, ui64 value) {
        rangeValues += value;
    });
    EXPECT_EQ(rangeValues, 3u * (10 + 19) * 10 / 2);
}

// BuildFromSorted packs a sorted stream into full nodes in O(n); the result
// must be a first-class tree: correct invariants, lookups, mutations and
// snapshots, at every remainder shape.
TEST(TCowTree, BuildFromSorted)
{
    for (int n : {0, 1, 2, 15, 16, 17, 23, 24, 255, 256, 257, 4096, 10007}) {
        std::vector<ui64> keys(n);
        for (int i = 0; i < n; ++i) {
            keys[i] = i * 3 + 1;
        }
        TTestTree tree;
        tree.BuildFromSorted(keys.begin(), keys.end());
        tree.SelfCheck();
        ASSERT_EQ(tree.size(), n);

        auto it = tree.begin();
        for (int i = 0; i < n; ++i, ++it) {
            ASSERT_EQ(*it, keys[i]);
        }
        ASSERT_EQ(it, tree.end());
        if (n == 0) {
            continue;
        }
        ASSERT_TRUE(tree.contains(keys[n / 2]));
        ASSERT_FALSE(tree.contains(keys[n / 2] + 1));

        // The packed tree takes mutations: splits on insert, erases, snapshots.
        auto snapshot = tree.Snapshot();
        for (int i = 0; i < n; i += 7) {
            ASSERT_TRUE(tree.Insert(keys[i] + 1));
        }
        for (int i = 0; i < n; i += 5) {
            tree.erase(keys[i]);
        }
        tree.SelfCheck();
        ASSERT_EQ(snapshot.size(), n);
        ASSERT_EQ(*snapshot.begin(), keys[0]);
    }

    // A map builds from pair-likes and keeps its values.
    std::vector<std::pair<ui64, ui64>> pairs;
    for (int i = 0; i < 1000; ++i) {
        pairs.emplace_back(i * 2, i * 7);
    }
    TTestMap map;
    map.BuildFromSorted(pairs.begin(), pairs.end());
    map.SelfCheck();
    EXPECT_EQ(map.size(), 1000u);
    EXPECT_EQ(std::as_const(map).at(84), 294u);

    // The flow pattern: rebuilding a fresh master straight from a snapshot.
    auto mapSnapshot = map.Snapshot();
    TTestMap rebuilt;
    rebuilt.BuildFromSorted(mapSnapshot.begin(), mapSnapshot.end());
    rebuilt.SelfCheck();
    EXPECT_TRUE(rebuilt == mapSnapshot);
    rebuilt.insert_or_assign(84, ui64{0});
    EXPECT_TRUE(rebuilt != mapSnapshot);
    EXPECT_EQ(mapSnapshot.at(84), 294u);
}

// The value-writing operations run a single descent and keep their std
// guarantees: try_emplace does not construct the value (nor consume the args)
// when the key is already there, insert_or_assign overwrites through
// copy-on-write, and both hand back the right iterator.
struct TCountedValue
{
    static inline int Constructions = 0;

    ui64 Payload = 0;

    TCountedValue() = default;

    explicit TCountedValue(ui64 payload)
        : Payload(payload)
    {
        ++Constructions;
    }

    TCountedValue(const TCountedValue& other) = default;
    TCountedValue& operator=(const TCountedValue& other) = default;
};

TEST(TCowTree, ValueWritingOperations)
{
    using TCounted = TCountedValue;
    using TCountedMap = TCowTree<TCowTreeDefaultConfig<ui64, 16, std::less<ui64>, TCounted>>;

    TCountedMap map;
    for (int i = 0; i < 200; ++i) {
        auto [it, emplaced] = map.try_emplace(i, i * 10);
        ASSERT_TRUE(emplaced);
        ASSERT_EQ(it->first, i);
        ASSERT_EQ(it->second.Payload, i * 10);
    }
    EXPECT_EQ(TCounted::Constructions, 200);
    map.SelfCheck();

    // The key is there: no value is constructed and the iterator points at the
    // preexisting element.
    for (int i = 0; i < 200; ++i) {
        auto [it, emplaced] = map.try_emplace(i, i * 777);
        ASSERT_FALSE(emplaced);
        ASSERT_EQ(it->second.Payload, i * 10);
    }
    EXPECT_EQ(TCounted::Constructions, 200);

    // A moved-from argument survives a duplicate try_emplace untouched.
    using TStringMap = TCowTree<TCowTreeDefaultConfig<ui64, 16, std::less<ui64>, std::string>>;
    TStringMap strings;
    strings.insert(1, "one");
    std::string spare = "spare";
    auto [kept, emplaced] = strings.try_emplace(1, std::move(spare));
    EXPECT_FALSE(emplaced);
    EXPECT_EQ(kept->second, "one");
    EXPECT_EQ(spare, "spare");
    EXPECT_TRUE(strings.try_emplace(2, std::move(spare)).second);
    EXPECT_EQ(strings.at(2), "spare");

    // insert_or_assign and operator[] write through copy-on-write: the snapshot
    // taken before them keeps the old values.
    TTestMap map2;
    for (int i = 0; i < 500; ++i) {
        map2.insert(i, i);
    }
    auto snapshot = map2.Snapshot();
    for (int i = 0; i < 500; i += 2) {
        auto [it, inserted] = map2.insert_or_assign(i, i + 1000);
        ASSERT_FALSE(inserted);
        ASSERT_EQ(it->first, i);
        ASSERT_EQ(it->second, i + 1000);
    }
    for (int i = 500; i < 600; ++i) {
        auto [it, inserted] = map2.insert_or_assign(i, i + 1000);
        ASSERT_TRUE(inserted);
        ASSERT_EQ(it->first, i);
    }
    map2[600] = ui64{1600};
    map2[0] += 1;
    map2.SelfCheck();

    EXPECT_EQ(map2.size(), 601u);
    EXPECT_EQ(std::as_const(map2).at(0), 1001u);
    EXPECT_EQ(std::as_const(map2).at(2), 1002u);
    EXPECT_EQ(std::as_const(map2).at(3), 3u);
    EXPECT_EQ(std::as_const(map2).at(550), 1550u);
    EXPECT_EQ(std::as_const(map2).at(600), 1600u);
    EXPECT_EQ(snapshot.size(), 500u);
    for (int i = 0; i < 500; ++i) {
        ASSERT_EQ(snapshot.at(i), i);
    }
}

// The std insert harvests its returned iterator from the insert descent
// itself; the position must be exact through every branch — plain, shared-leaf
// CoW, group redistributes, splits and cascading root growth — and the
// registered iterator must survive the cascade that follows its creation.
TEST(TCowTree, InsertReturnsPosition)
{
    TTestTree set;
    std::set<ui64> reference;
    std::mt19937_64 rng(7);
    std::optional<TTestTree::TSnapshot> snapshot;
    for (int i = 0; i < 20000; ++i) {
        ui64 key = rng() % 4096;
        auto [it, inserted] = set.insert(key);
        ASSERT_EQ(*it, key);
        ASSERT_EQ(inserted, reference.insert(key).second);
        if (i % 512 == 0) {
            // Live snapshots force the copy-on-write insert branches.
            auto fresh = set.Snapshot();
            snapshot.emplace(fresh);
        }
        auto next = it;
        ++next;
        auto expectedNext = reference.upper_bound(key);
        if (expectedNext == reference.end()) {
            ASSERT_EQ(next, set.end());
        } else {
            ASSERT_EQ(*next, *expectedNext);
        }
    }
    set.SelfCheck();
}

// The std-compat surface: list/range construction and inserts, reverse
// iteration, comparators, equality and the insert_or_assign/try_emplace pair.
TEST(TCowTree, StdCompatSurface)
{
    TTestTree set{5, 1, 3, 1};
    EXPECT_EQ(set.size(), 3u);
    set.insert({2, 4, 2});
    std::vector<ui64> tail{6, 0};
    set.insert(tail.begin(), tail.end());
    set.SelfCheck();

    std::vector<ui64> forward(set.begin(), set.end());
    EXPECT_EQ(forward, (std::vector<ui64>{0, 1, 2, 3, 4, 5, 6}));
    std::vector<ui64> backward(set.rbegin(), set.rend());
    std::reverse(backward.begin(), backward.end());
    EXPECT_EQ(forward, backward);

    EXPECT_TRUE(set.key_comp()(1, 2));
    EXPECT_TRUE(set.value_comp()(1, 2));

    // insert is std-shaped; Insert is the fast bool form.
    auto [inserted7, ok7] = set.insert(7);
    EXPECT_TRUE(ok7);
    EXPECT_EQ(*inserted7, 7u);
    EXPECT_FALSE(set.insert(7).second);
    EXPECT_TRUE(set.Insert(8));
    EXPECT_FALSE(set.Insert(8));
    set.erase(7);
    set.erase(8);

    // erase(key) counts the erased elements, std style.
    EXPECT_EQ(set.erase(3), 1u);
    EXPECT_EQ(set.erase(3), 0u);

    TTestTree same{0, 1, 2, 4, 5, 6};
    EXPECT_TRUE(set == same);
    same.erase(0);
    EXPECT_TRUE(set != same);

    TTestMap map{{1, 10}, {2, 20}};
    EXPECT_TRUE(map.value_comp()({1, 10}, {2, 0}));

    auto [assigned, inserted] = map.insert_or_assign(3, ui64{30});
    EXPECT_TRUE(inserted);
    EXPECT_EQ(assigned->first, 3u);
    std::tie(assigned, inserted) = map.insert_or_assign(3, ui64{33});
    EXPECT_FALSE(inserted);
    EXPECT_EQ(std::as_const(map).at(3), 33u);

    auto [tried, emplaced] = map.try_emplace(4, ui64{40});
    EXPECT_TRUE(emplaced);
    std::tie(tried, emplaced) = map.try_emplace(4, ui64{99});
    EXPECT_FALSE(emplaced);
    EXPECT_EQ(tried->first, 4u);
    EXPECT_EQ(tried->second, 40u);

    TTestMap sameMap{{1, 10}, {2, 20}, {3, 33}, {4, 40}};
    EXPECT_TRUE(map == sameMap);
    sameMap.insert_or_assign(4, ui64{41});
    EXPECT_TRUE(map != sameMap);
    map.SelfCheck();

    // Shared roots short-circuit the comparison; a mutation forks the root and
    // the comparison falls back to (and passes) the element-wise walk.
    auto snapshotA = map.Snapshot();
    auto snapshotB = map.Snapshot();
    EXPECT_TRUE(snapshotA == map);
    EXPECT_TRUE(snapshotA == snapshotB);
    map.insert_or_assign(1, ui64{10});
    EXPECT_TRUE(snapshotA == map);
    map.insert_or_assign(1, ui64{11});
    EXPECT_TRUE(snapshotA != map);
    EXPECT_TRUE(snapshotA == snapshotB);
}

// Erasing the element under an iterator redirects both reads and writes to the
// element the iterator recovers to.
TEST(TCowTree, MapIteratorRecovery)
{
    TTestMap map;
    for (int i = 0; i < 1000; ++i) {
        map.insert(i, i);
    }
    auto it = map.find(500);
    map.erase(500);
    EXPECT_EQ(it->first, 501u);
    EXPECT_EQ(it->second, 501u);

    auto other = map.find(600);
    map.erase(600);
    other->second = ui64{42};
    EXPECT_EQ(map.at(601), 42u);
    map.SelfCheck();
}

// erase(iterator) on a map returns the successor with a working value handle.
TEST(TCowTree, MapEraseIterator)
{
    constexpr int N = 500;
    TTestMap map;
    for (int i = 0; i < N; ++i) {
        map.insert(i, i * 10);
    }

    auto it = map.erase(map.find(100));
    EXPECT_EQ(it->first, 101u);
    EXPECT_EQ(it->second, 1010u);
    it->second = ui64{7};
    EXPECT_EQ(map.at(101), 7u);

    // Erase every element with an even value through the iterator loop.
    it = map.begin();
    while (it != map.end()) {
        if (it->first % 2 == 0) {
            it = map.erase(it);
        } else {
            ++it;
        }
    }
    map.SelfCheck();
    EXPECT_EQ(map.size(), N / 2);
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(map.contains(i), i % 2 == 1);
    }
}

TEST(TCowTree, MapStringValues)
{
    using TStringMap = TCowTree<TCowTreeDefaultConfig<ui64, 16, std::less<ui64>, std::string>>;
    constexpr int N = 500;

    TStringMap map;
    for (int i = 0; i < N; ++i) {
        map.insert(i, "value-" + std::to_string(i));
    }
    auto snapshot = map.Snapshot();

    for (int i = 0; i < N; i += 3) {
        map.at(i) = "updated-" + std::to_string(i);
    }
    for (int i = N / 4; i < N / 2; ++i) {
        map.erase(i);
    }
    map.SelfCheck();

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(snapshot.at(i), "value-" + std::to_string(i));
        if (map.contains(i)) {
            EXPECT_EQ(map.at(i), (i % 3 == 0 ? "updated-" : "value-") + std::to_string(i));
        }
    }
}

// The single-threaded facade config: snapshots work, nothing is atomic.
TEST(TCowTree, SingleThreadFacadeConfig)
{
    TCowSet<ui64, std::less<ui64>, TCowTreeStConfig<64>> set;
    for (int i = 0; i < 1000; ++i) {
        set.insert(i * 2);
    }
    auto snapshot = set.Snapshot();
    for (int i = 0; i < 1000; ++i) {
        set.erase(i * 2);
        set.insert(i * 2 + 1);
    }
    set.SelfCheck();
    EXPECT_EQ(snapshot.size(), 1000u);
    for (int i = 0; i < 1000; ++i) {
        EXPECT_TRUE(snapshot.contains(i * 2));
        EXPECT_TRUE(set.contains(i * 2 + 1));
    }

    TCowMap<ui64, ui64, std::less<ui64>, TCowTreeStConfig<64>> map;
    map[7] = ui64{49};
    EXPECT_EQ(map.at(7), 49u);
}

// Exercises the pure-binary write descent that expensive comparators get.
TEST(TCowTree, ExpensiveComparatorSet)
{
    constexpr int N = 1000;
    TCowTree<TCowTreeConfig<TCowTreeStConfig<16>, ui64, TExpensiveLess>> set;
    for (int i = 0; i < N; ++i) {
        set.insert(i * 37 % N);
    }
    auto snapshot = set.Snapshot();
    for (int i = 0; i < N; i += 2) {
        set.erase(i);
    }
    set.SelfCheck();
    EXPECT_EQ(set.size(), N / 2);
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(set.contains(i), i % 2 == 1);
        EXPECT_TRUE(snapshot.contains(i));
    }
}

TEST(TCowTree, CowMapFacade)
{
    TCowMap<ui64, ui64> map;
    for (int i = 0; i < 100; ++i) {
        map.insert(i * 10, i);
    }
    map.insert(std::pair<const ui64, ui64>(1000, 100));
    EXPECT_TRUE(map.contains(1000));
    EXPECT_EQ(map.at(500), 50u);
    EXPECT_EQ(map.lower_bound(495)->first, 500u);
    EXPECT_EQ(map.upper_bound(500)->second, 51u);
    EXPECT_EQ(map.count(505), 0u);
}

// The map twin of InsertErase: values must travel with their keys through every
// split, redistribution and merge.
TEST(TCowTree, MapInsertErase)
{
    constexpr int N = 1024;
    auto generate = [] (auto sequenceType, auto i) {
        switch (sequenceType) {
            case 0:
                return i;
            case 1:
                return N - i - 1;
            case 2:
                return i % 2 ? i / 2 : N - i / 2 - 1;
            case 3:
                return i % 2 ? N / 2 - i / 2 - 1 : N / 2 + i / 2;
            default:
                YT_VERIFY(false);
        };
    };
    auto valueOf = [] (ui64 key) {
        return key * 7 + 1;
    };
    for (int insertSequenceType = 0; insertSequenceType < 4; insertSequenceType++) {
        for (int eraseSequenceType = 0; eraseSequenceType < 4; eraseSequenceType++) {
            TTestMap testMap;
            std::map<ui64, ui64> referenceMap;
            for (int i = 0; i < N; ++i) {
                auto v = generate(insertSequenceType, i);
                referenceMap[v] = valueOf(v);
                EXPECT_TRUE(testMap.insert(v, valueOf(v)).second);
                EXPECT_FALSE(testMap.insert(v, 0).second);
                EXPECT_EQ(testMap.size(), i + 1);

                auto it = testMap.begin();
                auto referenceIt = referenceMap.begin();
                for (int j = 0; j <= i; ++j, ++it, ++referenceIt) {
                    EXPECT_NE(it, testMap.end());
                    EXPECT_EQ(it->first, referenceIt->first);
                    EXPECT_EQ(it->second, referenceIt->second);
                }
                EXPECT_EQ(it, testMap.end());
                testMap.SelfCheck();
            }
            for (int i = 0; i < N; ++i) {
                auto v = generate(eraseSequenceType, i);
                EXPECT_EQ(testMap.at(v), valueOf(v));
                referenceMap.erase(v);
                EXPECT_TRUE(testMap.erase(v));
                EXPECT_FALSE(testMap.erase(v));
                EXPECT_EQ(testMap.size(), N - i - 1);

                auto it = testMap.begin();
                auto referenceIt = referenceMap.begin();
                for (int j = 0; j < N - i - 1; ++j, ++it, ++referenceIt) {
                    EXPECT_NE(it, testMap.end());
                    EXPECT_EQ(it->first, referenceIt->first);
                    EXPECT_EQ(it->second, referenceIt->second);
                }
                EXPECT_EQ(it, testMap.end());
                testMap.SelfCheck();
            }
        }
    }
}

TEST(TCowTree, InsertErase)
{
    constexpr int N = 1024;
    auto generate = [] (auto sequenceType, auto i) {
        switch (sequenceType) {
            case 0:
                return i;
            case 1:
                return N - i - 1;
            case 2:
                return i % 2 ? i / 2 : N - i / 2 - 1;
            case 3:
                return i % 2 ? N / 2 - i / 2 - 1 : N / 2 + i / 2;
            default:
                YT_VERIFY(false);
        };
    };
    for (int insertSequenceType = 0; insertSequenceType < 4; insertSequenceType++) {
        for (int eraseSequenceType = 0; eraseSequenceType < 4; eraseSequenceType++) {
            TTestTree testSet;
            std::set<ui64> referenceSet;
            for (int i = 0; i < N; ++i) {
                auto v = generate(insertSequenceType, i);
                assert(testSet.find(v) == testSet.end());
                EXPECT_EQ(testSet.find(v), testSet.end());
                referenceSet.insert(v);
                bool success = testSet.insert(v).second;
                EXPECT_TRUE(success);
                EXPECT_EQ(testSet.size(), i + 1);

                auto it = testSet.begin();
                auto referenceIt = referenceSet.begin();
                for (int j = 0; j <= i; ++j, ++it, ++referenceIt) {
                    EXPECT_NE(it, testSet.end());
                    EXPECT_EQ(*it, *referenceIt);
                    bool success = testSet.insert(*it).second;
                    EXPECT_FALSE(success);
                    EXPECT_EQ(testSet.find(*it), it);
                }
                EXPECT_EQ(it, testSet.end());

                EXPECT_EQ(testSet.size(), i + 1);
                testSet.SelfCheck();
            }
            for (int i = 0; i < N; ++i) {
                auto v = generate(eraseSequenceType, i);
                EXPECT_NE(testSet.find(v), testSet.end());
                referenceSet.erase(v);
                bool success = testSet.erase(v);
                EXPECT_TRUE(success);
                EXPECT_EQ(testSet.size(), N - i - 1);

                auto it = testSet.begin();
                auto referenceIt = referenceSet.begin();
                for (int j = 0; j < N - i - 1; ++j, ++it, ++referenceIt) {
                    EXPECT_NE(it, testSet.end());
                    EXPECT_EQ(*it, *referenceIt);
                    EXPECT_EQ(testSet.find(*it), it);
                }
                EXPECT_EQ(it, testSet.end());

                for (int j = 0; j <= i; ++j) {
                    auto v = generate(eraseSequenceType, j);
                    bool success = testSet.erase(v);
                    EXPECT_FALSE(success);
                    EXPECT_EQ(testSet.find(v), testSet.end());
                }

                EXPECT_EQ(testSet.size(), N - i - 1);
                testSet.SelfCheck();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// A snapshot shares its leaves with the master, so taking an element out of a
// shared one has to copy the leaf first: the move would otherwise empty the value
// the snapshot is looking at.
TEST(TCowTree, FailedExtractionPreservesValue)
{
    bool fail = false;
    auto less = [&] (int lhs, int rhs) {
        if (fail) {
            throw std::runtime_error("Comparison failed");
        }
        return lhs < rhs;
    };
    TCowMap<int, std::string, decltype(less)> tree(less);
    tree.try_emplace(1, "original");
    auto it = tree.begin();
    fail = true;
    EXPECT_THROW(tree.ExtractAt(it), std::runtime_error);
    EXPECT_EQ(tree.begin()->second, "original");
    it = tree.begin();
    EXPECT_THROW(tree.ExtractAdvance(it), std::runtime_error);
    EXPECT_EQ(tree.begin()->second, "original");
}

TEST(TCowTree, ExtractLeavesSnapshotIntact)
{
    using TStringMap = TCowTree<TCowTreeDefaultConfig<ui64, 16, std::less<ui64>, std::string>>;

    auto makeValue = [] (ui64 key) {
        // Long enough to own a heap buffer, which is what a move steals.
        return "value-" + std::string(64, 'x') + ToString(key);
    };

    TStringMap map;
    for (int i = 0; i < 200; ++i) {
        map.emplace(i, makeValue(i));
    }

    auto snapshot = map.Snapshot();

    auto it = map.find(42);
    auto extracted = map.ExtractAt(it);
    EXPECT_EQ(extracted.first, 42u);
    EXPECT_EQ(extracted.second, makeValue(42));
    EXPECT_EQ(snapshot.at(42), makeValue(42));

    it = map.find(43);
    auto advanced = map.ExtractAdvance(it);
    EXPECT_EQ(advanced.second, makeValue(43));
    EXPECT_EQ(snapshot.at(43), makeValue(43));

    // The snapshot keeps the whole extracted range, the master loses it.
    EXPECT_EQ(snapshot.size(), 200u);
    EXPECT_EQ(map.size(), 198u);
    for (int i = 0; i < 200; ++i) {
        EXPECT_EQ(snapshot.at(i), makeValue(i));
    }
    EXPECT_FALSE(map.contains(42));
    EXPECT_FALSE(map.contains(43));
    map.SelfCheck();
}

// Replacing a value materializes the replacement before the stored one is
// destroyed, so assigning an element to itself is well defined.
TEST(TCowTree, AssignFromOwnValue)
{
    using TStringMap = TCowTree<TCowTreeDefaultConfig<ui64, 16, std::less<ui64>, std::string>>;

    TStringMap map;
    std::string first(64, 'a');
    std::string second(64, 'b');
    map.emplace(1, first);
    map.emplace(2, second);

    map.AssignValue(1, map.at(1));
    EXPECT_EQ(map.at(1), first);

    map.AssignOrInsert(1, map.at(1));
    EXPECT_EQ(map.at(1), first);

    map.insert_or_assign(2, map.at(2));
    EXPECT_EQ(map.at(2), second);

    // The same paths with a foreign value keep working.
    map.AssignValue(1, map.at(2));
    EXPECT_EQ(map.at(1), second);
    map.insert_or_assign(2, first);
    EXPECT_EQ(map.at(2), first);
    map.SelfCheck();
}

// Each default construction of this comparator is distinguishable, which is what
// makes a lost comparator copy observable.
struct TTaggedLess
{
    static inline int Counter = 0;

    int Tag = ++Counter;

    bool operator()(ui64 lhs, ui64 rhs) const
    {
        return lhs < rhs;
    }
};

TEST(TCowTree, SnapshotKeepsComparator)
{
    TCowSet<ui64, TTaggedLess, TCowTreeStConfig<16>> set;
    for (int i = 0; i < 100; ++i) {
        set.insert(i);
    }

    auto snapshot = set.Snapshot();
    EXPECT_EQ(snapshot.key_comp().Tag, set.key_comp().Tag);

    // The copy is the point: it registers a second view of the same version.
    auto copy = snapshot; // NOLINT(performance-unnecessary-copy-initialization)
    EXPECT_EQ(copy.key_comp().Tag, set.key_comp().Tag);
    EXPECT_EQ(copy.size(), 100u);
    EXPECT_TRUE(copy.contains(50));
}

TEST(TCowTree, SnapshotVersionBookkeeping)
{
    TTestTree set;
    for (int i = 0; i < 200; ++i) {
        set.insert(i);
    }

    auto oldSnapshot = set.Snapshot();
    set.insert(1000);

    {
        auto newSnapshot = set.Snapshot();
        auto oldCopy = oldSnapshot; // NOLINT(performance-unnecessary-copy-initialization)
        EXPECT_FALSE(oldCopy.contains(1000));
        {
            // One of the two views at the newest version goes away; the master
            // still shares its leaves with the other one.
            auto newCopy = newSnapshot; // NOLINT(performance-unnecessary-copy-initialization)
            EXPECT_TRUE(newCopy.contains(1000));
        }
        set.insert(1001);
        EXPECT_TRUE(newSnapshot.contains(1000));
        EXPECT_FALSE(newSnapshot.contains(1001));
        EXPECT_FALSE(oldSnapshot.contains(1000));
    }

    // The newest version is gone, but the older snapshot is not: the master keeps
    // copying its leaves rather than erasing in place.
    set.erase(100);
    EXPECT_FALSE(set.contains(100));
    EXPECT_TRUE(oldSnapshot.contains(100));

    {
        // Another view of the older version, made while it is the newest one live.
        auto oldCopy = oldSnapshot; // NOLINT(performance-unnecessary-copy-initialization)
        set.erase(101);
        EXPECT_TRUE(oldCopy.contains(101));
    }

    set.erase(102);
    EXPECT_TRUE(oldSnapshot.contains(102));
    EXPECT_EQ(oldSnapshot.size(), 200u);

    for (int i = 0; i < 100; ++i) {
        set.insert(2000 + i);
    }
    set.SelfCheck();
    EXPECT_EQ(oldSnapshot.size(), 200u);
    EXPECT_TRUE(set.contains(2099));
}

// The allocation hook is a pair: nodes and the shared core alike are born through
// CreateNode and die through DeleteNode, which is what lets a pool or slab
// allocator own the tree's memory.
struct TCountingHook
{
    static inline int Created = 0;
    static inline int Deleted = 0;
    static inline int AllocationsBeforeFailure = -1;
    static inline bool FailDeletion = false;
};

struct TCountingConfig
    : TCowTreeStConfig<16>
{
    template <class TNode, class... TArgs>
    static TNode* CreateNode(TArgs&&... args)
    {
        if (TCountingHook::AllocationsBeforeFailure == 0) {
            throw std::bad_alloc();
        }
        if (TCountingHook::AllocationsBeforeFailure > 0) {
            --TCountingHook::AllocationsBeforeFailure;
        }
        ++TCountingHook::Created;
        return new TNode(std::forward<TArgs>(args)...);
    }

    template <class TNode>
    static void DeleteNode(TNode* node)
    {
        if (TCountingHook::FailDeletion) {
            throw std::runtime_error("deliberate deletion failure");
        }
        ++TCountingHook::Deleted;
        delete node;
    }
};

TEST(TCowTree, AllocationHookPair)
{
    TCountingHook::Created = 0;
    TCountingHook::Deleted = 0;

    {
        TCowSet<ui64, std::less<ui64>, TCountingConfig> set;
        for (int i = 0; i < 1000; ++i) {
            set.insert(i);
        }

        auto snapshot = set.Snapshot();
        for (int i = 0; i < 1000; i += 2) {
            set.erase(i);
        }
        set.SelfCheck();

        // The copies made for the snapshot are alive, so nothing is balanced yet.
        EXPECT_GT(TCountingHook::Created, 1);
        EXPECT_LT(TCountingHook::Deleted, TCountingHook::Created);
        EXPECT_EQ(snapshot.size(), 1000u);
    }

    // The tree, its snapshot and the core they shared are gone.
    EXPECT_EQ(TCountingHook::Created, TCountingHook::Deleted);
}

TEST(TCowTreeDeathTest, NodeAllocationFailureAborts)
{
    TCowSet<int, std::less<int>, TCountingConfig> set;
    EXPECT_DEATH({
        TCountingHook::AllocationsBeforeFailure = 0;
        set.Insert(1); }, "Failed to create a CoW tree node.*bad_alloc");
}

TEST(TCowTreeDeathTest, NodeDeletionFailureAborts)
{
    TCowSet<int, std::less<int>, TCountingConfig> set;
    set.Insert(1);
    auto erase = [&] {
        TCountingHook::FailDeletion = true;
        set.erase(1);
    };
    EXPECT_DEATH(erase(), "Failed to delete a CoW tree node.*deliberate deletion failure");
}

struct TThrowingMove
{
    TThrowingMove(TThrowingMove&&) noexcept(false);
    bool operator<(const TThrowingMove&) const;
};

static_assert(!CCowTreeConfig<TCowTreeDefaultConfig<TThrowingMove>>);
static_assert(!CCowTreeConfig<TCowTreeDefaultConfig<int, 16, std::less<int>, TThrowingMove>>);

struct TFailingCopyValue
{
    TFailingCopyValue() = default;
    TFailingCopyValue(TFailingCopyValue&&) noexcept = default;

    TFailingCopyValue(const TFailingCopyValue&)
    {
        throw std::runtime_error("deliberate copy failure");
    }
};

TEST(TCowTreeDeathTest, SlotCopyFailureAborts)
{
    TCowMap<int, TFailingCopyValue> map;
    map.Insert(1, TFailingCopyValue{});
    auto snapshot = map.Snapshot();
    EXPECT_DEATH(map.MutableAt(1), "Failed to construct a CoW tree slot.*deliberate copy failure");
    EXPECT_EQ(snapshot.size(), 1u);
}

struct TCopyCheckedKey
{
    static inline std::optional<int> ForbiddenCopy;
    int Value;

    explicit TCopyCheckedKey(int value)
        : Value(value)
    { }

    TCopyCheckedKey(const TCopyCheckedKey& other)
        : Value(other.Value)
    {
        if (Value == ForbiddenCopy) {
            throw std::runtime_error("unexpected key copy");
        }
    }

    TCopyCheckedKey(TCopyCheckedKey&&) noexcept = default;

    bool operator<(const TCopyCheckedKey& other) const
    {
        return Value < other.Value;
    }
};

TEST(TCowTree, EraseAdvancesLiveIteratorsWithoutCopyingErasedKey)
{
    TCowSet<TCopyCheckedKey, std::less<>, TCowTreeStConfig<4>> set;
    for (int key = 0; key < 100; ++key) {
        set.Insert(TCopyCheckedKey(key));
    }
    auto first = set.begin();
    auto second = set.find(TCopyCheckedKey(0));
    for (int key = 0; key < 100; ++key) {
        TCopyCheckedKey::ForbiddenCopy = key;
        Y_DEFER
        {
            TCopyCheckedKey::ForbiddenCopy.reset();
        };
        EXPECT_EQ(set.erase(TCopyCheckedKey(key)), 1u);
        EXPECT_EQ(first, second);
        if (key + 1 < 100) {
            EXPECT_EQ(first->Value, key + 1);
        }
    }
    EXPECT_EQ(first, set.end());
}

TEST(TCowTree, ReplacementPreservesOldValueOnCopyFailure)
{
    TCowMap<int, TCopyCheckedKey, std::less<int>, TCountingConfig> map;
    map.Insert(1, TCopyCheckedKey(10));
    auto snapshot = map.Snapshot();
    int allocations = TCountingHook::Created;
    TCopyCheckedKey replacement(20);
    TCopyCheckedKey::ForbiddenCopy = 20;
    Y_DEFER
    {
        TCopyCheckedKey::ForbiddenCopy.reset();
    };
    EXPECT_THROW(map.AssignValue(1, replacement), std::runtime_error);
    EXPECT_THROW(map.AssignOrInsert(1, replacement), std::runtime_error);
    EXPECT_EQ(TCountingHook::Created, allocations);
    EXPECT_EQ(snapshot.at(1).Value, 10);
    EXPECT_EQ(std::as_const(map).at(1).Value, 10);
    map.AssignValue(1, std::as_const(map).at(1));
    EXPECT_EQ(std::as_const(map).at(1).Value, 10);
}

struct TCountComparisons
{
    int* Count = nullptr;

    bool operator()(int lhs, int rhs) const
    {
        ++*Count;
        return lhs < rhs;
    }
};

TEST(TCowTree, InsertAndEraseDoNotReseekForLiveIterators)
{
    int plainCount = 0;
    int trackedCount = 0;
    using TSet = TCowSet<int, TCountComparisons, TCowTreeStConfig<4>>;
    TSet plain(TCountComparisons{&plainCount});
    TSet tracked(TCountComparisons{&trackedCount});
    plain.Insert(0);
    tracked.Insert(0);
    auto it = tracked.begin();
    for (int key = 1; key < 256; ++key) {
        const auto last = std::prev(tracked.end());
        plain.Insert(key);
        tracked.Insert(key);
        ASSERT_EQ(*last, key - 1);
        ASSERT_EQ(plainCount, trackedCount);
    }
    auto plainSnapshot = plain.Snapshot();
    auto trackedSnapshot = tracked.Snapshot();
    auto follower = tracked.begin();
    plainCount = trackedCount = 0;
    for (int key = 0; key < 256; ++key) {
        ASSERT_EQ(*it, key);
        ASSERT_EQ(follower, it);
        ASSERT_EQ(plain.erase(key), 1u);
        ASSERT_EQ(tracked.erase(key), 1u);
        ASSERT_EQ(plainCount, trackedCount);
    }
    EXPECT_EQ(it, tracked.end());
    EXPECT_EQ(follower, tracked.end());
    EXPECT_EQ(plainSnapshot.size(), 256u);
    EXPECT_EQ(trackedSnapshot.size(), 256u);
}

TEST(TCowTree, EraseByKeyStopsComparingAfterExactHit)
{
    struct TLess
    {
        bool* Hit;

        bool operator()(int lhs, int rhs) const
        {
            EXPECT_FALSE(*Hit);
            return lhs < rhs;
        }

        std::weak_ordering Compare(int lhs, int rhs) const
        {
            EXPECT_FALSE(*Hit);
            *Hit = lhs == rhs;
            return lhs <=> rhs;
        }
    };

    bool hit = false;
    TCowSet<int, TLess, TCowTreeStConfig<4>> set(TLess{&hit});
    for (int key = 0; key < 256; ++key) {
        set.Insert(key);
    }
    auto snapshot = set.Snapshot();
    auto it = set.begin();
    for (int key = 0; key < 255; ++key) {
        ASSERT_EQ(*it, key);
        ASSERT_EQ(set.erase(key), 1u);
        ASSERT_TRUE(hit);
        hit = false;
    }
    EXPECT_EQ(*it, 255);
    EXPECT_EQ(snapshot.size(), 256u);
}
