#define COW_TREE_DEBUG__
#include <yt/yt/flow/library/cpp/misc/cow_tree.h>

#include <yt/yt/core/test_framework/framework.h>

#include <random>
#include <thread>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t BlockSize = 6;
using TCowSet = TCowTree<int, void, BlockSize>;

void ToArray(TCowSet root, std::vector<int>& elems)
{
    for (const auto& elem : std::as_const(root)) {
        elems.push_back(elem);
    }
}

void DelFromArray(std::vector<int>& v, int val)
{
    v.erase(std::remove(v.begin(), v.end(), val), v.end());
}

void CheckTreeStructure(TCowSet node)
{
    node.Verify();
    std::vector<int> buf;
    ToArray(node, buf);
    EXPECT_TRUE(std::is_sorted(buf.begin(), buf.end()));
}

std::vector<int> GetRange(const std::vector<int>& v, int l, int r)
{
    std::vector<int> res;
    for (auto x : v) {
        if (x >= l && x < r) {
            res.push_back(x);
        }
    }
    return res;
}

std::vector<int> GetRange(TCowSet v, int l, int r)
{
    std::vector<int> res;
    for (int value : std::as_const(v)) {
        if (value >= l && value < r) {
            res.push_back(value);
        }
    }
    return res;
}

void Compare(TCowSet root, const std::vector<int>& toComp)
{
    std::vector<int> printed;
    ToArray(root, printed);
    EXPECT_EQ(toComp, printed);
}

int RandChoice(const std::vector<int> v)
{
    return v[RandomNumber<ui32>(std::numeric_limits<int>::max()) % (v.size())];
}

void RunOperations(int numOps, int history, int valRange, int delFreqNom, int delFreqDenom, int reads)
{
    std::vector<TCowSet> trees;
    trees.push_back(TCowSet());

    std::vector<std::vector<int>> toComp;
    toComp.emplace_back();

    for (int i = 0; i < numOps; i++) {
        auto num = std::max(0, i - static_cast<int>(RandomNumber<ui32>(std::numeric_limits<int>::max()) % history));
        auto val = RandomNumber<ui32>(std::numeric_limits<int>::max()) % valRange;

        toComp.push_back(toComp[num]);
        if (i % delFreqDenom != delFreqNom || toComp.back().empty()) {
            auto newTree = trees[num];
            newTree.insert(val);
            trees.push_back(std::move(newTree));
            if (std::ranges::find(toComp.back(), val) == toComp.back().end()) {
                toComp.back().push_back(val);
            }
        } else {
            val = RandChoice(toComp.back());
            auto newTree = trees[num];
            newTree.erase(val);
            trees.push_back(std::move(newTree));
            DelFromArray(toComp.back(), val);
        }
        std::sort(toComp.back().begin(), toComp.back().end());

        Compare(trees.back(), toComp.back());
        CheckTreeStructure(trees.back());

        for (int i = 0; i < reads; i++) {
            int k1 = (RandomNumber<ui32>(std::numeric_limits<int>::max()) % (valRange + 10)) - 5;
            int k2 = (RandomNumber<ui32>(std::numeric_limits<int>::max()) % (valRange + 10)) - 5;
            if (k1 > k2) {
                std::swap(k1, k2);
            }
            auto a1 = GetRange(toComp.back(), k1, k2);
            auto a2 = GetRange(trees.back(), k1, k2);
            EXPECT_EQ(a1, a2);
        }
    }
}

struct TTestLeakCounter
    : TRefCounted
{
    static i64 Counter;
    i64 Value;

    TTestLeakCounter(i64 value)
        : Value(value)
    {
        Counter++;
    }

    ~TTestLeakCounter() override
    {
        Counter--;
    }
};

using TTestLeakCounterRef = TIntrusivePtr<TTestLeakCounter>;

bool operator<(const TTestLeakCounterRef& self, const TTestLeakCounterRef& oth)
{
    return self->Value < oth->Value;
}

i64 TTestLeakCounter::Counter = 0;

TEST(TCowTreeTest, Inserts)
{
    RunOperations(3, 2, 30, 0, 1, 10);
}

TEST(TCowTreeTest, LinearStress)
{
    RunOperations(4000, 1, 400, 1, 3, 10);
}

TEST(TCowTreeTest, Stress)
{
    RunOperations(4000, 20, 400, 1, 3, 10);
}

TEST(TCowTreeTest, Leaks)
{
    TTestLeakCounter::Counter = 0;
    TCowTree<TTestLeakCounterRef, void, 6> tree;
    for (int i = 0; i < 10000; i++) {
        if (i % 5 == 0) {
            auto v = tree.FindFirstSatisfying([] (const auto& key) {
                return key->Value > RandomNumber<ui32>(std::numeric_limits<int>::max());
            });
            if (v != tree.cend()) {
                tree.erase(v);
            }
        } else {
            tree.insert(New<TTestLeakCounter>(RandomNumber<ui32>(std::numeric_limits<int>::max())));
        }
        EXPECT_EQ(TTestLeakCounter::Counter, i64(tree.size()));
    }
}

TEST(TCowTreeTest, Iterate)
{
    TCowTree<int, void, 6> tree;
    std::set<int> toComp;
    for ([[maybe_unused]] auto v : std::as_const(tree)) {
        EXPECT_TRUE(false);
    }
    for (int i = 0; i < 4000; i++) {
        auto v = RandomNumber<ui32>(std::numeric_limits<int>::max());
        tree.insert(v);
        toComp.insert(v);
        if (i % 10 == 0) {
            std::set<int> segm;
            int l = RandomNumber<ui32>(std::numeric_limits<int>::max());
            int r = RandomNumber<ui32>(std::numeric_limits<int>::max());
            if (l > r) {
                std::swap(l, r);
            }
            for (auto v : std::as_const(tree)) {
                if (v >= l && v < r) {
                    segm.insert(v);
                }
            }
            std::set<int> toCompSegm;
            auto itl = toComp.lower_bound(l);
            auto itr = toComp.lower_bound(r);
            for (auto i = itl; i != itr; i++) {
                toCompSegm.insert(*i);
            }
            EXPECT_EQ(toCompSegm, segm);
        }
    }
}

template <class TLhs, class TRhs>
void ExpectEntryEq(const TLhs& lhs, const TRhs& rhs)
{
    if constexpr (requires { lhs.first; }) {
        EXPECT_EQ(lhs.first, rhs.first);
        EXPECT_EQ(lhs.second, rhs.second);
    } else {
        EXPECT_EQ(lhs, rhs);
    }
}

void VerifyEqual(const auto& tree, const auto& checkTree, int maxVal)
{
    auto it = tree.begin();
    auto checkIt = checkTree.begin();

    EXPECT_EQ(tree.size(), checkTree.size());
    EXPECT_EQ(tree.empty(), checkTree.empty());

    for (; checkIt != checkTree.end(); ++it, ++checkIt) {
        ExpectEntryEq(*it, *checkIt);
    }
    EXPECT_EQ(it, tree.end());

    auto rIt = std::make_reverse_iterator(tree.end());
    auto checkRIt = checkTree.rbegin();
    for (; checkRIt != checkTree.rend(); ++rIt, ++checkRIt) {
        ExpectEntryEq(*rIt, *checkRIt);
    }
    EXPECT_EQ(rIt, std::make_reverse_iterator(tree.begin()));

    for (int i = 0; i <= maxVal; i++) {
        EXPECT_EQ(tree.contains(i), checkTree.contains(i));
        EXPECT_EQ(tree.count(i), checkTree.count(i));
    }

    for (int i = 0; i <= maxVal; i++) {
        auto it = std::as_const(tree).lower_bound(i);
        auto checkIt = checkTree.lower_bound(i);
        if (it == tree.end()) {
            EXPECT_EQ(checkIt, checkTree.end());
        } else {
            EXPECT_NE(checkIt, checkTree.end());
            ExpectEntryEq(*it, *checkIt);
        }
    }
}

TEST(TCowTreeTest, BasicAdapterTest)
{
    NDetail::TMaybeIntrusivePtrAcquireCounters::Reset();
    TCowTree<int, void, 4> tree;
    std::set<int> checkTree;
    const int maxVal = 500;

    for (int i = 1; i < 700; i++) {
        tree.insert(i);
        tree.insert(tree.cend(), 2 * i);
        checkTree.insert(i);
        checkTree.insert(2 * i);
        tree.Verify();
        VerifyEqual(tree, checkTree, maxVal);
    }

    for (int i = 1; i < 700; i++) {
        tree.erase(i);
        checkTree.erase(i);
        if (std::as_const(tree).find(3 * i) != tree.cend()) {
            tree.erase(std::as_const(tree).find(3 * i));
        }
        checkTree.erase(3 * i);
        tree.Verify();
        VerifyEqual(tree, checkTree, maxVal);
    }

    tree.clear();
    checkTree.clear();
    VerifyEqual(tree, checkTree, maxVal);
    EXPECT_EQ(NDetail::TMaybeIntrusivePtrAcquireCounters::SlowPath, 0u);
}

TEST(TCowTreeTest, IteratorsDoNotInvalidate)
{
    NDetail::TMaybeIntrusivePtrAcquireCounters::Reset();
    TCowTree<int, void, 4> tree;
    std::set<int> checkTree;

    std::vector<TCowTree<int, void, 4>::iterator> iters;
    for (int i = 0; i < 7000; i++) {
        auto it = tree.insert(i).first;
        iters.push_back(it);
    }

    for (int i = 0; i < 7000; i++) {
        EXPECT_EQ(*iters[i], i);
        EXPECT_EQ(std::as_const(tree).find(i), iters[i]);
        EXPECT_NE(iters[i], tree.cend());

        if (i != 0) {
            EXPECT_EQ(std::prev(iters[i]), std::as_const(tree).find(i - 1));
        }
        if (i != 7000 - 1) {
            EXPECT_EQ(std::next(iters[i]), std::as_const(tree).find(i + 1));
        }
    }

    EXPECT_EQ(NDetail::TMaybeIntrusivePtrAcquireCounters::SlowPath, 0u);
    auto snapshot = tree;

    for (int i = 0; i < 7000; i++) {
        if (i % 7 != 0) {
            tree.erase(i);
        }
    }

    for (int i = 0; i < 7000; i++) {
        if (i % 7 == 0) {
            EXPECT_EQ(*iters[i], i);
            EXPECT_EQ(std::as_const(tree).find(i), iters[i]);
            EXPECT_NE(iters[i], tree.cend());

            if (i != 0) {
                EXPECT_EQ(std::prev(iters[i]), std::as_const(tree).find(i - 7));
            }
            if (i != 7000 - 7) {
                EXPECT_EQ(std::next(iters[i]), std::as_const(tree).find(i + 7));
            }
        }
    }
}

TEST(TCowTreeTest, Snapshot)
{
    SetRandomSeed(112);
    TCowTree<int, void, 8> tree;
    std::set<int> checkTree;
    const int maxVal = 100;
    for (int i = 0; i < maxVal / 2; i++) {
        tree.insert(i);
        checkTree.insert(i);
    }

    std::vector<TCowTree<int, void, 8>> snapshots;
    std::vector<std::set<int>> checkSnapshots;

    for (int i = 0; i < 1000; i++) {
        bool isInsert = RandomNumber<double>() < 0.5;
        int val = RandomNumber<ui64>(maxVal);
        if (isInsert) {
            tree.insert(val);
            checkTree.insert(val);
        } else {
            tree.erase(val);
            checkTree.erase(val);
        }
        snapshots.push_back(tree);
        checkSnapshots.push_back(checkTree);
    }

    for (int i = 0; i < std::ssize(snapshots); i++) {
        VerifyEqual(snapshots[i], checkSnapshots[i], maxVal);
    }
}

TEST(TCowTreeTest, BasicMapTest)
{
    NDetail::TMaybeIntrusivePtrAcquireCounters::Reset();
    TCowTree<int, int, 4> tree;
    for (int i = 0; i < 5000; i++) {
        tree.insert({i, i});
    }

    for (int i = 0; i < 5000; i++) {
        tree.at(i)++;
    }

    for (int i = 0; i < 5000; i++) {
        EXPECT_EQ(std::as_const(tree).at(i), i + 1);
        EXPECT_EQ(std::as_const(tree).find(i)->second, i + 1);
        EXPECT_EQ(std::as_const(tree).find(i)->second, i + 1);
        EXPECT_EQ(tree.at(i), i + 1);
    }

    for (int i = 0; i < 5000; i++) {
        if (i % 3 == 1) {
            tree.erase(i);
        } else if (i % 3 == 2) {
            tree.erase(std::as_const(tree).find(i));
        }
    }

    EXPECT_EQ(tree.size(), 1667u);
    for (int i = 0; i < 5000; i++) {
        if (i % 3 != 0) {
            EXPECT_FALSE(tree.contains(i));
        } else {
            EXPECT_TRUE(tree.contains(i));
            EXPECT_EQ(tree.at(i), i + 1);
        }
    }
    EXPECT_EQ(NDetail::TMaybeIntrusivePtrAcquireCounters::SlowPath, 0u);
}

TEST(TCowTreeTest, SnapshotMap)
{
    TCowTree<int, int, 4> map;
    std::map<int, int> checkMap;
    std::vector<TCowTree<int, int, 4>> snapshots;
    std::vector<std::map<int, int>> checkSnapshots;

    const int maxKey = 30;
    const int maxVal = 100;

    for (int i = 0; i < 5000; i++) {
        bool isSet = RandomNumber<double>() < 0.7;
        int key = RandomNumber<ui64>(maxKey);
        if (isSet) {
            int val = RandomNumber<ui64>(maxVal);
            map.at(key) = val;
            checkMap[key] = val;
        } else {
            map.erase(key);
            checkMap.erase(key);
        }
        snapshots.push_back(map);
        checkSnapshots.push_back(checkMap);
        VerifyEqual(map, checkMap, maxVal);
    }

    for (int i = 0; i < std::ssize(snapshots); i++) {
        VerifyEqual(snapshots[i], checkSnapshots[i], maxVal);
    }
}

TEST(TCowTreeTest, MapIteratorsDoNotInvalidateWithSnapshots)
{
    //! Important (though quite rarely happening case when 2 snapshots hold refs to individual values inside map).
    {
        TCowTree<int, int, 20> map;
        for (int i = 0; i < 5; i++) {
            map.at(i) = i;
        }
        auto snapshot = map;
        snapshot.at(6) = 6;
        auto it = map.find(3);
        it->second = 0;
        std::prev(it)->second = 0;
        std::next(it)->second = 0;

        for (int i = 2; i <= 4; i++) {
            EXPECT_EQ(std::as_const(map).at(i), 0);
            EXPECT_EQ(std::as_const(snapshot).at(i), i);
        }
    }

    {
        TCowTree<int, int, 4> map;
        std::map<int, int> checkMap;
        std::map<int, TCowTree<int, int, 4>::iterator> iters;
        std::map<int, std::map<int, int>::iterator> checkIters;
        std::vector<TCowTree<int, int, 4>> snapshots;

        auto checkIterator = [&] (const auto& iter, const auto& checkIter) {
            EXPECT_EQ(iter == map.end(), checkIter == checkMap.end());
            if (checkIter == checkMap.end()) {
                return;
            }
            ExpectEntryEq(*iter, *checkIter);
        };

        auto checkIteratorVicinity = [&] (const auto& iter, const auto& checkIter) {
            checkIterator(iter, checkIter);
            if (checkIter != checkMap.end()) {
                checkIterator(std::next(iter), std::next(checkIter));
            }
            if (checkIter != checkMap.begin()) {
                checkIterator(std::prev(iter), std::prev(checkIter));
            }
        };

        const int maxKey = 500;

        for (int i = 0; i < 50000; i++) {
            bool isInsert = RandomNumber<double>() < 0.8;
            int key = RandomNumber<ui32>(maxKey);
            if (isInsert) {
                int val = RandomNumber<ui32>(1000);
                map.at(key) = val;
                checkMap[key] = val;

                auto randomItKey = RandomNumber<ui32>(maxKey);
                auto newIt = map.lower_bound(randomItKey);
                if (newIt != map.end() && !iters.contains(newIt->first)) {
                    iters[newIt->first] = newIt;
                    checkIters[newIt->first] = checkMap.find(newIt->first);
                }

            } else {
                map.erase(key);
                checkMap.erase(key);
                iters.erase(key);
                checkIters.erase(key);
            }

            if (i % 5000 == 0) {
                snapshots.push_back(map);
                iters.clear();
                checkIters.clear();
            }

            if (i % 500 == 0) {
                for (const auto& [key, iter] : iters) {
                    checkIteratorVicinity(iter, checkIters[key]);
                }
            }
        }

        for (const auto& [key, iter] : iters) {
            checkIteratorVicinity(iter, checkIters[key]);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCowTreeTest, SetRemoveAll)
{
    constexpr int testRounds = 10;
    constexpr int roundCheckCost = 200000;
    for (int testSize : {2, 10, 100, 10000}) {
        int checkCount = std::min(roundCheckCost / testSize, testSize);
        int checkEvery = testSize / checkCount;
        std::vector<int> data(testSize);
        for (int i = 0; i < testSize; ++i) {
            data[i] = i;
        }
        std::random_device rd;
        std::mt19937_64 g(rd());

        for (int round = 0; round < testRounds; ++round) {
            std::ranges::shuffle(data, g);
            TCowTree<int, void, 8> test;
            std::set<int> check;
            auto verify = [&] {
                test.Verify();
                EXPECT_EQ(test.size(), check.size());
                auto testIt = test.cbegin();
                auto checkIt = check.cbegin();
                for (; testIt != test.cend() && checkIt != check.cend(); ++testIt, ++checkIt) {
                    ExpectEntryEq(*testIt, *checkIt);
                }
                EXPECT_EQ(testIt, test.cend());
                EXPECT_EQ(checkIt, check.cend());
            };
            int checkDelay = checkEvery;
            for (int i = 0; i < testSize; ++i) {
                test.insert(data[i]);
                check.insert(data[i]);
                if (--checkDelay == 0) {
                    verify();
                    checkDelay = checkEvery;
                }
            }
            verify();
            if (round % 2 == 0) {
                std::ranges::reverse(data);
            }
            for (int i = 0; i < testSize; ++i) {
                test.erase(data[i]);
                check.erase(data[i]);
                if (--checkDelay == 0) {
                    verify();
                    checkDelay = checkEvery;
                }
            }
            verify();
        }
    }
}

TEST(TCowTreeTest, MapRemoveAll)
{
    constexpr int testRounds = 10;
    constexpr int roundCheckCost = 200000;
    for (int testSize : {2, 10, 100, 10000}) {
        int checkCount = std::min(roundCheckCost / testSize, testSize);
        int checkEvery = testSize / checkCount;
        std::vector<int> data(testSize);
        for (int i = 0; i < testSize; ++i) {
            data[i] = i;
        }
        std::random_device rd;
        std::mt19937_64 g(rd());

        for (int round = 0; round < testRounds; ++round) {
            std::ranges::shuffle(data, g);
            TCowTree<int, int, 8> test;
            std::map<int, int> check;
            auto verify = [&] {
                test.Verify();
                EXPECT_EQ(test.size(), check.size());
                auto testIt = test.cbegin();
                auto checkIt = check.cbegin();
                for (; testIt != test.cend() && checkIt != check.cend(); ++testIt, ++checkIt) {
                    ExpectEntryEq(*testIt, *checkIt);
                }
                EXPECT_EQ(testIt, test.cend());
                EXPECT_EQ(checkIt, check.cend());
            };
            int checkDelay = checkEvery;
            for (int i = 0; i < testSize; ++i) {
                test.insert(std::pair(data[i], data[i]));
                check.emplace(data[i], data[i]);
                if (--checkDelay == 0) {
                    verify();
                    checkDelay = checkEvery;
                }
            }
            verify();
            if (round % 2 == 0) {
                std::ranges::reverse(data);
            }
            for (int i = 0; i < testSize; ++i) {
                test.erase(data[i]);
                check.erase(data[i]);
                if (--checkDelay == 0) {
                    verify();
                    checkDelay = checkEvery;
                }
            }
            verify();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Multithread test that tests CowSet. Sum of keys of every set must remain equal to zero.
TEST(TCowTreeTest, SetMultithread)
{
    // Settings.
    using TKey = int;
    using TTestTree = TCowTree<TKey, void, 8>;
    constexpr ssize_t numThreads = 16;
    constexpr ssize_t maxKey = 64;
    constexpr ssize_t waitTime = 5;

    // Helpers.
    auto randomKey = [] () -> TKey {
        return RandomNumber<ui64>() % (2 * maxKey + 1) - maxKey;
    };
    auto randomKeyNot = [&] (TKey anotherKey) -> TKey {
        TKey key;
        while ((key = randomKey()) == anotherKey) {
            // Nothing.
        }
        return key;
    };
    auto randomTheadNo = [] (int currentThreadNo) {
        int threadNo;
        while ((threadNo = RandomNumber<ui64>() % numThreads) == currentThreadNo) {
            // Nothing.
        }
        return threadNo;
    };

    // Each thread will handle mostly with its own context.
    struct TThreadContext
    {
        std::mutex Mutex;
        TTestTree Tree;
    };

    TThreadContext contexts[numThreads];
    TTestTree tree;
    for (int i = -maxKey; i <= maxKey; i += 2) {
        tree.insert(i);
    }
    for (auto& context : contexts) {
        context.Tree = tree;
    }
    std::atomic<bool> done = false;

    // Common thead function to be called by different threads with different argument.
    auto threadFunc = [&] (ssize_t threadNum) {
        auto& context = contexts[threadNum];
        while (!done) {
            auto roll = RandomNumber<ui64>() % 5;
            if (roll == 0) {
                // Steal tree from another thread.
                ssize_t anotherThreadNum = randomTheadNo(threadNum);
                std::unique_lock lock1(threadNum < anotherThreadNum ? contexts[threadNum].Mutex : contexts[anotherThreadNum].Mutex);
                std::unique_lock lock2(threadNum < anotherThreadNum ? contexts[anotherThreadNum].Mutex : contexts[threadNum].Mutex);
                contexts[threadNum].Tree = contexts[anotherThreadNum].Tree;
            } else if (roll == 1) {
                // Check invariant.
                TKey sum = 0;
                std::unique_lock lock(context.Mutex);
                for (const auto& key : context.Tree) {
                    sum += key;
                }
                EXPECT_EQ(sum, 0);
            } else if (roll == 2) {
                // Check invariant with const iteration.
                TKey sum = 0;
                std::unique_lock lock(context.Mutex);
                for (const auto& key : std::as_const(context.Tree)) {
                    sum += key;
                }
                EXPECT_EQ(sum, 0);
            } else if (roll == 3) {
                // Insert/remove some key so that sum of keys will remain zero.
                std::unique_lock lock(context.Mutex);
                TKey keys[3];
                bool willRemove[3];
                keys[0] = randomKey();
                keys[1] = randomKeyNot(keys[0]);
                willRemove[0] = context.Tree.contains(keys[0]);
                willRemove[1] = context.Tree.contains(keys[1]);
                willRemove[2] = !willRemove[0];
                keys[2] = (willRemove[0] ? 1 : -1) * keys[0];
                keys[2] += (willRemove[1] ? 1 : -1) * keys[1];
                keys[2] *= -(willRemove[2] ? 1 : -1);
                if (keys[2] < -maxKey || keys[2] > maxKey) {
                    continue;
                }
                if ((willRemove[2] && !context.Tree.contains(keys[2])) || (!willRemove[2] && context.Tree.contains(keys[2]))) {
                    willRemove[2] = !willRemove[2];
                    keys[2] = -keys[2];
                    if ((willRemove[2] && !context.Tree.contains(keys[2])) || (!willRemove[2] && context.Tree.contains(keys[2]))) {
                        continue;
                    }
                }
                if (keys[2] == keys[0] || keys[2] == keys[1]) {
                    continue;
                }
                for (int i = 0; i < 3; i++) {
                    if (willRemove[i]) {
                        context.Tree.erase(keys[i]);
                    } else {
                        context.Tree.insert(keys[i]);
                    }
                }
            } else if (roll == 4) {
                // The same as above but with const iterators.
                std::unique_lock lock(context.Mutex);
                TKey keys[3];
                bool willRemove[3];
                keys[0] = randomKey();
                keys[1] = randomKeyNot(keys[0]);
                willRemove[0] = std::as_const(context.Tree).contains(keys[0]);
                willRemove[1] = std::as_const(context.Tree).contains(keys[1]);
                willRemove[2] = !willRemove[0];
                keys[2] = (willRemove[0] ? 1 : -1) * keys[0];
                keys[2] += (willRemove[1] ? 1 : -1) * keys[1];
                keys[2] *= -(willRemove[2] ? 1 : -1);
                if (keys[2] < -maxKey || keys[2] > maxKey) {
                    continue;
                }
                if ((willRemove[2] && !std::as_const(context.Tree).contains(keys[2])) || (!willRemove[2] && std::as_const(context.Tree).contains(keys[2]))) {
                    willRemove[2] = !willRemove[2];
                    keys[2] = -keys[2];
                    if ((willRemove[2] && !std::as_const(context.Tree).contains(keys[2])) || (!willRemove[2] && std::as_const(context.Tree).contains(keys[2]))) {
                        continue;
                    }
                }
                if (keys[2] == keys[0] || keys[2] == keys[1]) {
                    continue;
                }
                for (int i = 0; i < 3; i++) {
                    if (willRemove[i]) {
                        context.Tree.erase(keys[i]);
                    } else {
                        context.Tree.insert(keys[i]);
                    }
                }
            } else {
                EXPECT_TRUE(false);
            }
        }
    };

    // Run and wait.
    std::thread threads[numThreads];
    for (int i = 0; i < numThreads; i++) {
        threads[i] = std::thread(threadFunc, i);
    }
    sleep(waitTime);
    done = true;
    for (auto& thread : threads) {
        thread.join();
    }

    // Final single-thread check.
    for (const auto& context : contexts) {
        TKey sum = 0;
        for (const auto& key : context.Tree) {
            sum += key;
        }
        EXPECT_EQ(sum, 0);
    }
}

//! Multithread test that tests CowMap. Sum of values of every map must remain equal to zero.
TEST(TCowTreeTest, MapMultithread)
{
    // Settings.
    using TKey = int;
    using TValue = int;
    using TTestTree = TCowTree<TKey, TValue, 8>;
    constexpr ssize_t numThreads = 16;
    constexpr ssize_t maxKey = 64;
    constexpr ssize_t waitTime = 5;
    static_assert(maxKey % 2 == 0);

    // Helpers.
    auto nodeMapped = [] (auto&& node) {
        return std::forward<decltype(node)>(node).value().second;
    };
    auto randomKey = [] () -> TKey {
        return RandomNumber<ui64>() % (maxKey + 1);
    };
    auto randomKeyNot = [&] (TKey anotherKey) -> TKey {
        while (true) {
            if (auto key = randomKey(); key != anotherKey) {
                return key;
            }
        }
    };
    auto randomTheadNo = [] (int currentThreadNo) {
        while (true) {
            if (int threadNo = RandomNumber<ui64>() % numThreads; threadNo != currentThreadNo) {
                return threadNo;
            }
        }
    };

    // Each thread will handle mostly with its own context.
    struct TThreadContext
    {
        std::mutex Mutex;
        TTestTree Tree;
    };

    TThreadContext contexts[numThreads];
    TTestTree tree;
    for (int i = 0; i <= maxKey; i++) {
        tree.insert({i, i - maxKey / 2});
    }
    for (auto& context : contexts) {
        context.Tree = tree;
    }
    std::atomic<bool> done = false;

    // Common thead function to be called by different threads with different argument.
    auto threadFunc = [&] (ssize_t threadNum) {
        auto& context = contexts[threadNum];
        while (!done) {
            auto roll = RandomNumber<ui64>() % 8;
            if (roll == 0 || roll == 1) {
                // Steal tree from another thread.
                ssize_t anotherThreadNum = randomTheadNo(threadNum);
                std::unique_lock lock1(threadNum < anotherThreadNum ? contexts[threadNum].Mutex : contexts[anotherThreadNum].Mutex);
                std::unique_lock lock2(threadNum < anotherThreadNum ? contexts[anotherThreadNum].Mutex : contexts[threadNum].Mutex);
                contexts[threadNum].Tree = contexts[anotherThreadNum].Tree;
            } else if (roll == 2) {
                // Check invariant.
                TValue sum = 0;
                std::unique_lock lock(context.Mutex);
                for (const auto& [key, value] : context.Tree) {
                    sum += value;
                }
                EXPECT_EQ(sum, 0);
            } else if (roll == 3) {
                // Check invariant with const iteration.
                TValue sum = 0;
                std::unique_lock lock(context.Mutex);
                for (const auto& [key, value] : std::as_const(context.Tree)) {
                    sum += value;
                }
                EXPECT_EQ(sum, 0);
            } else if (roll == 4) {
                // Update two random keys, adding random value to one and substracting from another.
                TKey key1 = randomKey();
                TKey key2 = randomKeyNot(key1);
                TValue diff = 1 + RandomNumber<ui64>() % (maxKey / 2);
                std::unique_lock lock(context.Mutex);
                auto it = context.Tree.find(key1);
                if (it == context.Tree.end()) {
                    context.Tree.insert({key1, diff});
                } else {
                    auto kv = context.Tree.extract(key1);
                    context.Tree.insert({key1, diff + nodeMapped(kv)});
                }
                it = context.Tree.find(key2);
                if (it == context.Tree.end()) {
                    context.Tree.insert({key2, -diff});
                } else {
                    auto kv = context.Tree.extract(key2);
                    context.Tree.insert({key2, -diff + nodeMapped(kv)});
                }
            } else if (roll == 5) {
                // The same as above but with some const variation.
                TKey key1 = randomKey();
                TKey key2 = randomKeyNot(key1);
                TValue diff = 1 + RandomNumber<ui64>() % (maxKey / 2);
                std::unique_lock lock(context.Mutex);
                auto it = std::as_const(context.Tree).find(key1);
                if (it == std::as_const(context.Tree).end()) {
                    context.Tree.insert({key1, diff});
                } else {
                    auto kv = context.Tree.extract(key1);
                    context.Tree.insert({key1, diff + nodeMapped(kv)});
                }
                it = std::as_const(context.Tree).find(key2);
                if (it == std::as_const(context.Tree).end()) {
                    context.Tree.insert({key2, -diff});
                } else {
                    auto kv = context.Tree.extract(key2);
                    context.Tree.insert({key2, -diff + nodeMapped(kv)});
                }
            } else if (roll == 6) {
                // Remove one key and add its value to some other key.
                TKey key1 = randomKey();
                TKey key2 = randomKeyNot(key1);
                std::unique_lock lock(context.Mutex);
                auto it = context.Tree.find(key1);
                if (it != context.Tree.end()) {
                    auto kv = context.Tree.extract(key1);
                    it = context.Tree.find(key2);
                    if (it == context.Tree.end()) {
                        context.Tree.insert({key2, nodeMapped(kv)});
                    } else {
                        auto kv2 = context.Tree.extract(key2);
                        context.Tree.insert({key2, nodeMapped(kv) + nodeMapped(kv2)});
                    }
                }
            } else if (roll == 7) {
                // The same as above but with some const variation.
                TKey key1 = randomKey();
                TKey key2 = randomKeyNot(key1);
                std::unique_lock lock(context.Mutex);
                auto it = std::as_const(context.Tree).find(key1);
                if (it != std::as_const(context.Tree).end()) {
                    auto kv = context.Tree.extract(key1);
                    it = std::as_const(context.Tree).find(key2);
                    if (it == std::as_const(context.Tree).end()) {
                        context.Tree.insert({key2, nodeMapped(kv)});
                    } else {
                        auto kv2 = context.Tree.extract(key2);
                        context.Tree.insert({key2, nodeMapped(kv) + nodeMapped(kv2)});
                    }
                }
            } else {
                EXPECT_TRUE(false);
            }
        }
    };

    // Run and wait.
    std::thread threads[numThreads];
    for (int i = 0; i < numThreads; i++) {
        threads[i] = std::thread(threadFunc, i);
    }
    sleep(waitTime);
    done = true;
    for (auto& thread : threads) {
        thread.join();
    }

    // Final single-thread check.
    for (const auto& context : contexts) {
        TValue sum = 0;
        for (const auto& [key, value] : context.Tree) {
            sum += value;
        }
        EXPECT_EQ(sum, 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
