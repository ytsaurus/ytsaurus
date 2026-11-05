#include <yt/yt/flow/library/cpp/misc/cow_tree.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

void VerifyEqual(const auto& tree, const auto& checkTree, int maxVal)
{
    auto it = tree.begin();
    auto checkIt = checkTree.begin();

    EXPECT_EQ(tree.size(), checkTree.size());
    EXPECT_EQ(tree.empty(), checkTree.empty());

    for (; checkIt != checkTree.end(); ++it, ++checkIt) {
        EXPECT_EQ(*it, *checkIt);
    }
    EXPECT_EQ(it, tree.end());

    auto rIt = std::make_reverse_iterator(tree.end());
    auto checkRIt = checkTree.rbegin();
    for (; checkRIt != checkTree.rend(); ++rIt, ++checkRIt) {
        EXPECT_EQ(*rIt, *checkRIt);
    }
    EXPECT_EQ(rIt, std::make_reverse_iterator(tree.begin()));

    for (int i = 0; i <= maxVal; i++) {
        EXPECT_EQ(tree.contains(i), checkTree.contains(i));
        EXPECT_EQ(tree.count(i), checkTree.count(i));
    }

    for (int i = 0; i <= maxVal; i++) {
        auto it = tree.lower_bound(i);
        auto checkIt = checkTree.lower_bound(i);
        if (it == tree.end()) {
            EXPECT_EQ(checkIt, checkTree.end());
        } else {
            EXPECT_NE(checkIt, checkTree.end());
            EXPECT_EQ(*it, *checkIt);
        }
    }
}

void InnerFuzzingSet(const std::vector<std::pair<bool, int>>& data)
{
    TCowTree<int, void, 2> tree;
    std::set<int> checkTree;
    std::deque<TCowTree<int, void, 2>> snapshots;
    std::deque<std::set<int>> checkSnapshots;

    auto checkIterator = [&] (auto&& it, auto&& checkIt) {
        YT_VERIFY((it == tree.cend()) == (checkIt == checkTree.end()));
        if (checkIt == checkTree.end()) {
            return;
        }
        YT_VERIFY(*it == *checkIt);
    };

    auto checkIteratorVicinity = [&] (auto&& it, auto&& checkIt) {
        checkIterator(it, checkIt);
        if (checkIt != checkTree.end()) {
            checkIterator(std::next(it), std::next(checkIt));
        }
        if (checkIt != checkTree.begin()) {
            checkIterator(std::prev(it), std::prev(checkIt));
        }
    };

    auto checkValue = [&] (int value) {
        checkIteratorVicinity(std::as_const(tree).find(value), checkTree.find(value));
        checkIteratorVicinity(std::as_const(tree).lower_bound(value), checkTree.lower_bound(value));
        checkIteratorVicinity(std::as_const(tree).upper_bound(value), checkTree.upper_bound(value));
    };

    for (int i = 0; i < std::ssize(data); i++) {
        const auto& [isModify, value] = data[i];
        if (isModify) {
            if (checkTree.contains(value)) {
                checkTree.erase(value);
                tree.erase(value);
            } else {
                checkTree.insert(value);
                tree.insert(value);
            }
        } else {
            checkValue(value);
        }

        if (i % 100 == 0) {
            snapshots.push_back(tree);
            checkSnapshots.push_back(checkTree);

            if (std::ssize(snapshots) > 10) {
                snapshots.pop_front();
                checkSnapshots.pop_front();
            }

            int snapshotNum = value % 50;
            if (snapshotNum < std::ssize(snapshots)) {
                tree = snapshots[snapshotNum];
                checkTree = checkSnapshots[snapshotNum];
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size)
{
    size_t realSize = size / 2;
    std::vector<std::pair<bool, int>> dataPrepared(realSize);
    ui16* dataPtr = (ui16*)data;
    for (size_t i = 0; i < realSize; i++) {
        dataPrepared[i] = {dataPtr[i] & 1, dataPtr[i] >> 1};
    }
    NYT::NFlow::InnerFuzzingSet(dataPrepared);
    return 0;
}
