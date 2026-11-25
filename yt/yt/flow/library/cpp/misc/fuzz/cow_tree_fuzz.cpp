#include <yt/yt/flow/library/cpp/misc/cow_tree.h>

#include <cstring>
#include <deque>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

void VerifyEqual(const auto& tree, const auto& checkTree, int maxVal)
{
    auto it = tree.begin();
    auto checkIt = checkTree.begin();

    YT_VERIFY(tree.size() == checkTree.size());
    YT_VERIFY(tree.empty() == checkTree.empty());

    for (; checkIt != checkTree.end(); ++it, ++checkIt) {
        YT_VERIFY(*it == *checkIt);
    }
    YT_VERIFY(it == tree.end());

    auto rIt = std::make_reverse_iterator(tree.end());
    auto checkRIt = checkTree.rbegin();
    for (; checkRIt != checkTree.rend(); ++rIt, ++checkRIt) {
        YT_VERIFY(*rIt == *checkRIt);
    }
    YT_VERIFY(rIt == std::make_reverse_iterator(tree.begin()));

    for (int i = 0; i <= maxVal; i++) {
        YT_VERIFY(tree.contains(i) == checkTree.contains(i));
        YT_VERIFY(tree.count(i) == checkTree.count(i));
    }

    for (int i = 0; i <= maxVal; i++) {
        auto it = tree.lower_bound(i);
        auto checkIt = checkTree.lower_bound(i);
        if (it == tree.end()) {
            YT_VERIFY(checkIt == checkTree.end());
        } else {
            YT_VERIFY(checkIt != checkTree.end());
            YT_VERIFY(*it == *checkIt);
        }
    }
}

//! A deliberately tiny node keeps the structural machinery (splits, merges,
//! redistributes) firing on every few operations.
using TFuzzSet = TCowSet<int, std::less<int>, TCowTreeStConfig<4>>;

//! The point-query sweep of #VerifyEqual() runs over a small key window; the
//! whole-sequence comparison next to it covers the rest.
constexpr int PointQueryBound = 64;

void InnerFuzzingSet(const std::vector<std::pair<bool, int>>& data)
{
    TFuzzSet tree;
    std::set<int> checkTree;
    std::deque<TFuzzSet::TSnapshot> snapshots;
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
            auto snapshot = tree.Snapshot();
            snapshots.push_back(snapshot);
            checkSnapshots.push_back(checkTree);

            if (std::ssize(snapshots) > 10) {
                snapshots.pop_front();
                checkSnapshots.pop_front();
            }

            tree.SelfCheck();
            VerifyEqual(tree, checkTree, PointQueryBound);
            for (int j = 0; j < std::ssize(snapshots); j++) {
                VerifyEqual(snapshots[j], checkSnapshots[j], PointQueryBound);
            }

            int snapshotNum = value % 50;
            if (snapshotNum < std::ssize(snapshots)) {
                // A tree has a single master, so rolling back to an earlier
                // state re-loads it from that snapshot instead of assigning it.
                const auto& restored = snapshots[snapshotNum];
                tree.clear();
                tree.BuildFromSorted(restored.begin(), restored.end());
                checkTree = checkSnapshots[snapshotNum];
            }
        }
    }
    tree.SelfCheck();
    VerifyEqual(tree, checkTree, PointQueryBound);
    for (int i = 0; i < std::ssize(snapshots); ++i) {
        VerifyEqual(snapshots[i], checkSnapshots[i], PointQueryBound);
    }
}

////////////////////////////////////////////////////////////////////////////////

//! The map flavour with a value that owns a heap buffer: moving it out of a leaf
//! shared with a snapshot, or replacing it in place, is what the set model above
//! cannot reach.
using TFuzzMap = TCowMap<int, std::string, std::less<int>, TCowTreeStConfig<4>>;

std::string MakeValue(int key)
{
    return std::string(8 + key % 40, static_cast<char>('a' + key % 26));
}

void VerifyMapEqual(const auto& map, const auto& checkMap)
{
    YT_VERIFY(map.size() == checkMap.size());
    auto it = map.begin();
    auto checkIt = checkMap.begin();
    for (; checkIt != checkMap.end(); ++it, ++checkIt) {
        YT_VERIFY(it->first == checkIt->first);
        YT_VERIFY(it->second == checkIt->second);
    }
    YT_VERIFY(it == map.end());

    for (const auto& [key, value] : checkMap) {
        auto found = map.find(key);
        YT_VERIFY(found != map.end());
        YT_VERIFY(found->second == value);
    }
}

void InnerFuzzingMap(const std::vector<ui16>& words)
{
    TFuzzMap map;
    std::map<int, std::string> checkMap;
    std::deque<TFuzzMap::TSnapshot> snapshots;
    std::deque<std::map<int, std::string>> checkSnapshots;

    for (int i = 0; i < std::ssize(words); i++) {
        int key = words[i] >> 2;
        switch (words[i] & 3) {
            case 0: {
                auto value = MakeValue(key);
                map.insert_or_assign(key, value);
                checkMap.insert_or_assign(key, value);
                break;
            }
            case 1: {
                if (map.contains(key)) {
                    // Self-aliasing replacement: the argument is the stored value.
                    map.AssignValue(key, map.at(key));
                    YT_VERIFY(map.at(key) == checkMap.at(key));
                } else {
                    auto value = MakeValue(key + 1);
                    map.AssignOrInsert(key, value);
                    checkMap.insert_or_assign(key, value);
                }
                break;
            }
            case 2: {
                auto it = map.find(key);
                if (it == map.end()) {
                    YT_VERIFY(!checkMap.contains(key));
                    break;
                }
                // Extraction moves the value out; on a leaf shared with a live
                // snapshot it has to copy that leaf first.
                auto extracted = map.ExtractAt(it);
                YT_VERIFY(extracted.first == key);
                YT_VERIFY(extracted.second == checkMap.at(key));
                checkMap.erase(key);
                break;
            }
            case 3: {
                if (i % 2 == 0) {
                    YT_VERIFY(map.erase(key) == checkMap.erase(key));
                } else {
                    auto it = map.find(key);
                    if (it != map.end()) {
                        auto extracted = map.ExtractAdvance(it);
                        YT_VERIFY(extracted.second == checkMap.at(key));
                        checkMap.erase(key);
                        auto checkIt = checkMap.upper_bound(key);
                        YT_VERIFY((it == map.end()) == (checkIt == checkMap.end()));
                        if (checkIt != checkMap.end()) {
                            YT_VERIFY(it->first == checkIt->first);
                        }
                    }
                }
                break;
            }
        }

        if (i % 50 == 0) {
            // A snapshot is copyable but not movable, so it is pushed as an lvalue.
            auto snapshot = map.Snapshot();
            snapshots.push_back(snapshot);
            checkSnapshots.push_back(checkMap);
            if (std::ssize(snapshots) > 5) {
                snapshots.pop_front();
                checkSnapshots.pop_front();
            }

            map.SelfCheck();
            VerifyMapEqual(map, checkMap);
            for (int j = 0; j < std::ssize(snapshots); j++) {
                VerifyMapEqual(snapshots[j], checkSnapshots[j]);
            }
        }
    }

    map.SelfCheck();
    VerifyMapEqual(map, checkMap);
    for (int j = 0; j < std::ssize(snapshots); j++) {
        VerifyMapEqual(snapshots[j], checkSnapshots[j]);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size)
{
    std::vector<ui16> words(size / sizeof(ui16));
    if (!words.empty()) {
        // The fuzzer input need not be aligned for ui16.
        std::memcpy(words.data(), data, words.size() * sizeof(ui16));
    }

    // The leading word picks the model, so each input exercises one of them at
    // full length instead of both at half throughput.
    if (!words.empty() && (words[0] & 1) != 0) {
        NYT::NFlow::InnerFuzzingMap(words);
        return 0;
    }

    std::vector<std::pair<bool, int>> dataPrepared;
    dataPrepared.reserve(words.size());
    for (ui16 word : words) {
        dataPrepared.emplace_back(static_cast<bool>(word & 1), word >> 1);
    }
    NYT::NFlow::InnerFuzzingSet(dataPrepared);
    return 0;
}
