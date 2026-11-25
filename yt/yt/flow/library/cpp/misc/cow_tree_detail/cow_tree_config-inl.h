#ifndef COW_TREE_CONFIG_INL_H
    #error "Direct inclusion of this file is not allowed, include cow_tree_config.h"
    #include "cow_tree_config.h"
#endif

namespace NYT::NFlow {
namespace NCowTreeInternals {

template <class TConfig, class TNode, class... TArgs>
TNode* CreateCowTreeNode(TArgs&&... args) noexcept
{
    try {
        return TConfig::template CreateNode<TNode>(std::forward<TArgs>(args)...);
    } catch (...) {
        AbortOnCowTreeException("Failed to create a CoW tree node");
    }
}

template <class TConfig, class TNode>
void DeleteCowTreeNode(TNode* node) noexcept
{
    try {
        TConfig::DeleteNode(node);
    } catch (...) {
        AbortOnCowTreeException("Failed to delete a CoW tree node");
    }
}

} // namespace NCowTreeInternals

////////////////////////////////////////////////////////////////////////////////

consteval int ComputeCowTreeMaxHeight(ui64 maxNodeSize, ui64 maxSize)
{
    ui64 minKeys = 1;
    int height = 1;
    while (true) {
        ui64 levelFill = height == 1
            ? maxNodeSize
            : height == 2 ? maxNodeSize / 2
                          : 2 * maxNodeSize / 3;
        if (minKeys > maxSize / levelFill) {
            break;
        }
        minKeys *= levelFill;
        ++height;
    }
    return height + 1;
}

template <class TKey, class TLess>
consteval bool CowTreePrefersLinearSearch()
{
    if constexpr (CHasLinearSearchPreference<TLess>) {
        return TLess::PrefersLinearSearch;
    } else if constexpr (CHasLinearSearchPreference<TKey>) {
        return TKey::PrefersLinearSearch;
    } else {
        return std::is_trivially_copyable_v<TKey> && sizeof(TKey) <= 16 &&
            (std::is_base_of_v<std::less<TKey>, TLess> || std::is_base_of_v<std::greater<TKey>, TLess> ||
            std::is_base_of_v<std::less<>, TLess> || std::is_base_of_v<std::greater<>, TLess>);
    }
}

template <class TKey, class TLess>
consteval int CowTreeStdOrder()
{
    if constexpr (std::same_as<TKey, std::string> || std::same_as<TKey, std::string_view>) {
        if constexpr (std::same_as<TLess, std::less<TKey>> || std::same_as<TLess, std::less<>>) {
            return 1;
        } else if constexpr (std::same_as<TLess, std::greater<TKey>> || std::same_as<TLess, std::greater<>>) {
            return -1;
        }
    }
    return 0;
}

template <class TKey, class TLess>
consteval bool CowTreeComparesThreeWay()
{
    return !CowTreePrefersLinearSearch<TKey, TLess>() &&
        (CCowTreeHasCompare<TLess, TKey, TKey> || CowTreeStdOrder<TKey, TLess>() != 0);
}

template <class TLess, class TStored, class TAnyKey>
    requires CCowTreeThreeWayLookup<TLess, TStored, TAnyKey>
std::weak_ordering CowTreeCompare(const TLess& less, const TStored& stored, const TAnyKey& key)
{
    if constexpr (CCowTreeHasCompare<TLess, TStored, TAnyKey>) {
        return less.Compare(stored, key);
    } else if constexpr (CowTreeStdOrder<TStored, TLess>() > 0) {
        return stored <=> key;
    } else {
        return key <=> stored;
    }
}

template <class TKey, class TLess>
consteval ECowTreeSearch CowTreeReadSearch()
{
    if constexpr (CHasReadSearch<TLess>) {
        return TLess::ReadSearch;
    } else if constexpr (CHasReadSearch<TKey>) {
        return TKey::ReadSearch;
    } else {
        // With indirect keys, a predicted branch can overlap the next key load with the current comparison.
        return CowTreePrefersLinearSearch<TKey, TLess>()
            ? ECowTreeSearch::BranchlessBinary
            : ECowTreeSearch::BranchyBinary;
    }
}

template <class TKey, class TLess>
consteval ECowTreeSearch CowTreeWriteSearch()
{
    if constexpr (CHasWriteSearch<TLess>) {
        return TLess::WriteSearch;
    } else if constexpr (CHasWriteSearch<TKey>) {
        return TKey::WriteSearch;
    } else {
        return CowTreePrefersLinearSearch<TKey, TLess>()
            ? ECowTreeSearch::HybridLinear
            : ECowTreeSearch::BranchyBinary;
    }
}

constexpr int CowTreeDefaultLinearSearchLimit(int maxNodeSize)
{
    return maxNodeSize <= 64 ? maxNodeSize : 32;
}

template <int MaxNodeSizeArg, int LinearSearchLimitArg>
template <class TNode, class... TArgs>
TNode* TCowTreeStConfig<MaxNodeSizeArg, LinearSearchLimitArg>::CreateNode(TArgs&&... args)
{
    return new TNode(std::forward<TArgs>(args)...);
}

template <int MaxNodeSizeArg, int LinearSearchLimitArg>
template <class TNode>
void TCowTreeStConfig<MaxNodeSizeArg, LinearSearchLimitArg>::DeleteNode(TNode* node)
{
    delete node;
}

template <int MaxNodeSizeArg, int LinearSearchLimitArg>
template <class TNode, class... TArgs>
TNode* TCowTreeMtConfig<MaxNodeSizeArg, LinearSearchLimitArg>::CreateNode(TArgs&&... args)
{
    return new TNode(std::forward<TArgs>(args)...);
}

template <int MaxNodeSizeArg, int LinearSearchLimitArg>
template <class TNode>
void TCowTreeMtConfig<MaxNodeSizeArg, LinearSearchLimitArg>::DeleteNode(TNode* node)
{
    delete node;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
