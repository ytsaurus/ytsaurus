#pragma once

#include "cow_tree_value.h"

#include <library/cpp/yt/misc/enum.h>

#include <util/system/spinlock.h>
#include <util/system/types.h>

#include <compare>
#include <concepts>
#include <functional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace NYT::NFlow {
namespace NCowTreeInternals {

//! A stand-in node type for probing TConfig::CreateNode in the concept below
//! (the real node types cannot be named here without a circular dependency).
struct TCowTreeConfigProbe
{ };

template <class TConfig, class TNode, class... TArgs>
TNode* CreateCowTreeNode(TArgs&&... args) noexcept;

template <class TConfig, class TNode>
void DeleteCowTreeNode(TNode* node) noexcept;

} // namespace NCowTreeInternals

////////////////////////////////////////////////////////////////////////////////

//! Everything the tree needs comes from a single config structure; see the
//! spelled-out example atop cow_tree.h and the presets at the bottom of this
//! file. The members, all mandatory:
//!   - TKey — stored by value in the leaves;
//!   - TValue — void makes the tree a set; anything else is stored inline in
//!     the leaves next to its key (use a refcounted pointer for shared or
//!     bulky values);
//!   - TLess — a strict weak order over TKey; a stateful one (a function
//!     pointer, a functor carrying data) is handed to the master's constructor
//!     and inherited by its snapshots. Declare is_transparent to enable
//!     std-style heterogeneous lookup, and see the ReadSearch/WriteSearch/
//!     PrefersLinearSearch traits below for per-key in-node search tuning;
//!   - TRefCounter — TAtomicRefCounter lets snapshots cross threads,
//!     TPlainRefCounter declares the whole family single-threaded;
//!   - TMutex — guards the view list: a real lock next to TAtomicRefCounter,
//!     TNullMutex next to TPlainRefCounter;
//!   - MaxNodeSize — elements per node: even, at least 4, 64 is the tuned
//!     default;
//!   - LinearSearchLimit — the linear-tail length of the HybridLinear in-node
//!     search flavour (CowTreeDefaultLinearSearchLimit(MaxNodeSize) unless
//!     measured otherwise);
//!   - MaxSize — the capacity bound sizing the fixed descent stack; a hard
//!     contract, not a hint: growing the tree beyond the height budget derived
//!     from it fails loudly (in release too) at the root split;
//!   - CreateNode<TNode>(args...) / DeleteNode(node) — the allocation hook pair;
//!     everything the tree allocates, its nodes and the shared core alike, is born
//!     and dies through these two, so a pool or slab allocator can replace both.
template <class TConfig>
concept CCowTreeConfig = requires(typename TConfig::TKey key, const typename TConfig::TLess& less) {
    typename TConfig::TKey;
    typename TConfig::TValue;
    typename TConfig::TLess;
    requires std::is_nothrow_move_constructible_v<typename TConfig::TKey>;
    requires std::is_void_v<typename TConfig::TValue> || std::is_nothrow_move_constructible_v<typename TConfig::TValue>;
    typename TConfig::TRefCounter;
    typename TConfig::TMutex;
    {
        less(key, key)
    } -> std::convertible_to<bool>;
    requires std::convertible_to<decltype(TConfig::MaxNodeSize), int>;
    requires std::convertible_to<decltype(TConfig::LinearSearchLimit), int>;
    requires std::convertible_to<decltype(TConfig::MaxSize), ui64>;
    {
        TConfig::template CreateNode<NCowTreeInternals::TCowTreeConfigProbe>()
    }
    -> std::same_as<NCowTreeInternals::TCowTreeConfigProbe*>;
    {
        TConfig::DeleteNode(std::declval<NCowTreeInternals::TCowTreeConfigProbe*>())
    }
    -> std::same_as<void>;
};

//! The tallest tree that can exist while holding at most |maxSize| keys, derived
//! from the minimal sustained node fill: the root keeps at least two children,
//! inner levels sustain at least 2/3 of MaxNodeSize per node, and the leaf level
//! is counted at MaxNodeSize/2 — an individual leaf may thin down to half a node
//! while its sibling group still holds 2/3 per node on average. One extra level
//! absorbs the transient underfill of two-sibling erase redistributes.
consteval int ComputeCowTreeMaxHeight(ui64 maxNodeSize, ui64 maxSize);

//! A comparator supporting std-style heterogeneous lookup: is_transparent promises
//! that operator() accepts mixed argument types whose ordering is consistent with
//! the stored-key one.
template <class TLess>
concept CCowTreeTransparentLess = requires {
    typename TLess::is_transparent;
};

//! Declared by a comparator or a key type to force the in-node linear-tail policy
//! regardless of the built-in default:
//! `static constexpr bool PrefersLinearSearch = ...;`.
template <class T>
concept CHasLinearSearchPreference = requires {
    {
        T::PrefersLinearSearch
    } -> std::convertible_to<bool>;
};

//! The comparator may override the key's linear-search preference via PrefersLinearSearch.
template <class TKey, class TLess>
consteval bool CowTreePrefersLinearSearch();

//! An explicit three-way comparator for branchy binary search.
template <class TLess, class TStored, class TAnyKey>
concept CCowTreeHasCompare = requires(const TLess& less, const TStored& stored, const TAnyKey& key) {
    {
        less.Compare(stored, key)
    } -> std::convertible_to<std::weak_ordering>;
};

//! Only the standard string order has an implicit three-way fast path.
//! Custom comparators can provide #Compare() with their own ordering.
template <class TKey, class TLess>
consteval int CowTreeStdOrder();

template <class TKey, class TLess>
consteval bool CowTreeComparesThreeWay();

//! Whether |less| can order a stored key against |key| three ways (the lookup key
//! may be of another type under a transparent comparator).
template <class TLess, class TStored, class TAnyKey>
concept CCowTreeThreeWayLookup = CCowTreeHasCompare<TLess, TStored, TAnyKey> ||
    (CowTreeStdOrder<TStored, TLess>() != 0 && (std::same_as<TAnyKey, std::string> || std::same_as<TAnyKey, std::string_view>) && requires(const TStored& stored, const TAnyKey& key) {
        {
            stored <=> key
        } -> std::convertible_to<std::weak_ordering>;
    });

template <class TLess, class TStored, class TAnyKey>
    requires CCowTreeThreeWayLookup<TLess, TStored, TAnyKey>
std::weak_ordering CowTreeCompare(const TLess& less, const TStored& stored, const TAnyKey& key);

//! In-node search strategies for read and write lookups.
DEFINE_ENUM(ECowTreeSearch,
    (BranchlessBinary)
    (BranchyBinary)
    (HybridLinear)
);

//! Declared by a comparator or a key type to pin a search flavour per direction:
//! `static constexpr ECowTreeSearch ReadSearch/WriteSearch = ...;`.
template <class T>
concept CHasReadSearch = requires {
    {
        T::ReadSearch
    } -> std::convertible_to<ECowTreeSearch>;
};

template <class T>
concept CHasWriteSearch = requires {
    {
        T::WriteSearch
    } -> std::convertible_to<ECowTreeSearch>;
};

template <class TKey, class TLess>
consteval ECowTreeSearch CowTreeReadSearch();

template <class TKey, class TLess>
consteval ECowTreeSearch CowTreeWriteSearch();

//! Default linear-tail length for the hybrid search.
constexpr int CowTreeDefaultLinearSearchLimit(int maxNodeSize);

//! The behavioural half of a configuration — everything but the stored types.
//! Single-threaded flavour: no atomics anywhere, snapshots included; the tree and
//! all its views must stay on one thread.
template <int MaxNodeSizeArg = 64, int LinearSearchLimitArg = CowTreeDefaultLinearSearchLimit(MaxNodeSizeArg)>
struct TCowTreeStConfig
{
    using TRefCounter = NCowTreeInternals::TPlainRefCounter;
    using TMutex = NCowTreeInternals::TNullMutex;

    static constexpr int MaxNodeSize = MaxNodeSizeArg;
    //! The linear-tail length of the HybridLinear search flavour; meaningless for
    //! the binary flavours (the flavour itself is chosen by ECowTreeSearch traits).
    static constexpr int LinearSearchLimit = LinearSearchLimitArg;
    static constexpr ui64 MaxSize = ui64(1) << 32;

    template <class TNode, class... TArgs>
    static TNode* CreateNode(TArgs&&... args);

    template <class TNode>
    static void DeleteNode(TNode* node);
};

//! Concurrent flavour: reference counting is atomic and the view list is guarded by
//! a real lock, so snapshots may be copied, read and destroyed on any thread. The
//! master itself stays single-threaded.
template <int MaxNodeSizeArg = 64, int LinearSearchLimitArg = CowTreeDefaultLinearSearchLimit(MaxNodeSizeArg)>
struct TCowTreeMtConfig
{
    using TRefCounter = NCowTreeInternals::TAtomicRefCounter;
    using TMutex = TAdaptiveLock;

    static constexpr int MaxNodeSize = MaxNodeSizeArg;
    static constexpr int LinearSearchLimit = LinearSearchLimitArg;
    static constexpr ui64 MaxSize = ui64(1) << 32;

    template <class TNode, class... TArgs>
    static TNode* CreateNode(TArgs&&... args);

    template <class TNode>
    static void DeleteNode(TNode* node);
};

//! Completes a behavioural preset with the stored types into a full CCowTreeConfig.
template <class TBaseConfig, class TKeyArg, class TLessArg = std::less<TKeyArg>, class TValueArg = void>
struct TCowTreeConfig
    : TBaseConfig
{
    using TKey = TKeyArg;
    using TLess = TLessArg;
    using TValue = TValueArg;
};

//! Single-threaded full-configuration preset.
template <class TKeyArg, int MaxNodeSizeArg = 64, class TLessArg = std::less<TKeyArg>, class TValueArg = void>
using TCowTreeDefaultConfig = TCowTreeConfig<TCowTreeStConfig<MaxNodeSizeArg>, TKeyArg, TLessArg, TValueArg>;

//! Full-configuration preset whose snapshots may cross threads.
template <class TKeyArg, int MaxNodeSizeArg = 64, class TLessArg = std::less<TKeyArg>, class TValueArg = void>
using TCowTreeConcurrentConfig = TCowTreeConfig<TCowTreeMtConfig<MaxNodeSizeArg>, TKeyArg, TLessArg, TValueArg>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define COW_TREE_CONFIG_INL_H
#include "cow_tree_config-inl.h"
#undef COW_TREE_CONFIG_INL_H
