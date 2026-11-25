#pragma once

#include "cow_tree_value.h"

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/misc/property.h>

#include <util/system/types.h>

#include <iterator>
#include <optional>
#include <tuple>
#include <utility>

#ifdef __GNUC__
    #define COW_TREE_LIKELY(x) (__builtin_expect((x), 1))
    #define COW_TREE_UNLIKELY(x) (__builtin_expect((x), 0))
#else
    #define COW_TREE_LIKELY(x) (x)
    #define COW_TREE_UNLIKELY(x) (x)
#endif

namespace NYT::NFlow::NCowTreeInternals {

////////////////////////////////////////////////////////////////////////////////

struct TNode
{
    int Size;
    bool IsLeaf;
    //! The tree version this node was last written at (stamped by the master);
    //! compared against the newest live snapshot version to tell exclusive nodes
    //! from shared ones. Written and read by the master thread only.
    ui64 Version = 0;
    //! The intrusive list of the master's live iterators parked on this node (used
    //! for leaves only); type-erased to keep TNode config-agnostic. Master-only,
    //! like Version: snapshots neither read nor write it, so master writes through
    //! shared nodes are safe.
    void* TrackedIterators = nullptr;

    TNode(int size, bool isLeaf);
};

//! Intermediate base carrying the reference counter; gives counter access
//! through a plain #TNode pointer without dispatching on the node kind.
template <class TRefCounter>
struct TCountedNode
    : TNode
{
    TRefCounter RefCounter;

    TCountedNode(int size, bool isLeaf);
};

//! The per-slot payload of a leaf: keys alone for a set, keys plus mapped values
//! for a map. Values ride the generic member-tuple machinery for free.
template <class TConfig, class TValue>
struct TLeafData
{
    TValueHolder<typename TConfig::TKey> Keys[TConfig::MaxNodeSize];
    TValueHolder<TValue> Values[TConfig::MaxNodeSize];

    static constexpr auto Members = std::make_tuple(&TLeafData::Keys, &TLeafData::Values);
};

template <class TConfig>
struct TLeafData<TConfig, void>
{
    TValueHolder<typename TConfig::TKey> Keys[TConfig::MaxNodeSize];

    static constexpr auto Members = std::make_tuple(&TLeafData::Keys);
};

template <class TConfig>
struct TLeaf
    : TCountedNode<typename TConfig::TRefCounter>
    , TLeafData<TConfig, typename TConfig::TValue>
{
    using TTreeConfig = TConfig;
    using TKey = typename TConfig::TKey;
    using TValue = typename TConfig::TValue;

    static constexpr bool HasChildren = false;
    static constexpr int MaxNodeSize = TConfig::MaxNodeSize;
    static constexpr auto Members = TLeafData<TConfig, TValue>::Members;

    TLeaf();

    //! The sibling hint matches the #TInner signature used by the shared machinery.
    TLeaf(TLeaf*);
};

//! Inner node in the leading-key layout: |Size| children and |Size - 1| keys, where
//! Keys[i] (i >= 1) is the exact max of the subtree of Children[i - 1] — the separator
//! between Children[i - 1] and Children[i]. Keys[0] is permanently vacant; it briefly
//! hosts a parent separator while node data is redistributed between siblings.
template <class TConfig>
struct TInner
    : TCountedNode<typename TConfig::TRefCounter>
{
    using TTreeConfig = TConfig;
    using TKey = typename TConfig::TKey;

    static constexpr bool HasChildren = true;
    static constexpr int MaxNodeSize = TConfig::MaxNodeSize;
    TValueHolder<TKey> Keys[MaxNodeSize];
    TNode* Children[MaxNodeSize] = {};
    static constexpr auto Members = std::make_tuple(&TInner::Keys, &TInner::Children);

    TInner();

    TInner(TInner*);

    template <std::convertible_to<TKey> TKeyRef>
    TInner(TKeyRef&& separator, TNode* child1, TNode* child2);
};

//! Discriminates the two node kinds inside the shared machinery.
template <class TAnyNode>
constexpr bool NodeHasChildren = TAnyNode::HasChildren;

////////////////////////////////////////////////////////////////////////////////

//! Reference-count operations over type-erased node pointers. Destruction
//! dispatches on #TNode::IsLeaf and recursively releases the children.
template <class TConfig>
void RefNode(TNode* node);

template <class TConfig>
void UnrefNode(TNode* node);

template <class TConfig>
void DestroyNode(TNode* node);

////////////////////////////////////////////////////////////////////////////////

// The multi-node operations below take |separators| — the NumNodes - 1 parent key
// slots between the participating siblings (parent Keys[first + 1 .. first + NumNodes)).
// For inner groups the separators are injected into the vacant Keys[0] of every node but the
// first, redistributed uniformly as (leading key, child) pairs, and extracted back from
// the resulting node heads. For leaf groups the separators are rewritten as copies of the new
// leaf maxima. A shrink leaves the dying separator slot (between the last survivor and the
// dropped node) empty; the caller erases that (empty key, child) pair from the parent
// with ClearKey = false.

template <class TAnyNode, class TTuple>
void NodeInsertSimple(TAnyNode* node, int pos, TTuple&& value);

template <size_t NumNodes, size_t InsertIdx, class TAnyNode, class TKey, class TTuple>
void NodeInsertRedistribute(TNode** nodes, TValueHolder<TKey>* separators, int pos, TTuple&& value, int totalSize);

template <size_t NumNodes, size_t InsertIdx, class TAnyNode, class TKey, class TTuple>
void NodeInsertExpand(TNode** nodes, TValueHolder<TKey>* separators, TAnyNode* newNode, TValueHolder<TKey>& upSeparator, int pos, TTuple&& value, int totalSize);

template <size_t NumNodes, size_t InsertIdx, class TAnyNode, class TKey, class TTuple>
std::optional<std::tuple<TKey, TNode*>> NodeInsertComplex(TNode** nodes, TValueHolder<TKey>* separators, int pos, TTuple&& value);

////////////////////////////////////////////////////////////////////////////////

template <bool ClearKey, class TAnyNode>
void NodeEraseSimple(TAnyNode* node, int pos);

template <bool ClearKey, size_t NumNodes, size_t EraseIdx, class TAnyNode, class TKey>
void NodeEraseRedistribute(TNode** nodes, TValueHolder<TKey>* separators, int pos, int totalSize);

template <bool ClearKey, size_t NumNodes, size_t EraseIdx, class TAnyNode, class TKey>
void NodeEraseShrink(TNode** nodes, TValueHolder<TKey>* separators, int pos, int totalSize);

template <bool ClearKey, size_t NumNodes, size_t EraseIdx, class TAnyNode, class TKey>
bool NodeEraseComplex(TNode** nodes, TValueHolder<TKey>* separators, int pos);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCowTreeInternals

#define COW_TREE_NODE_INL_H
#include "cow_tree_node-inl.h"
#undef COW_TREE_NODE_INL_H
