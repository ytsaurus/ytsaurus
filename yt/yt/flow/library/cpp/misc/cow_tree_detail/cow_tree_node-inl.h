#ifndef COW_TREE_NODE_INL_H
    #error "Direct inclusion of this file is not allowed, include cow_tree_node.h"
    // For the sake of sane code completion.
    #include "cow_tree_node.h"
#endif

namespace NYT::NFlow::NCowTreeInternals {

////////////////////////////////////////////////////////////////////////////////

inline TNode::TNode(int size, bool isLeaf)
    : Size(size)
    , IsLeaf(isLeaf)
{ }

template <class TRefCounter>
TCountedNode<TRefCounter>::TCountedNode(int size, bool isLeaf)
    : TNode(size, isLeaf)
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TConfig>
TLeaf<TConfig>::TLeaf()
    : TCountedNode<typename TConfig::TRefCounter>(0, /*isLeaf*/ true)
{ }

template <class TConfig>
TLeaf<TConfig>::TLeaf(TLeaf*)
    : TCountedNode<typename TConfig::TRefCounter>(0, /*isLeaf*/ true)
{ }

template <class TConfig>
TInner<TConfig>::TInner()
    : TCountedNode<typename TConfig::TRefCounter>(0, /*isLeaf*/ false)
{ }

template <class TConfig>
TInner<TConfig>::TInner(TInner*)
    : TCountedNode<typename TConfig::TRefCounter>(0, /*isLeaf*/ false)
{ }

template <class TConfig>
template <std::convertible_to<typename TConfig::TKey> TKeyRef>
TInner<TConfig>::TInner(TKeyRef&& separator, TNode* child1, TNode* child2)
    : TCountedNode<typename TConfig::TRefCounter>(2, /*isLeaf*/ false)
{
    Keys[1].Set(std::forward<TKeyRef>(separator));
    Children[0] = child1;
    Children[1] = child2;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTuple>
constexpr auto IndexSequenceForTuple(TTuple&&)
{
    return std::make_index_sequence<std::tuple_size_v<std::remove_cvref_t<TTuple>>>();
}

template <size_t Addend, size_t... I>
constexpr auto IndexSequenceAdd(std::index_sequence<I...>)
{
    return std::index_sequence<Addend + I...>();
}

////////////////////////////////////////////////////////////////////////////////

template <class TAnyNode>
struct TNodeDataRange
{
    TAnyNode* NodePtr;
    int NodeIdx;
    int Begin;
    int End;

    int Size() const
    {
        return End - Begin;
    }

    auto Origin() const
    {
        return std::tie(NodeIdx, Begin);
    }
};

template <class TAnyNode>
struct TNodeDataRoute
{
    TNodeDataRange<TAnyNode> Src;
    TNodeDataRange<TAnyNode> Dst;
};

////////////////////////////////////////////////////////////////////////////////

template <bool Backward, class TAnyNode>
void NodeDataMove(TAnyNode* src, int srcFrom, int srcTo, TAnyNode* dst, int dstPos)
{
    [&]<size_t... I>(std::index_sequence<I...>) {
        if constexpr (Backward) {
            (HolderOrValueMoveBackward((src->*(std::get<I>(TAnyNode::Members))) + srcFrom, (src->*(std::get<I>(TAnyNode::Members))) + srcTo, (dst->*(std::get<I>(TAnyNode::Members))) + dstPos), ...);
        } else {
            (HolderOrValueMove((src->*(std::get<I>(TAnyNode::Members))) + srcFrom, (src->*(std::get<I>(TAnyNode::Members))) + srcTo, (dst->*(std::get<I>(TAnyNode::Members))) + dstPos), ...);
        }
    }(IndexSequenceForTuple(TAnyNode::Members));
}

template <class TAnyNode, class TTuple>
void NodeDataSet(TAnyNode* node, int pos, TTuple&& value)
{
    auto& key = node->Keys[pos];
    HolderOrValueAssign(key, std::forward<std::tuple_element_t<0, std::remove_reference_t<TTuple>>>(std::get<0>(value)));
    if constexpr (NodeHasChildren<TAnyNode>) {
        node->Children[pos] = std::get<1>(value);
    } else if constexpr (requires { node->Values[pos]; }) {
        HolderOrValueAssign(node->Values[pos], std::forward<std::tuple_element_t<1, std::remove_reference_t<TTuple>>>(std::get<1>(value)));
    }
}

template <class TAnyNode>
void NodeDataClear(TAnyNode* node, int pos)
{
    [&]<size_t... I>(std::index_sequence<I...>) {
        ((HolderOrValueClear((node->*(std::get<I>(TAnyNode::Members)))[pos])), ...);
    }(IndexSequenceForTuple(TAnyNode::Members));
}

////////////////////////////////////////////////////////////////////////////////

template <bool IsInsert, class TAnyNode, size_t NumSrc, size_t NumDst, class TTuple>
void NodeRedistribute(TNodeDataRange<TAnyNode> (&src)[NumSrc], TNodeDataRange<TAnyNode> (&dst)[NumDst], [[maybe_unused]] TTuple&& value)
{
    TNodeDataRoute<TAnyNode> routes[NumSrc + NumDst - 1];
    int numRoutes = 0;
    [[maybe_unused]] TNodeDataRange<TAnyNode> finalInsert{};
    for (int i = 0, j = 0; i < std::ssize(src) && j < std::ssize(dst);) {
        routes[numRoutes].Src = src[i];
        routes[numRoutes].Dst = dst[j];
        int countDiff = src[i].Size() - dst[j].Size();
        if (countDiff < 0) {
            i++;
            dst[j].Begin = routes[numRoutes].Dst.End += countDiff;
        } else if (countDiff > 0) {
            src[i].Begin = routes[numRoutes].Src.End -= countDiff;
            j++;
        } else {
            i++;
            j++;
        }
        YT_ASSERT(routes[numRoutes].Src.Size() == routes[numRoutes].Dst.Size());
        if (routes[numRoutes].Src.Size() != 0 && routes[numRoutes].Src.Origin() != routes[numRoutes].Dst.Origin()) {
            if constexpr (IsInsert) {
                if (!routes[numRoutes].Src.NodePtr) {
                    finalInsert = routes[numRoutes].Dst;
                } else {
                    numRoutes++;
                }
            } else {
                numRoutes++;
            }
        }
    }
    for (int i = 0; i < numRoutes; i++) {
        if (routes[i].Src.Origin() > routes[i].Dst.Origin()) {
            NodeDataMove<false>(routes[i].Src.NodePtr, routes[i].Src.Begin, routes[i].Src.End, routes[i].Dst.NodePtr, routes[i].Dst.Begin);
        }
    }
    for (int i = numRoutes - 1; i >= 0; i--) {
        if (routes[i].Src.Origin() < routes[i].Dst.Origin()) {
            NodeDataMove<true>(routes[i].Src.NodePtr, routes[i].Src.Begin, routes[i].Src.End, routes[i].Dst.NodePtr, routes[i].Dst.End);
        }
    }
    if constexpr (IsInsert) {
        YT_ASSERT(finalInsert.Size() == 1);
        NodeDataSet(static_cast<TAnyNode*>(finalInsert.NodePtr), finalInsert.Begin, value);
    }
}

//! Moves the parent separators of an inner sibling group into the vacant Keys[0] of every
//! node but the first, turning the group into a uniform stream of (leading key, child)
//! pairs. No-op for leaf groups.
template <class TAnyNode, class TKey, size_t NumSeparators>
void InjectSeparators(TNode** nodes, TValueHolder<TKey>* separators)
{
    if constexpr (NodeHasChildren<TAnyNode>) {
        [&]<size_t... J>(std::index_sequence<J...>) {
            ((static_cast<TAnyNode*>(nodes[J + 1])->Keys[0].MoveFrom(std::move(separators[J]))), ...);
        }(std::make_index_sequence<NumSeparators>());
    }
}

//! The reverse of #InjectSeparators: for inner groups moves the new node-head keys back
//! into the parent separator slots; for leaf groups rewrites the separators as copies of the new
//! leaf maxima.
template <class TAnyNode, class TKey, size_t NumSeparators>
void ExtractSeparators(TNode** nodes, TValueHolder<TKey>* separators)
{
    [&]<size_t... J>(std::index_sequence<J...>) {
        if constexpr (NodeHasChildren<TAnyNode>) {
            ((separators[J].MoveFrom(std::move(static_cast<TAnyNode*>(nodes[J + 1])->Keys[0]))), ...);
        } else {
            ((separators[J].Clear(),
                separators[J].CopyFrom(static_cast<TAnyNode*>(nodes[J])->Keys[nodes[J]->Size - 1])),
                ...);
        }
    }(std::make_index_sequence<NumSeparators>());
}

////////////////////////////////////////////////////////////////////////////////

template <class TAnyNode, class TTuple>
void NodeInsertSimple(TAnyNode* node, int pos, TTuple&& value)
{
    YT_ASSERT(!NodeHasChildren<TAnyNode> || pos >= 1);
    NodeDataMove<true>(node, pos, node->Size, node, node->Size + 1);
    NodeDataSet(node, pos, std::forward<TTuple>(value));
    node->Size++;
}

////////////////////////////////////////////////////////////////////////////////

template <size_t NumNodes, size_t M, class TAnyNode, class TKey, class TTuple, size_t... L, size_t... R, size_t... A>
void NodeInsertRedistribute(TNode** nodes, TValueHolder<TKey>* separators, int pos, TTuple&& value, int totalSize, std::index_sequence<L...>, std::index_sequence<R...>, std::index_sequence<A...>)
{
    constexpr int maxNodeSize = TAnyNode::MaxNodeSize;
    auto node = [&] (size_t I) {
        return static_cast<TAnyNode*>(nodes[I]);
    };
    YT_ASSERT(node(M)->Size == maxNodeSize);
    YT_ASSERT(totalSize == (node(L)->Size + ... + 0) + maxNodeSize + 1 + (node(R)->Size + ... + 0));
    YT_ASSERT(totalSize <= static_cast<int>(maxNodeSize * NumNodes));

    InjectSeparators<TAnyNode, TKey, NumNodes - 1>(nodes, separators);

    TNodeDataRange<TAnyNode> src[NumNodes + 2] = {{node(L), L, 0, nodes[L]->Size}..., {node(M), M, 0, pos}, {nullptr, -1, 0, 1}, {node(M), M, pos, maxNodeSize}, {node(R), R, 0, nodes[R]->Size}...};
    YT_ASSERT(src[0].Size() + src[1].Size() + (src[A + 2].Size() + ...) == totalSize);

    int dstSizes[NumNodes] = {static_cast<int>((totalSize + A) / NumNodes)...};
    TNodeDataRange<TAnyNode> dst[NumNodes] = {{node(A), A, 0, dstSizes[A]}...};
    YT_ASSERT((dst[A].Size() + ...) == totalSize);

    NodeRedistribute<true>(src, dst, std::forward<TTuple>(value));
    ((node(A)->Size = dstSizes[A]), ...);
    ExtractSeparators<TAnyNode, TKey, NumNodes - 1>(nodes, separators);
}

template <size_t NumNodes, size_t InsertIdx, class TAnyNode, class TKey, class TTuple>
void NodeInsertRedistribute(TNode** nodes, TValueHolder<TKey>* separators, int pos, TTuple&& value, int totalSize)
{
    static_assert(InsertIdx < NumNodes);
    constexpr auto leftIdx = std::make_index_sequence<InsertIdx>();
    constexpr auto rightIdx = IndexSequenceAdd<InsertIdx + 1>(std::make_index_sequence<NumNodes - InsertIdx - 1>());
    constexpr auto allIdx = std::make_index_sequence<NumNodes>();
    NodeInsertRedistribute<NumNodes, InsertIdx, TAnyNode>(nodes, separators, pos, std::forward<TTuple>(value), totalSize, leftIdx, rightIdx, allIdx);
}

////////////////////////////////////////////////////////////////////////////////

template <size_t NumNodes, size_t M, class TAnyNode, class TKey, class TTuple, size_t... L, size_t... R, size_t... A>
void NodeInsertExpand(TNode** nodes, TValueHolder<TKey>* separators, TAnyNode* newNode, TValueHolder<TKey>& upSeparator, int pos, TTuple&& value, int totalSize, std::index_sequence<L...>, std::index_sequence<R...>, std::index_sequence<A...>)
{
    constexpr int maxNodeSize = TAnyNode::MaxNodeSize;
    auto node = [&] (size_t I) {
        return static_cast<TAnyNode*>(nodes[I]);
    };
    YT_ASSERT(node(M)->Size == maxNodeSize);
    YT_ASSERT(totalSize == (node(L)->Size + ... + 0) + maxNodeSize + 1 + (node(R)->Size + ... + 0));
    YT_ASSERT(totalSize <= static_cast<int>(maxNodeSize * (NumNodes + 1)));

    InjectSeparators<TAnyNode, TKey, NumNodes - 1>(nodes, separators);

    TNodeDataRange<TAnyNode> src[NumNodes + 2] = {{node(L), L, 0, nodes[L]->Size}..., {node(M), M, 0, pos}, {nullptr, -1, 0, 1}, {node(M), M, pos, maxNodeSize}, {node(R), R, 0, nodes[R]->Size}...};
    YT_ASSERT(src[0].Size() + src[1].Size() + (src[A + 2].Size() + ...) == totalSize);

    int dstSizes[NumNodes + 1] = {static_cast<int>((totalSize + A) / (NumNodes + 1))..., static_cast<int>((totalSize + NumNodes) / (NumNodes + 1))};
    TNodeDataRange<TAnyNode> dst[NumNodes + 1] = {{node(A), A, 0, dstSizes[A]}..., {newNode, NumNodes, 0, dstSizes[NumNodes]}};
    YT_ASSERT(dst[0].Size() + (dst[A + 1].Size() + ...) == totalSize);

    NodeRedistribute<true>(src, dst, std::forward<TTuple>(value));
    ((node(A)->Size = dstSizes[A]), ...);
    newNode->Size = dstSizes[NumNodes];
    ExtractSeparators<TAnyNode, TKey, NumNodes - 1>(nodes, separators);
    // The separator between the last old node and the new one is handed up to the caller.
    if constexpr (NodeHasChildren<TAnyNode>) {
        upSeparator.MoveFrom(std::move(newNode->Keys[0]));
    } else {
        upSeparator.CopyFrom(node(NumNodes - 1)->Keys[nodes[NumNodes - 1]->Size - 1]);
    }
}

template <size_t NumNodes, size_t InsertIdx, class TAnyNode, class TKey, class TTuple>
void NodeInsertExpand(TNode** nodes, TValueHolder<TKey>* separators, TAnyNode* newNode, TValueHolder<TKey>& upSeparator, int pos, TTuple&& value, int totalSize)
{
    static_assert(InsertIdx < NumNodes);
    constexpr auto leftIdx = std::make_index_sequence<InsertIdx>();
    constexpr auto rightIdx = IndexSequenceAdd<InsertIdx + 1>(std::make_index_sequence<NumNodes - InsertIdx - 1>());
    constexpr auto allIdx = std::make_index_sequence<NumNodes>();
    NodeInsertExpand<NumNodes, InsertIdx, TAnyNode>(nodes, separators, newNode, upSeparator, pos, std::forward<TTuple>(value), totalSize, leftIdx, rightIdx, allIdx);
}

////////////////////////////////////////////////////////////////////////////////

template <size_t NumNodes, size_t M, class TAnyNode, class TKey, class TTuple, size_t... L, size_t... R, size_t... A>
std::optional<std::tuple<TKey, TNode*>> NodeInsertComplex(TNode** nodes, TValueHolder<TKey>* separators, int pos, TTuple&& value, std::index_sequence<L...>, std::index_sequence<R...>, std::index_sequence<A...>)
{
    constexpr int maxNodeSize = TAnyNode::MaxNodeSize;
    YT_ASSERT(nodes[M]->Size == maxNodeSize);

    // An append past the end of the group's last leaf splits the new key off alone
    // (the absl split bias): sequential keys then fill every leaf to the brim
    // without moving a single element, and the tail leaf grows into a full one
    // before the next split. An underfull tail is nothing the erase side does not
    // handle already. Inner nodes keep the uniform expand — they hold two children
    // at least.
    if constexpr (M == NumNodes - 1 && !NodeHasChildren<TAnyNode>) {
        if (pos == maxNodeSize) {
            auto* last = static_cast<TAnyNode*>(nodes[M]);
            auto* newNode = CreateCowTreeNode<typename TAnyNode::TTreeConfig, TAnyNode>(last);
            TValueHolder<TKey> separator;
            separator.CopyFrom(last->Keys[maxNodeSize - 1]);
            NodeInsertSimple(newNode, 0, std::forward<TTuple>(value));
            std::optional<std::tuple<TKey, TNode*>> result(std::in_place, std::move(separator.Get()), static_cast<TNode*>(newNode));
            separator.Clear();
            return result;
        }
    }

    int totalSize = (nodes[L]->Size + ... + 0) + maxNodeSize + 1 + (nodes[R]->Size + ... + 0);
    if (COW_TREE_LIKELY(totalSize < static_cast<int>(NumNodes * (maxNodeSize - 1)))) {
        NodeInsertRedistribute<NumNodes, M, TAnyNode>(nodes, separators, pos, std::forward<TTuple>(value), totalSize);
        return std::nullopt;
    } else {
        auto* newNode = CreateCowTreeNode<typename TAnyNode::TTreeConfig, TAnyNode>(static_cast<TAnyNode*>(nodes[NumNodes - 1]));
        TValueHolder<TKey> upSeparator;
        NodeInsertExpand<NumNodes, M, TAnyNode>(nodes, separators, newNode, upSeparator, pos, std::forward<TTuple>(value), totalSize);
        std::optional<std::tuple<TKey, TNode*>> result(std::in_place, std::move(upSeparator.Get()), static_cast<TNode*>(newNode));
        upSeparator.Clear();
        return result;
    }
}

template <size_t NumNodes, size_t InsertIdx, class TAnyNode, class TKey, class TTuple>
std::optional<std::tuple<TKey, TNode*>> NodeInsertComplex(TNode** nodes, TValueHolder<TKey>* separators, int pos, TTuple&& value)
{
    static_assert(InsertIdx < NumNodes);
    constexpr auto leftIdx = std::make_index_sequence<InsertIdx>();
    constexpr auto rightIdx = IndexSequenceAdd<InsertIdx + 1>(std::make_index_sequence<NumNodes - InsertIdx - 1>());
    constexpr auto allIdx = std::make_index_sequence<NumNodes>();
    return NodeInsertComplex<NumNodes, InsertIdx, TAnyNode>(nodes, separators, pos, std::forward<TTuple>(value), leftIdx, rightIdx, allIdx);
}

////////////////////////////////////////////////////////////////////////////////

template <bool ClearKey, class TAnyNode>
void NodeEraseSimple(TAnyNode* node, int pos)
{
    YT_ASSERT(node->Size > 1);
    YT_ASSERT(!NodeHasChildren<TAnyNode> || pos >= 1);
    if constexpr (ClearKey) {
        NodeDataClear(node, pos);
    }
    NodeDataMove<false>(node, pos + 1, node->Size, node, pos);
    node->Size--;
}

////////////////////////////////////////////////////////////////////////////////

template <bool ClearKey, size_t NumNodes, size_t M, class TAnyNode, class TKey, size_t... L, size_t... R, size_t... A>
void NodeEraseRedistribute(TNode** nodes, TValueHolder<TKey>* separators, int pos, int totalSize, std::index_sequence<L...>, std::index_sequence<R...>, std::index_sequence<A...>)
{
    auto node = [&] (size_t I) {
        return static_cast<TAnyNode*>(nodes[I]);
    };
    YT_ASSERT(totalSize == (nodes[A]->Size + ...) - 1);

    // Clear the erased element up front (with ClearKey = false its key is already gone).
    if constexpr (ClearKey) {
        NodeDataClear(node(M), pos);
    }

    InjectSeparators<TAnyNode, TKey, NumNodes - 1>(nodes, separators);

    TNodeDataRange<TAnyNode> src[NumNodes + 1] = {{node(L), L, 0, node(L)->Size}..., {node(M), M, 0, pos}, {node(M), M, pos + 1, node(M)->Size}, {node(R), R, 0, node(R)->Size}...};
    YT_ASSERT(src[0].Size() + (src[A + 1].Size() + ...) == totalSize);

    int dstSizes[NumNodes] = {static_cast<int>((totalSize + A) / NumNodes)...};
    TNodeDataRange<TAnyNode> dst[NumNodes] = {{node(A), A, 0, dstSizes[A]}...};
    YT_ASSERT((dst[A].Size() + ...) == totalSize);

    NodeRedistribute<false>(src, dst, std::make_tuple());
    ((node(A)->Size = dstSizes[A]), ...);
    ExtractSeparators<TAnyNode, TKey, NumNodes - 1>(nodes, separators);
}

template <bool ClearKey, size_t NumNodes, size_t EraseIdx, class TAnyNode, class TKey>
void NodeEraseRedistribute(TNode** nodes, TValueHolder<TKey>* separators, int pos, int totalSize)
{
    constexpr auto leftIdx = std::make_index_sequence<EraseIdx>();
    constexpr auto rightIdx = IndexSequenceAdd<EraseIdx + 1>(std::make_index_sequence<NumNodes - EraseIdx - 1>());
    constexpr auto allIdx = std::make_index_sequence<NumNodes>();
    NodeEraseRedistribute<ClearKey, NumNodes, EraseIdx, TAnyNode, TKey>(nodes, separators, pos, totalSize, leftIdx, rightIdx, allIdx);
}

////////////////////////////////////////////////////////////////////////////////

template <bool ClearKey, size_t NumNodes, size_t M, class TAnyNode, class TKey, size_t... L, size_t... R, size_t... A>
void NodeEraseShrink(TNode** nodes, TValueHolder<TKey>* separators, int pos, int totalSize, std::index_sequence<L...>, std::index_sequence<R...>, std::index_sequence<A...>)
{
    [[maybe_unused]] constexpr int maxNodeSize = TAnyNode::MaxNodeSize;
    auto node = [&] (size_t I) {
        return static_cast<TAnyNode*>(nodes[I]);
    };
    YT_ASSERT(totalSize == node(0)->Size + (node(A + 1)->Size + ...) - 1);
    YT_ASSERT(totalSize <= static_cast<int>(maxNodeSize * (NumNodes - 1)));

    // Clear the erased element up front (with ClearKey = false its key is already gone).
    if constexpr (ClearKey) {
        NodeDataClear(node(M), pos);
    }

    InjectSeparators<TAnyNode, TKey, NumNodes - 1>(nodes, separators);

    TNodeDataRange<TAnyNode> src[NumNodes + 1] = {{node(L), L, 0, node(L)->Size}..., {node(M), M, 0, pos}, {node(M), M, pos + 1, node(M)->Size}, {node(R), R, 0, node(R)->Size}...};
    YT_ASSERT(src[0].Size() + src[1].Size() + (src[A + 2].Size() + ...) == totalSize);

    int dstSizes[NumNodes - 1] = {static_cast<int>((totalSize + A) / (NumNodes - 1))...};
    TNodeDataRange<TAnyNode> dst[NumNodes - 1] = {{node(A), A, 0, dstSizes[A]}...};
    YT_ASSERT((dst[A].Size() + ...) == totalSize);

    NodeRedistribute<false>(src, dst, std::make_tuple());
    ((node(A)->Size = dstSizes[A]), ...);
    ExtractSeparators<TAnyNode, TKey, NumNodes - 2>(nodes, separators);
    // The dying separator (between the last survivor and the dropped node) ends up empty;
    // the caller erases its (key, child) pair from the parent with ClearKey = false.
    if constexpr (!NodeHasChildren<TAnyNode>) {
        separators[NumNodes - 2].Clear();
    }
}

template <bool ClearKey, size_t NumNodes, size_t EraseIdx, class TAnyNode, class TKey>
void NodeEraseShrink(TNode** nodes, TValueHolder<TKey>* separators, int pos, int totalSize)
{
    constexpr auto leftIdx = std::make_index_sequence<EraseIdx>();
    constexpr auto rightIdx = IndexSequenceAdd<EraseIdx + 1>(std::make_index_sequence<NumNodes - EraseIdx - 1>());
    constexpr auto allIdx = std::make_index_sequence<NumNodes - 1>();
    NodeEraseShrink<ClearKey, NumNodes, EraseIdx, TAnyNode>(nodes, separators, pos, totalSize, leftIdx, rightIdx, allIdx);
}

////////////////////////////////////////////////////////////////////////////////

template <bool ClearKey, size_t NumNodes, size_t M, class TAnyNode, class TKey, size_t... L, size_t... R, size_t... A>
bool NodeEraseComplex(TNode** nodes, TValueHolder<TKey>* separators, int pos, std::index_sequence<L...>, std::index_sequence<R...>, std::index_sequence<A...>)
{
    constexpr int maxNodeSize = TAnyNode::MaxNodeSize;
    constexpr int underfillBar = maxNodeSize * 2 / 3;

    int totalSize = (nodes[A]->Size + ... + 0) - 1;
    // Redistribute only when the equalized nodes all clear the underfill bar —
    // otherwise the group would stay pinned at the bar, redistributing again on
    // every subsequent erase — or when the group cannot fit into one node fewer
    // (a two-node group under a two-child parent may have no legal shrink).
    // Everything else shrinks right away; the shrunk nodes land between the bar
    // and a full node, so the memory contract (at most 1.5 slots per record on
    // average) holds on both branches.
    //
    // One more case keeps a node: the erase empties it while the survivors would
    // be packed nearly full by the shrink — the appended-key leaf drained right
    // after its split. Shrinking would hand the next append another split, and
    // the pair would keep splitting and merging; equalizing instead leaves every
    // node slack (all above half a node, so the fast erase path takes them), and
    // the tree settles.
    const bool drainsNode = nodes[M]->Size == 1 && totalSize / static_cast<int>(NumNodes) > maxNodeSize / 2;
    if (COW_TREE_LIKELY(
        totalSize >= static_cast<int>(NumNodes) * (underfillBar + 1) ||
        totalSize > static_cast<int>((NumNodes - 1) * maxNodeSize) ||
        drainsNode))
    {
        NodeEraseRedistribute<ClearKey, NumNodes, M, TAnyNode>(nodes, separators, pos, totalSize);
        return false;
    } else {
        NodeEraseShrink<ClearKey, NumNodes, M, TAnyNode>(nodes, separators, pos, totalSize);
        // The dropped node's slots are already moved out; reset the stale size so
        // that the release below does not cascade into transferred children.
        auto* doomed = static_cast<TAnyNode*>(nodes[NumNodes - 1]);
        doomed->Size = 0;
        UnrefNode<typename TAnyNode::TTreeConfig>(doomed);
        return true;
    }
}

template <bool ClearKey, size_t NumNodes, size_t EraseIdx, class TAnyNode, class TKey>
bool NodeEraseComplex(TNode** nodes, TValueHolder<TKey>* separators, int pos)
{
    static_assert(EraseIdx < NumNodes);
    constexpr auto leftIdx = std::make_index_sequence<EraseIdx>();
    constexpr auto rightIdx = IndexSequenceAdd<EraseIdx + 1>(std::make_index_sequence<NumNodes - EraseIdx - 1>());
    constexpr auto allIdx = std::make_index_sequence<NumNodes>();
    return NodeEraseComplex<ClearKey, NumNodes, EraseIdx, TAnyNode>(nodes, separators, pos, leftIdx, rightIdx, allIdx);
}

////////////////////////////////////////////////////////////////////////////////

template <class TConfig>
void RefNode(TNode* node)
{
    static_cast<TCountedNode<typename TConfig::TRefCounter>*>(node)->RefCounter.Ref();
}

template <class TConfig>
void UnrefNode(TNode* node)
{
    if (static_cast<TCountedNode<typename TConfig::TRefCounter>*>(node)->RefCounter.Unref()) {
        DestroyNode<TConfig>(node);
    }
}

template <class TConfig>
void DestroyNode(TNode* node)
{
    if (node->IsLeaf) {
        auto* leaf = static_cast<TLeaf<TConfig>*>(node);
        for (int i = 0; i < leaf->Size; ++i) {
            NodeDataClear(leaf, i);
        }
        DeleteCowTreeNode<TConfig>(leaf);
    } else {
        auto* inner = static_cast<TInner<TConfig>*>(node);
        for (int i = 0; i < inner->Size; ++i) {
            UnrefNode<TConfig>(inner->Children[i]);
        }
        for (int i = 1; i < inner->Size; ++i) {
            inner->Keys[i].Clear();
        }
        DeleteCowTreeNode<TConfig>(inner);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCowTreeInternals
