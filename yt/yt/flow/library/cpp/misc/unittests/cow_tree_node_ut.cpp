#include <yt/yt/flow/library/cpp/misc/cow_tree_detail/cow_tree_node.h>

#include <yt/yt/core/test_framework/framework.h>

using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////

template <int BlockSize>
struct TTestNodeConfig
{
    using TKey = ui64;
    using TValue = void;
    using TLess = std::less<ui64>;
    using TRefCounter = NCowTreeInternals::TPlainRefCounter;
    using TMutex = NCowTreeInternals::TNullMutex;

    static constexpr int MaxNodeSize = BlockSize;
    static constexpr ui64 MaxSize = ui64(1) << 32;

    template <class TNode, class... TArgs>
    static TNode* CreateNode(TArgs&&... args)
    {
        return new TNode(std::forward<TArgs>(args)...);
    }
};

template <int BlockSize>
using TTestLeaf = NCowTreeInternals::TLeaf<TTestNodeConfig<BlockSize>>;

template <int BlockSize>
using TTestInner = NCowTreeInternals::TInner<TTestNodeConfig<BlockSize>>;

using THolder64 = NCowTreeInternals::TValueHolder<ui64>;

////////////////////////////////////////////////////////////////////////////////

// Group-aware item accessors. An item of a leaf is a key; an item of an inner node is a
// (leading key, child) pair whose key part lives in |headSeparator| (the parent separator slot)
// when the item is a node head, and is absent altogether for the head of the group.

template <int BlockSize>
constexpr int MinPos(const TTestLeaf<BlockSize>&)
{
    return 0;
}

template <int BlockSize>
constexpr int MinPos(const TTestInner<BlockSize>&)
{
    return 1;
}

template <int BlockSize>
void NodeSetItem(TTestLeaf<BlockSize>& node, THolder64* /*headSeparator*/, int index, ui64 value)
{
    node.Keys[index].Set(value);
}

template <int BlockSize>
void NodeSetItem(TTestInner<BlockSize>& node, THolder64* headSeparator, int index, ui64 value)
{
    node.Children[index] = reinterpret_cast<NCowTreeInternals::TNode*>(value);
    if (index == 0) {
        if (headSeparator) {
            headSeparator->Set(value);
        }
    } else {
        node.Keys[index].Set(value);
    }
}

template <int BlockSize>
void NodeCheckItem(const TTestLeaf<BlockSize>& node, const THolder64* /*headSeparator*/, int index, ui64 value)
{
    EXPECT_EQ(node.Keys[index].Get(), value);
}

template <int BlockSize>
void NodeCheckItem(const TTestInner<BlockSize>& node, const THolder64* headSeparator, int index, ui64 value)
{
    EXPECT_EQ(reinterpret_cast<ui64>(node.Children[index]), value);
    if (index == 0) {
        if (headSeparator) {
            EXPECT_EQ(headSeparator->Get(), value);
        }
    } else {
        EXPECT_EQ(node.Keys[index].Get(), value);
    }
}

template <int BlockSize>
void NodeClearItems(TTestLeaf<BlockSize>& node)
{
    for (int i = 0; i < node.Size; i++) {
        node.Keys[i].Clear();
    }
}

template <int BlockSize>
void NodeClearItems(TTestInner<BlockSize>& node)
{
    for (int i = 1; i < node.Size; i++) {
        node.Keys[i].Clear();
    }
}

//! Leaf-group separators are not independent stream items: they are copies of the left
//! node's max. Inner-group separators are filled by NodeSetItem via |headSeparator|.
template <class TTestNode, size_t N>
void SetupLeafSeparators(std::array<TTestNode, N>& nodes, THolder64* separators, int numNodes)
{
    if constexpr (!NCowTreeInternals::NodeHasChildren<TTestNode>) {
        for (int j = 1; j < numNodes; j++) {
            separators[j - 1].Set(nodes[j - 1].Keys[nodes[j - 1].Size - 1].Get());
        }
    }
}

template <class TTestNode, size_t N>
void CheckLeafSeparators(std::array<TTestNode, N>& nodes, const THolder64* separators, int numNodes)
{
    if constexpr (!NCowTreeInternals::NodeHasChildren<TTestNode>) {
        for (int j = 1; j < numNodes; j++) {
            EXPECT_EQ(separators[j - 1].Get(), nodes[j - 1].Keys[nodes[j - 1].Size - 1].Get());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template <int BlockSize, class TTestNode>
void InsertSimpleTest()
{
    for (int size = 1; size < BlockSize - 1; size++) {
        for (int pos = MinPos(TTestNode()); pos <= size; pos++) {
            TTestNode node;
            node.Size = size;
            ui64 gen = 0;
            ui64 value;
            for (int i = 0; i < size; ++i) {
                if (i == pos) {
                    value = gen++;
                }
                NodeSetItem(node, nullptr, i, gen++);
            }
            if (pos == size) {
                value = gen++;
            }
            NCowTreeInternals::NodeInsertSimple(&node, pos, std::forward_as_tuple(value, reinterpret_cast<NCowTreeInternals::TNode*>(value)));
            gen = 0;
            EXPECT_EQ(node.Size, size + 1);
            for (int i = 0; i < node.Size; i++) {
                NodeCheckItem(node, nullptr, i, gen++);
            }
            NodeClearItems(node);
        }
    }
}

template <int BlockSize>
void InsertSimpleTest()
{
    InsertSimpleTest<BlockSize, TTestLeaf<BlockSize>>();
    InsertSimpleTest<BlockSize, TTestInner<BlockSize>>();
}

TEST(TCowTreeNode, InsertSimple)
{
    constexpr int BlockSize = 8;
    InsertSimpleTest<BlockSize>();
}

////////////////////////////////////////////////////////////////////////////////

template <int BlockSize, int NodeCount, int InsertIdx, class TTestNode, size_t... I>
void InsertRedistributeTest(std::index_sequence<I...>)
{
    std::array<size_t, NodeCount - 1> sizes{};
    for (auto& size : sizes) {
        size = 1;
    }
    bool done = false;
    while (!done) {
        for (int pos = MinPos(TTestNode()); pos <= BlockSize; pos++) {
            std::array<TTestNode, NodeCount> nodes{};
            std::array<NCowTreeInternals::TNode*, NodeCount> nodePtrs = {&nodes[I]...};
            std::array<THolder64, NodeCount - 1> separators{};
            ui64 value;
            ui64 gen = 0;
            int totalSize = 1;
            for (int i = 0; i < NodeCount; ++i) {
                if (i < InsertIdx) {
                    nodes[i].Size = sizes[i];
                    totalSize += sizes[i];
                } else if (i > InsertIdx) {
                    nodes[i].Size = sizes[i - 1];
                    totalSize += sizes[i - 1];
                } else {
                    nodes[i].Size = BlockSize;
                    totalSize += BlockSize;
                }
                THolder64* headSeparator = i > 0 ? &separators[i - 1] : nullptr;
                if (i != InsertIdx) {
                    for (int j = 0; j < nodes[i].Size; j++) {
                        NodeSetItem(nodes[i], headSeparator, j, gen++);
                    }
                } else {
                    for (intmax_t j = 0; j < nodes[i].Size; j++) {
                        if (j == pos) {
                            value = gen++;
                        }
                        NodeSetItem(nodes[i], headSeparator, j, gen++);
                    }
                    if (pos == BlockSize) {
                        value = gen++;
                    }
                }
            }
            SetupLeafSeparators(nodes, &separators[0], NodeCount);
            if (totalSize > static_cast<int>(NodeCount * BlockSize)) {
                // Does not fit without expansion; not a redistribute case.
                for (int i = 0; i < NodeCount; ++i) {
                    NodeClearItems(nodes[i]);
                }
                for (auto& separator : separators) {
                    separator.Clear();
                }
                continue;
            }
            NCowTreeInternals::NodeInsertRedistribute<NodeCount, InsertIdx, TTestNode>(&nodePtrs[0], &separators[0], pos, std::forward_as_tuple(value, reinterpret_cast<NCowTreeInternals::TNode*>(value)), totalSize);
            int minSize = totalSize / NodeCount;
            gen = 0;
            int checkSize = 0;
            for (int i = 0; i < NodeCount; ++i) {
                EXPECT_GE(nodes[i].Size, minSize);
                EXPECT_LE(nodes[i].Size, minSize + 1);
                const THolder64* headSeparator = i > 0 ? &separators[i - 1] : nullptr;
                for (int j = 0; j < nodes[i].Size; j++) {
                    NodeCheckItem(nodes[i], headSeparator, j, gen++);
                }
                checkSize += nodes[i].Size;
            }
            EXPECT_EQ(checkSize, totalSize);
            CheckLeafSeparators(nodes, &separators[0], NodeCount);
            for (int i = 0; i < NodeCount; ++i) {
                NodeClearItems(nodes[i]);
            }
            for (auto& separator : separators) {
                separator.Clear();
            }
        }
        sizes[0]++;
        size_t i = 0;
        while (sizes[i] == BlockSize) {
            sizes[i] = 1;
            i++;
            if (i == sizes.size()) {
                done = true;
                break;
            }
            sizes[i]++;
        }
    }
}

template <int BlockSize, int NodeCount, int InsertIdx>
void InsertRedistributeTest()
{
    InsertRedistributeTest<BlockSize, NodeCount, InsertIdx, TTestLeaf<BlockSize>>(std::make_index_sequence<NodeCount>());
    InsertRedistributeTest<BlockSize, NodeCount, InsertIdx, TTestInner<BlockSize>>(std::make_index_sequence<NodeCount>());
}

template <int BlockSize, int NodeCount, size_t... InsertIdx>
void InsertRedistributeTest(std::index_sequence<InsertIdx...>)
{
    (InsertRedistributeTest<BlockSize, NodeCount, InsertIdx>(), ...);
}

template <int BlockSize, size_t... NodeCount>
void InsertRedistributeTest(std::index_sequence<NodeCount...>)
{
    (InsertRedistributeTest<BlockSize, NodeCount>(std::make_index_sequence<NodeCount>()), ...);
}

TEST(TCowTreeNode, InsertRedistribute)
{
    constexpr int BlockSize = 8;
    InsertRedistributeTest<BlockSize>(std::index_sequence<2, 3, 4, 5>());
}

////////////////////////////////////////////////////////////////////////////////

template <int BlockSize, int NodeCount, int InsertIdx, class TTestNode, size_t... I>
void InsertExpandTest(std::index_sequence<I...>)
{
    std::array<size_t, NodeCount - 2> sizes{};
    for (auto& size : sizes) {
        size = 1;
    }
    bool done = false;
    while (!done) {
        for (int pos = MinPos(TTestNode()); pos <= BlockSize; pos++) {
            std::array<TTestNode, NodeCount> nodes{};
            std::array<NCowTreeInternals::TNode*, NodeCount - 1> nodePtrs{};
            for (int i = 0; i < NodeCount - 1; i++) {
                nodePtrs[i] = &nodes[i];
            }
            // Separators between the NodeCount - 1 old nodes, plus the separator handed up for
            // the new node; the layout keeps headSeparator(i) == separators[i - 1] uniform.
            std::array<THolder64, NodeCount - 1> separators{};
            THolder64& upSeparator = separators[NodeCount - 2];
            ui64 value;
            ui64 gen = 0;
            int totalSize = 1;
            for (int i = 0; i < NodeCount - 1; ++i) {
                if (i < InsertIdx) {
                    nodes[i].Size = sizes[i];
                    totalSize += sizes[i];
                } else if (i > InsertIdx) {
                    nodes[i].Size = sizes[i - 1];
                    totalSize += sizes[i - 1];
                } else {
                    nodes[i].Size = BlockSize;
                    totalSize += BlockSize;
                }
                THolder64* headSeparator = i > 0 ? &separators[i - 1] : nullptr;
                if (i != InsertIdx) {
                    for (int j = 0; j < nodes[i].Size; j++) {
                        NodeSetItem(nodes[i], headSeparator, j, gen++);
                    }
                } else {
                    for (intmax_t j = 0; j < nodes[i].Size; j++) {
                        if (j == pos) {
                            value = gen++;
                        }
                        NodeSetItem(nodes[i], headSeparator, j, gen++);
                    }
                    if (pos == BlockSize) {
                        value = gen++;
                    }
                }
            }
            SetupLeafSeparators(nodes, &separators[0], NodeCount - 1);
            NCowTreeInternals::NodeInsertExpand<NodeCount - 1, InsertIdx, TTestNode>(&nodePtrs[0], &separators[0], &nodes[NodeCount - 1], upSeparator, pos, std::forward_as_tuple(value, reinterpret_cast<NCowTreeInternals::TNode*>(value)), totalSize);
            int minSize = totalSize / NodeCount;
            gen = 0;
            int checkSize = 0;
            for (int i = 0; i < NodeCount; ++i) {
                EXPECT_GE(nodes[i].Size, minSize);
                EXPECT_LE(nodes[i].Size, minSize + 1);
                const THolder64* headSeparator = i > 0 ? &separators[i - 1] : nullptr;
                for (int j = 0; j < nodes[i].Size; j++) {
                    NodeCheckItem(nodes[i], headSeparator, j, gen++);
                }
                checkSize += nodes[i].Size;
            }
            EXPECT_EQ(checkSize, totalSize);
            if constexpr (!NCowTreeInternals::NodeHasChildren<TTestNode>) {
                CheckLeafSeparators(nodes, &separators[0], NodeCount - 1);
                EXPECT_EQ(upSeparator.Get(), nodes[NodeCount - 2].Keys[nodes[NodeCount - 2].Size - 1].Get());
            }
            for (int i = 0; i < NodeCount; ++i) {
                NodeClearItems(nodes[i]);
            }
            for (auto& separator : separators) {
                separator.Clear();
            }
        }
        if constexpr (sizes.size() == 0) {
            break;
        } else {
            sizes[0]++;
            size_t i = 0;
            while (sizes[i] == BlockSize) {
                sizes[i] = 1;
                i++;
                if (i == sizes.size()) {
                    done = true;
                    break;
                }
                sizes[i]++;
            }
        }
    }
}

template <int BlockSize, int NodeCount, int InsertIdx>
void InsertExpandTest()
{
    InsertExpandTest<BlockSize, NodeCount, InsertIdx, TTestLeaf<BlockSize>>(std::make_index_sequence<NodeCount>());
    InsertExpandTest<BlockSize, NodeCount, InsertIdx, TTestInner<BlockSize>>(std::make_index_sequence<NodeCount>());
}

template <int BlockSize, int NodeCount, size_t... InsertIdx>
void InsertExpandTest(std::index_sequence<InsertIdx...>)
{
    (InsertExpandTest<BlockSize, NodeCount, InsertIdx>(), ...);
}

template <int BlockSize, size_t... NodeCount>
void InsertExpandTest(std::index_sequence<NodeCount...>)
{
    (InsertExpandTest<BlockSize, NodeCount>(std::make_index_sequence<NodeCount - 1>()), ...);
}

TEST(TCowTreeNode, InsertExpand)
{
    constexpr int BlockSize = 8;
    InsertExpandTest<BlockSize>(std::index_sequence<2, 3, 4, 5, 6>());
}

////////////////////////////////////////////////////////////////////////////////

template <int BlockSize, class TTestNode>
void EraseSimpleTest()
{
    for (int size = 2; size < BlockSize; size++) {
        for (int pos = MinPos(TTestNode()); pos < size; pos++) {
            TTestNode node;
            node.Size = size;
            ui64 gen = 0;
            ui64 value;
            for (int i = 0; i < size; ++i) {
                if (i == pos) {
                    value = gen;
                }
                NodeSetItem(node, nullptr, i, gen++);
            }
            constexpr bool ClearKey = !NCowTreeInternals::NodeHasChildren<TTestNode>;
            if constexpr (!ClearKey) {
                // Inner pair erase happens after the dying separator was already emptied.
                node.Keys[pos].Clear();
            }
            NCowTreeInternals::NodeEraseSimple<ClearKey>(&node, pos);
            gen = 0;
            EXPECT_EQ(node.Size, size - 1);
            for (int i = 0; i < node.Size; i++) {
                if (gen == value) {
                    gen++;
                }
                NodeCheckItem(node, nullptr, i, gen++);
            }
            NodeClearItems(node);
        }
    }
}

template <int BlockSize>
void EraseSimpleTest()
{
    EraseSimpleTest<BlockSize, TTestLeaf<BlockSize>>();
    EraseSimpleTest<BlockSize, TTestInner<BlockSize>>();
}

TEST(TCowTreeNode, EraseSimple)
{
    constexpr int BlockSize = 8;
    EraseSimpleTest<BlockSize>();
}

////////////////////////////////////////////////////////////////////////////////

template <int BlockSize, int NodeCount, int EraseIdx, class TTestNode, size_t... I>
void EraseRedistributeTest(std::index_sequence<I...>)
{
    std::array<int, NodeCount> sizes{};
    for (auto& size : sizes) {
        size = 1;
    }
    bool done = false;
    while (!done) {
        if ((sizes[I] + ...) > NodeCount && sizes[EraseIdx] > 0) {
            for (int pos = MinPos(TTestNode()); pos < sizes[EraseIdx]; pos++) {
                std::array<TTestNode, NodeCount> nodes{};
                std::array<NCowTreeInternals::TNode*, NodeCount> nodePtrs = {&nodes[I]...};
                std::array<THolder64, NodeCount - 1> separators{};
                ui64 value;
                ui64 gen = 0;
                int totalSize = (sizes[I] + ...) - 1;
                for (int i = 0; i < NodeCount; ++i) {
                    nodes[i].Size = sizes[i];
                    THolder64* headSeparator = i > 0 ? &separators[i - 1] : nullptr;
                    for (int j = 0; j < nodes[i].Size; j++) {
                        if (i == EraseIdx && j == pos) {
                            value = gen;
                        }
                        NodeSetItem(nodes[i], headSeparator, j, gen++);
                    }
                }
                SetupLeafSeparators(nodes, &separators[0], NodeCount);
                constexpr bool ClearKey = !NCowTreeInternals::NodeHasChildren<TTestNode>;
                if constexpr (!ClearKey) {
                    nodes[EraseIdx].Keys[pos].Clear();
                }
                NCowTreeInternals::NodeEraseRedistribute<ClearKey, NodeCount, EraseIdx, TTestNode>(&nodePtrs[0], &separators[0], pos, totalSize);
                int minSize = (gen - 1) / NodeCount;
                gen = 0;
                int checkSize = 0;
                for (int i = 0; i < NodeCount; ++i) {
                    EXPECT_GE(nodes[i].Size, minSize);
                    EXPECT_LE(nodes[i].Size, minSize + 1);
                    const THolder64* headSeparator = i > 0 ? &separators[i - 1] : nullptr;
                    for (int j = 0; j < nodes[i].Size; j++) {
                        if (gen == value) {
                            gen++;
                        }
                        NodeCheckItem(nodes[i], headSeparator, j, gen++);
                    }
                    checkSize += nodes[i].Size;
                }
                EXPECT_EQ(checkSize, totalSize);
                CheckLeafSeparators(nodes, &separators[0], NodeCount);
                for (int i = 0; i < NodeCount; ++i) {
                    NodeClearItems(nodes[i]);
                }
                for (auto& separator : separators) {
                    separator.Clear();
                }
            }
        }
        sizes[0]++;
        size_t i = 0;
        while (sizes[i] == BlockSize) {
            sizes[i] = 1;
            i++;
            if (i == sizes.size()) {
                done = true;
                break;
            }
            sizes[i]++;
        }
    }
}

template <int BlockSize, int NodeCount, int EraseIdx>
void EraseRedistributeTest()
{
    EraseRedistributeTest<BlockSize, NodeCount, EraseIdx, TTestLeaf<BlockSize>>(std::make_index_sequence<NodeCount>());
    EraseRedistributeTest<BlockSize, NodeCount, EraseIdx, TTestInner<BlockSize>>(std::make_index_sequence<NodeCount>());
}

template <int BlockSize, int NodeCount, size_t... EraseIdx>
void EraseRedistributeTest(std::index_sequence<EraseIdx...>)
{
    (EraseRedistributeTest<BlockSize, NodeCount, EraseIdx>(), ...);
}

template <int BlockSize, size_t... NodeCount>
void EraseRedistributeTest(std::index_sequence<NodeCount...>)
{
    (EraseRedistributeTest<BlockSize, NodeCount>(std::make_index_sequence<NodeCount>()), ...);
}

TEST(TCowTreeNode, EraseRedistribute)
{
    constexpr int BlockSize = 8;
    EraseRedistributeTest<BlockSize>(std::index_sequence<2, 3, 4>());
}

////////////////////////////////////////////////////////////////////////////////

template <int BlockSize, int NodeCount, int EraseIdx, class TTestNode, size_t... I>
void EraseShrinkTest(std::index_sequence<I...>)
{
    std::array<int, NodeCount> sizes = {};
    for (auto& size : sizes) {
        size = 1;
    }
    bool done = false;
    while (!done) {
        if ((sizes[I] + ...) > NodeCount && (sizes[I] + ...) <= ((NodeCount - 1) * BlockSize) && sizes[EraseIdx] > 0) {
            for (int pos = MinPos(TTestNode()); pos < sizes[EraseIdx]; pos++) {
                std::array<TTestNode, NodeCount> nodes{};
                std::array<NCowTreeInternals::TNode*, NodeCount> nodePtrs = {&nodes[I]...};
                std::array<THolder64, NodeCount - 1> separators{};
                ui64 value;
                ui64 gen = 0;
                int totalSize = (sizes[I] + ...) - 1;
                for (int i = 0; i < NodeCount; ++i) {
                    nodes[i].Size = sizes[i];
                    THolder64* headSeparator = i > 0 ? &separators[i - 1] : nullptr;
                    for (int j = 0; j < nodes[i].Size; j++) {
                        if (i == EraseIdx && j == pos) {
                            value = gen;
                        }
                        NodeSetItem(nodes[i], headSeparator, j, gen++);
                    }
                }
                SetupLeafSeparators(nodes, &separators[0], NodeCount);
                constexpr bool ClearKey = !NCowTreeInternals::NodeHasChildren<TTestNode>;
                if constexpr (!ClearKey) {
                    nodes[EraseIdx].Keys[pos].Clear();
                }
                NCowTreeInternals::NodeEraseShrink<ClearKey, NodeCount, EraseIdx, TTestNode>(&nodePtrs[0], &separators[0], pos, totalSize);
                int minSize = (gen - 1) / (NodeCount - 1);
                gen = 0;
                int checkSize = 0;
                for (int i = 0; i < NodeCount - 1; ++i) {
                    EXPECT_GE(nodes[i].Size, minSize);
                    EXPECT_LE(nodes[i].Size, minSize + 1);
                    const THolder64* headSeparator = i > 0 ? &separators[i - 1] : nullptr;
                    for (int j = 0; j < nodes[i].Size; j++) {
                        if (gen == value) {
                            gen++;
                        }
                        NodeCheckItem(nodes[i], headSeparator, j, gen++);
                    }
                    checkSize += nodes[i].Size;
                }
                EXPECT_EQ(checkSize, totalSize);
                CheckLeafSeparators(nodes, &separators[0], NodeCount - 1);
                // The dying separator (the last separator slot) is left empty by the shrink.
                for (int i = 0; i + 1 < NodeCount - 1; ++i) {
                    separators[i].Clear();
                }
                for (int i = 0; i < NodeCount - 1; ++i) {
                    NodeClearItems(nodes[i]);
                }
            }
        }
        sizes[0]++;
        size_t i = 0;
        while (sizes[i] == BlockSize) {
            sizes[i] = 1;
            i++;
            if (i == sizes.size()) {
                done = true;
                break;
            }
            sizes[i]++;
        }
    }
}

template <int BlockSize, int NodeCount, int EraseIdx>
void EraseShrinkTest()
{
    EraseShrinkTest<BlockSize, NodeCount, EraseIdx, TTestLeaf<BlockSize>>(std::make_index_sequence<NodeCount>());
    EraseShrinkTest<BlockSize, NodeCount, EraseIdx, TTestInner<BlockSize>>(std::make_index_sequence<NodeCount>());
}

template <int BlockSize, int NodeCount, size_t... EraseIdx>
void EraseShrinkTest(std::index_sequence<EraseIdx...>)
{
    (EraseShrinkTest<BlockSize, NodeCount, EraseIdx>(), ...);
}

template <int BlockSize, size_t... NodeCount>
void EraseShrinkTest(std::index_sequence<NodeCount...>)
{
    (EraseShrinkTest<BlockSize, NodeCount>(std::make_index_sequence<NodeCount>()), ...);
}

TEST(TCowTreeNode, EraseShrink)
{
    constexpr int BlockSize = 8;
    EraseShrinkTest<BlockSize>(std::index_sequence<2, 3, 4>());
}

////////////////////////////////////////////////////////////////////////////////
