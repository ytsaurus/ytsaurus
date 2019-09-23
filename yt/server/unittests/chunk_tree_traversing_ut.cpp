#include "chunk_helpers.h"

#include <yt/core/test_framework/framework.h>

#include "chunk_helpers.h"

#include <yt/server/master/chunk_server/chunk.h>
#include <yt/server/master/chunk_server/chunk_list.h>
#include <yt/server/master/chunk_server/chunk_view.h>
#include <yt/server/master/chunk_server/chunk_tree_traverser.h>
#include <yt/server/master/chunk_server/helpers.h>

#include <yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/client/chunk_client/read_limit.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/client/table_client/helpers.h>

#include <yt/core/actions/invoker_util.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/finally.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/yson/string.h>

#include <random>

namespace NYT::NChunkServer {

namespace {

using namespace NTesting;

using namespace NChunkClient::NProto;
using namespace NCypressClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

static constexpr double ForgetRowIndexProbability = 0.1;
static constexpr double ForgetReadLimitProbability = 0.5;

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TReadLimit& lhs, const TReadLimit& rhs)
{
    return lhs.AsProto().DebugString() == rhs.AsProto().DebugString();
}

////////////////////////////////////////////////////////////////////////////////

struct TChunkInfo
{
public:
    TChunkInfo(
        TChunk* chunk,
        i64 rowIndex,
        TReadLimit lowerLimit,
        TReadLimit upperLimit)
            : Chunk(chunk)
            , RowIndex(rowIndex)
            , TabletIndex(std::nullopt)
            , LowerLimit(lowerLimit)
            , UpperLimit(upperLimit)
    { }

    TChunkInfo(
        TChunk* chunk,
        i64 rowIndex,
        std::optional<i32> tabletIndex,
        TReadLimit lowerLimit,
        TReadLimit upperLimit)
            : Chunk(chunk)
            , RowIndex(rowIndex)
            , TabletIndex(tabletIndex)
            , LowerLimit(lowerLimit)
            , UpperLimit(upperLimit)
    { }

    TChunk* Chunk;
    i64 RowIndex;
    std::optional<i32> TabletIndex;
    TReadLimit LowerLimit;
    TReadLimit UpperLimit;
};

bool operator < (const TChunkInfo& lhs, const TChunkInfo& rhs)
{
    if (lhs.Chunk->GetId() != rhs.Chunk->GetId()) {
        return lhs.Chunk->GetId() < rhs.Chunk->GetId();
    }
    if (lhs.TabletIndex.has_value() || rhs.TabletIndex.has_value()) {
        YT_VERIFY(lhs.TabletIndex.has_value() && rhs.TabletIndex.has_value());
        return std::tie(*lhs.TabletIndex, lhs.RowIndex) < std::tie(*rhs.TabletIndex, rhs.RowIndex);
    }
    if (lhs.RowIndex != rhs.RowIndex) {
        return lhs.RowIndex < rhs.RowIndex;
    }
    // We compare uncomparable.
    if (ToString(lhs.LowerLimit) != ToString(rhs.LowerLimit)) {
        return ToString(lhs.LowerLimit) < ToString(rhs.LowerLimit);
    }
    if (ToString(lhs.UpperLimit) != ToString(rhs.UpperLimit)) {
        return ToString(lhs.UpperLimit) < ToString(rhs.UpperLimit);
    }
    return false;
}

bool operator == (const TChunkInfo& lhs, const TChunkInfo& rhs)
{
    return lhs.Chunk->GetId() == rhs.Chunk->GetId()
        && lhs.RowIndex == rhs.RowIndex
        && lhs.TabletIndex == rhs.TabletIndex
        && lhs.LowerLimit == rhs.LowerLimit
        && lhs.UpperLimit == rhs.UpperLimit;
}

std::ostream& operator << (std::ostream& os, const TChunkInfo& chunkInfo)
{
    auto tabletIndexString = chunkInfo.TabletIndex.has_value() ? ", TabletIndex=" + ToString(chunkInfo.TabletIndex) : "";
    os << "ChunkInfo(Id=" << ToString(chunkInfo.Chunk->GetId())
       << ", RowIndex=" << chunkInfo.RowIndex
       << tabletIndexString
       << ", LowerLimit=" << ToString(chunkInfo.LowerLimit)
       << ", UpperLimit=" << ToString(chunkInfo.UpperLimit)
       << ")";
    return os;
}

////////////////////////////////////////////////////////////////////////////////

class TTestChunkVisitor
    : public IChunkVisitor
{
public:
    virtual bool OnChunk(
        TChunk* chunk,
        i64 rowIndex,
        std::optional<i32> tabletIndex,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit,
        TTransactionId /*timestampTransactionId*/) override
    {
        ChunkInfos.insert(TChunkInfo(chunk, rowIndex, tabletIndex, lowerLimit, upperLimit));
        return true;
    }

    virtual bool OnChunkView(TChunkView*) override
    {
        return false;
    }

    virtual void OnFinish(const TError& error) override
    {
        ASSERT_TRUE(error.IsOK());
    }

    const std::set<TChunkInfo>& GetChunkInfos() const
    {
        return ChunkInfos;
    }

private:
    std::set<TChunkInfo> ChunkInfos;

};

////////////////////////////////////////////////////////////////////////////////

class TNaiveChunkTreeTraverser
{
public:
    std::set<TChunkInfo> Traverse(
        TChunkList* chunkList,
        bool calculateRowIndex,
        bool calculateTabletIndex,
        const TReadLimit& lowerLimit = TReadLimit{},
        const TReadLimit& upperLimit = TReadLimit{},
        const std::vector<int>& tabletStartRowCount = std::vector<int>{})
    {
        CalculateRowIndex_ = calculateRowIndex;
        CalculateTabletIndex_ = calculateTabletIndex;
        LowerLimit_ = lowerLimit;
        UpperLimit_ = upperLimit;
        TabletStartRowCount_ = tabletStartRowCount;

        ChunkCount_ = 0;
        RowCount_ = TabletStartRowCount_.empty() ? 0 : TabletStartRowCount_[0];
        Result_.clear();

        if (chunkList->GetKind() == EChunkListKind::OrderedDynamicRoot) {
            TabletIndex_ = 0;
        }

        TraverseChunkList(chunkList, LowerKeyLimit(), UpperKeyLimit());
        return Result_;
    }

private:
    bool CalculateRowIndex_;
    bool CalculateTabletIndex_;
    TReadLimit LowerLimit_;
    TReadLimit UpperLimit_;

    std::set<TChunkInfo> Result_;
    int ChunkCount_ = 0;
    int RowCount_ = 0;
    std::optional<i32> TabletIndex_ = std::nullopt;

    std::vector<int> TabletStartRowCount_;

    i64 LowerChunkIndexLimit() const
    {
        return LowerLimit_.HasChunkIndex() ? LowerLimit_.GetChunkIndex() : 0;
    }

    i64 UpperChunkIndexLimit() const
    {
        return UpperLimit_.HasChunkIndex() ? UpperLimit_.GetChunkIndex() : std::numeric_limits<i64>::max();
    }

    i64 LowerRowIndexLimit() const
    {
        return LowerLimit_.HasRowIndex() ? LowerLimit_.GetRowIndex() : 0;
    }

    i64 UpperRowIndexLimit() const
    {
        return UpperLimit_.HasRowIndex() ? UpperLimit_.GetRowIndex() : std::numeric_limits<i64>::max();
    }

    TOwningKey LowerKeyLimit() const
    {
        return LowerLimit_.HasKey() ? LowerLimit_.GetKey() : MinKey();
    }

    TOwningKey UpperKeyLimit() const
    {
        return UpperLimit_.HasKey() ? UpperLimit_.GetKey() : MaxKey();
    }

    i32 LowerTabletIndexLimit() const
    {
        return LowerLimit_.HasTabletIndex() ? LowerLimit_.GetTabletIndex() : 0;
    }

    i32 UpperTabletIndexLimit() const
    {
        return UpperLimit_.HasTabletIndex() ? UpperLimit_.GetTabletIndex() : std::numeric_limits<i32>::max();
    }

    TOwningKey GetNextPivotKey(TChunkList* tabletChunkList)
    {
        auto* parent = GetUniqueParent(tabletChunkList);
        int index = parent->ChildToIndex()[tabletChunkList];
        return index + 1 == parent->Children().size()
            ? MaxKey()
            : parent->Children()[index + 1]->AsChunkList()->GetPivotKey();;
    }

    bool HasBoundaryKeys(TChunk* chunk)
    {
        if (!HasProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(chunk->ChunkMeta().extensions())) {
            return false;
        }
        // It is assumed that min and max keys either both exist or both do not.
        return static_cast<bool>(GetMinKeyOrThrow(chunk));
    }

    bool TraverseChunkList(
        TChunkList* chunkList,
        TOwningKey lowerKeyLimit = MinKey(),
        TOwningKey upperKeyLimit = MaxKey())
    {
        auto updateTabletIndex = [&] () {
            if (chunkList->GetKind() == EChunkListKind::OrderedDynamicRoot) {
                ++*TabletIndex_;
                // Is actually used only for trimmed chunks in ordered tables.
                RowCount_ = *TabletIndex_ < TabletStartRowCount_.size() ? TabletStartRowCount_[*TabletIndex_] : 0;
            }
        };

        if (chunkList->GetKind() == EChunkListKind::SortedDynamicTablet) {
            lowerKeyLimit = ChooseMaxKey(lowerKeyLimit, chunkList->GetPivotKey());
            upperKeyLimit = ChooseMinKey(upperKeyLimit, GetNextPivotKey(chunkList));
            if (lowerKeyLimit >= upperKeyLimit) {
                return true;
            }
        }
        if (chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet) {
            if (*TabletIndex_ < LowerTabletIndexLimit()) {
                return true;
            }
            if (*TabletIndex_ > UpperTabletIndexLimit()) {
                return false;
            }
        }

        for (auto* child : chunkList->Children()) {
            if (child->GetType() == EObjectType::ChunkList) {
                if (!TraverseChunkList(child->AsChunkList(), lowerKeyLimit, upperKeyLimit)) {
                    return false;
                }
            } else if (child->GetType() == EObjectType::ChunkView) {
                const auto& readRange = child->AsChunkView()->ReadRange();
                if (readRange.LowerLimit().HasKey()) {
                    lowerKeyLimit = ChooseMaxKey(lowerKeyLimit, readRange.LowerLimit().GetKey());
                }
                if (readRange.UpperLimit().HasKey()) {
                    upperKeyLimit = ChooseMinKey(upperKeyLimit, readRange.UpperLimit().GetKey());
                }
                if (!OnChunk(child->AsChunk(), lowerKeyLimit, upperKeyLimit)) {
                    return false;
                }
            } else {
                if (!OnChunk(child->AsChunk(), lowerKeyLimit, upperKeyLimit)) {
                    return false;
                }
            }
            updateTabletIndex();
        }
        return true;
    }

    bool OnChunk(
        TChunk* chunk,
        const TOwningKey& lowerKeyLimit = MinKey(),
        const TOwningKey& upperKeyLimit = MaxKey())
    {
        int newChunkCount = ChunkCount_ + 1;
        int newRowCount = RowCount_ + GetChunkTreeStatistics(chunk).LogicalRowCount;

        auto finally = Finally([&] {
            ChunkCount_ = newChunkCount;
            RowCount_ = newRowCount;
        });

        TReadLimit correctLowerLimit;
        TReadLimit correctUpperLimit;

        if (ChunkCount_ >= UpperChunkIndexLimit()) {
            return false;
        } else if (newChunkCount <= LowerChunkIndexLimit()) {
            return true;
        }

        if (TabletIndex_.has_value()) {
            // For ordered dynamic tables.
            YT_VERIFY(*TabletIndex_ >= LowerTabletIndexLimit());
            YT_VERIFY(*TabletIndex_ <= UpperTabletIndexLimit());

            if (*TabletIndex_ == LowerTabletIndexLimit()) {
                if (newRowCount <= LowerRowIndexLimit()) {
                    return true;
                }
                if (RowCount_ < LowerRowIndexLimit()) {
                    correctLowerLimit.SetRowIndex(LowerRowIndexLimit() - RowCount_);
                }
            }
            if (*TabletIndex_ == UpperTabletIndexLimit()) {
                if (RowCount_ >= UpperRowIndexLimit()) {
                    return false;
                }
                if (newRowCount > UpperRowIndexLimit()) {
                    correctUpperLimit.SetRowIndex(UpperRowIndexLimit() - RowCount_);
                }
            }
        } else {
            if (RowCount_ >= UpperRowIndexLimit()) {
                return false;
            } else if (newRowCount <= LowerRowIndexLimit()) {
                return true;
            } else {
                if (RowCount_ < LowerRowIndexLimit()) {
                    correctLowerLimit.SetRowIndex(LowerRowIndexLimit() - RowCount_);
                }
                if (UpperRowIndexLimit() < newRowCount) {
                    correctUpperLimit.SetRowIndex(UpperRowIndexLimit() - RowCount_);
                }
            }
        }

        if (HasBoundaryKeys(chunk)) {
            if (GetMinKeyOrThrow(chunk) >= upperKeyLimit) {
                // Intentionally true. We do not want to stop traversing when one of the chunks
                // inside the sorted tablet falls out of the range.
                return true;
            } else if (GetUpperBoundKeyOrThrow(chunk) <= lowerKeyLimit) {
                return true;
            } else {
                if (GetMinKeyOrThrow(chunk) < lowerKeyLimit) {
                    correctLowerLimit.SetKey(lowerKeyLimit);
                }
                if (upperKeyLimit <= GetUpperBoundKeyOrThrow(chunk)) {
                    correctUpperLimit.SetKey(upperKeyLimit);
                }
            }
        }

        Result_.emplace(
            chunk,
            CalculateRowIndex_ ? RowCount_ : 0,
            CalculateTabletIndex_ ? TabletIndex_ : std::nullopt,
            correctLowerLimit,
            correctUpperLimit);
        return true;
    }
};

std::set<TChunkInfo> TraverseNaively(
    TChunkList* chunkList,
    bool calculateRowIndex,
    bool calculateTabletIndex = false,
    const TReadLimit& lowerLimit = TReadLimit{},
    const TReadLimit& upperLimit = TReadLimit{},
    const std::vector<int>& tabletStartRowCount = std::vector<int>{})
{
    TNaiveChunkTreeTraverser naiveTraverser;
    return naiveTraverser.Traverse(chunkList, calculateRowIndex, calculateTabletIndex, lowerLimit, upperLimit, tabletStartRowCount);
}

////////////////////////////////////////////////////////////////////////////////

TChunkTree* GenerateChunkTree(
    TChunkGeneratorBase* chunkGenerator,
    int numLayers,
    std::mt19937& randomGenerator,
    const std::function<int()>& keyYielder,
    std::vector<EChunkListKind> chunkListKindByLayer)
{
    if (numLayers == 0) {
        int rowCount = randomGenerator() % 10 + 1;
        int dataSize = randomGenerator() % 100 + 1;
        int dataWeight = randomGenerator() % 100 + 1;
        TOwningKey minKey;
        TOwningKey maxKey;
        if (keyYielder) {
            minKey = BuildKey(ToString(keyYielder()));
            maxKey = BuildKey(ToString(keyYielder()));
        }
        return chunkGenerator->CreateChunk(rowCount, dataSize, dataSize, dataWeight, minKey, maxKey);
    } else {
        auto* chunkList = chunkGenerator->CreateChunkList(chunkListKindByLayer[0]);
        chunkListKindByLayer.erase(chunkListKindByLayer.begin());

        std::vector<TChunkTree*> children;
        for (int i = 0; i < 5; ++i) {
            auto* child = GenerateChunkTree(
                chunkGenerator, numLayers - 1, randomGenerator, keyYielder, chunkListKindByLayer);

            // Special case: set pivot keys of sorted dynamic table tablets.
            if (chunkList->GetKind() == EChunkListKind::SortedDynamicRoot) {
                YT_VERIFY(chunkListKindByLayer[0] == EChunkListKind::SortedDynamicTablet);
                if (children.empty()) {
                    child->AsChunkList()->SetPivotKey(BuildKey(""));
                } else {
                    child->AsChunkList()->SetPivotKey(GetMinKeyOrThrow(child));
                }
            }

            // Special case: wrap subtablet's children into chunk views.
            if (chunkList->GetKind() == EChunkListKind::SortedDynamicSubtablet && numLayers == 1) {
                child = chunkGenerator->CreateChunkView(child->AsChunk(), MinKey(), MaxKey());
            }

            children.push_back(child);
        }

        AttachToChunkList(chunkList, children);
        return chunkList;
    }
}

std::function<int()> CreateBasicKeyYielder()
{
    int dummy = 0;
    return [=] () mutable {
        dummy += 10;
        return dummy;
    };
}

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeTraversingTest
    : public TChunkGeneratorBase
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TChunkTreeTraversingTest, Simple)
{
    //     listA           //
    //    /     \          //
    // chunk1   listB      //
    //         /     \     //
    //     chunk2   chunk3 //

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    auto* chunk3 = CreateChunk(3, 3, 3, 3);

    auto* listA = CreateChunkList();
    auto* listB = CreateChunkList();

    {
        std::vector<TChunkTree*> items;
        items.push_back(chunk2);
        items.push_back(chunk3);
        AttachToChunkList(listB, items);
    }

    {
        std::vector<TChunkTree*> items;
        items.push_back(chunk1);
        items.push_back(listB);
        AttachToChunkList(listA, items);
    }

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, listA);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2,
            1,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk3,
            3,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
        EXPECT_EQ(TraverseNaively(listA, true), visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetRowIndex(2);

        TReadLimit upperLimit;
        upperLimit.SetRowIndex(5);

        TraverseChunkTree(callbacks, visitor, listA, lowerLimit, upperLimit);

        TReadLimit correctLowerLimit;
        correctLowerLimit.SetRowIndex(1);

        TReadLimit correctUpperLimit;
        correctUpperLimit.SetRowIndex(2);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk2,
            1,
            correctLowerLimit,
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk3,
            3,
            TReadLimit(),
            correctUpperLimit));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
        EXPECT_EQ(TraverseNaively(listA, true, false, lowerLimit, upperLimit), visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingTest, WithEmptyChunkLists)
{
    //               list              //
    //    /     |     |     |     \    //
    // empty chunk1 empty chunk2 empty //

    auto chunk1 = CreateChunk(1, 1, 1, 1);
    auto chunk2 = CreateChunk(2, 2, 2, 2);

    auto list = CreateChunkList();
    auto empty1 = CreateChunkList();
    auto empty2 = CreateChunkList();
    auto empty3 = CreateChunkList();

    {
        std::vector<TChunkTree*> items;
        items.push_back(empty1);
        items.push_back(chunk1);
        items.push_back(empty2);
        items.push_back(chunk2);
        items.push_back(empty3);
        AttachToChunkList(list, items);
    }

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, list);
        EXPECT_EQ(TraverseNaively(list, true, {}, {}), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2,
            1,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetRowIndex(1);

        TReadLimit upperLimit;
        upperLimit.SetRowIndex(2);

        TraverseChunkTree(callbacks, visitor, list, lowerLimit, upperLimit);
        EXPECT_EQ(TraverseNaively(list, true, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        TReadLimit correctLowerLimit;

        TReadLimit correctUpperLimit;
        correctUpperLimit.SetRowIndex(1);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk2,
            1,
            correctLowerLimit,
            correctUpperLimit));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingTest, TestIvan)
{
    //     root               //
    //    /    \              //
    // tablet1  tablet2       //
    //  /       /     \       //
    // chunk1  chunk2  chunk3 //

    auto chunk1 = CreateChunk(1, 1, 1, 1, BuildKey("1"), BuildKey("1"));
    auto chunk2 = CreateChunk(2, 2, 2, 2, BuildKey("2"), BuildKey("2"));
    auto chunk3 = CreateChunk(3, 3, 3, 3, BuildKey("3"), BuildKey("3"));

    auto root = CreateChunkList(EChunkListKind::SortedDynamicRoot);
    auto tablet1 = CreateChunkList(EChunkListKind::SortedDynamicTablet);
    auto tablet2 = CreateChunkList(EChunkListKind::SortedDynamicTablet);
    tablet1->SetPivotKey(BuildKey(""));
    tablet2->SetPivotKey(BuildKey("2"));

    AttachToChunkList(tablet1, std::vector<TChunkTree*>{chunk1, chunk2, chunk3});
    // AttachToChunkList(tablet2, std::vector<TChunkTree*>{chunk2, chunk3});
    AttachToChunkList(root, std::vector<TChunkTree*>{tablet1, tablet2});

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, root);
        EXPECT_EQ(TraverseNaively(root, false, {}, {}), visitor->GetChunkInfos());
    }
    return;

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(2);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);
        EXPECT_EQ(
            TraverseNaively(root, false, false, lowerLimit, upperLimit),
            visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2,
            0,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetKey(BuildKey("1"));

        TReadLimit upperLimit;
        upperLimit.SetKey(BuildKey("5"));

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);
        EXPECT_EQ(
            TraverseNaively(root, false, false, lowerLimit, upperLimit),
            visitor->GetChunkInfos());
    }
}
TEST_F(TChunkTreeTraversingTest, SortedDynamic)
{
    //     root               //
    //    /    \              //
    // tablet1  tablet2       //
    //  /       /     \       //
    // chunk1  chunk2  chunk3 //

    auto chunk1 = CreateChunk(1, 1, 1, 1, BuildKey("1"), BuildKey("1"));
    auto chunk2 = CreateChunk(2, 2, 2, 2, BuildKey("3"), BuildKey("5"));
    auto chunk3 = CreateChunk(3, 3, 3, 3, BuildKey("2"), BuildKey("4"));

    auto root = CreateChunkList(EChunkListKind::SortedDynamicRoot);
    auto tablet1 = CreateChunkList(EChunkListKind::SortedDynamicTablet);
    auto tablet2 = CreateChunkList(EChunkListKind::SortedDynamicTablet);
    tablet1->SetPivotKey(BuildKey(""));
    tablet2->SetPivotKey(BuildKey("2"));

    AttachToChunkList(tablet1, std::vector<TChunkTree*>{chunk1});
    AttachToChunkList(tablet2, std::vector<TChunkTree*>{chunk2, chunk3});
    AttachToChunkList(root, std::vector<TChunkTree*>{tablet1, tablet2});

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, root);
        EXPECT_EQ(TraverseNaively(root, false, {}, {}), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2,
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk3,
            0,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(2);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);
        EXPECT_EQ(TraverseNaively(root, false, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2,
            0,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(2);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(3);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);
        EXPECT_EQ(TraverseNaively(root, false, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk3,
            0,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetKey(BuildKey("1"));

        TReadLimit upperLimit;
        upperLimit.SetKey(BuildKey("5"));

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);
        EXPECT_EQ(TraverseNaively(root, false, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2,
            0,
            TReadLimit(),
            upperLimit));
        correctResult.insert(TChunkInfo(
            chunk3,
            0,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
        EXPECT_EQ(TraverseNaively(root, false, false, lowerLimit, upperLimit), visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingTest, SortedDynamicWithChunkView)
{
    //            root             //
    //              |              //
    //           tablet1           //
    //       /      |      \       //
    //   view1   chunk2    view2   //
    //       \             /       //
    //           chunk1            //

    auto chunk1 = CreateChunk(1, 1, 1, 1, BuildKey("1"), BuildKey("9"));
    auto chunk2 = CreateChunk(2, 2, 2, 2, BuildKey("4"), BuildKey("6"));

    auto view1 = CreateChunkView(chunk1, BuildKey("1"), BuildKey("4"));
    auto view2 = CreateChunkView(chunk1, BuildKey("7"), BuildKey("10"));

    auto root = CreateChunkList(EChunkListKind::SortedDynamicRoot);
    auto tablet1 = CreateChunkList(EChunkListKind::SortedDynamicTablet);
    tablet1->SetPivotKey(BuildKey(""));

    AttachToChunkList(tablet1, {view1, chunk2, view2});
    AttachToChunkList(root, {tablet1});

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto chunksAndViews = EnumerateChunksAndChunkViewsInChunkTree(root);
        std::vector<TChunkTree*> correct{view1, chunk2, view2};

        EXPECT_EQ(correct, chunksAndViews);
    }

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, root);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(BuildKey("1")),
            TReadLimit(BuildKey("4"))));
        correctResult.insert(TChunkInfo(
            chunk2,
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(BuildKey("7")),
            TReadLimit(BuildKey("10"))));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(2);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(BuildKey("1")),
            TReadLimit(BuildKey("4"))));
        correctResult.insert(TChunkInfo(
            chunk2,
            0,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(2);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(3);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(BuildKey("7")),
            TReadLimit(BuildKey("10"))));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetKey(BuildKey("1"));

        TReadLimit upperLimit;
        upperLimit.SetKey(BuildKey("5"));

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(BuildKey("1")),
            TReadLimit(BuildKey("4"))));
        correctResult.insert(TChunkInfo(
            chunk2,
            0,
            TReadLimit(),
            TReadLimit(BuildKey("5"))));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetKey(BuildKey("2"));

        TReadLimit upperLimit;
        upperLimit.SetKey(BuildKey("8"));

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(BuildKey("2")),
            TReadLimit(BuildKey("4"))));
        correctResult.insert(TChunkInfo(
            chunk2,
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(BuildKey("7")),
            TReadLimit(BuildKey("8"))));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingTest, SortedDynamicChunkShared)
{
    //          root            //
    //        /   |   \         //
    // tablet1 tablet2 tablet3  //
    //        \   |   /         //
    //          chunk           //

    auto chunk = CreateChunk(1, 1, 1, 1, BuildKey("0"), BuildKey("6"));

    auto root = CreateChunkList(EChunkListKind::SortedDynamicRoot);
    auto tablet1 = CreateChunkList(EChunkListKind::SortedDynamicTablet);
    auto tablet2 = CreateChunkList(EChunkListKind::SortedDynamicTablet);
    auto tablet3 = CreateChunkList(EChunkListKind::SortedDynamicTablet);
    tablet1->SetPivotKey(BuildKey(""));
    tablet2->SetPivotKey(BuildKey("2"));
    tablet3->SetPivotKey(BuildKey("4"));

    AttachToChunkList(tablet1, std::vector<TChunkTree*>{chunk});
    AttachToChunkList(tablet2, std::vector<TChunkTree*>{chunk});
    AttachToChunkList(tablet3, std::vector<TChunkTree*>{chunk});
    AttachToChunkList(root, std::vector<TChunkTree*>{tablet1, tablet2, tablet3});

    TReadLimit limit2;
    limit2.SetKey(BuildKey("2"));

    TReadLimit limit4;
    limit4.SetKey(BuildKey("4"));

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, root);
        EXPECT_EQ(TraverseNaively(root, false, {}, {}), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk,
            0,
            TReadLimit(),
            limit2));
        correctResult.insert(TChunkInfo(
            chunk,
            0,
            limit2,
            limit4));
        correctResult.insert(TChunkInfo(
            chunk,
            0,
            limit4,
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetKey(BuildKey("2"));

        TReadLimit upperLimit;
        upperLimit.SetKey(BuildKey("4"));

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);
        EXPECT_EQ(TraverseNaively(root, false, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk,
            0,
            limit2,
            limit4));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetKey(BuildKey("1"));

        TReadLimit upperLimit;
        upperLimit.SetKey(BuildKey("5"));

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);
        EXPECT_EQ(TraverseNaively(root, false, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk,
            0,
            lowerLimit,
            limit2));
        correctResult.insert(TChunkInfo(
            chunk,
            0,
            limit2,
            limit4));
        correctResult.insert(TChunkInfo(
            chunk,
            0,
            limit4,
            upperLimit));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingTest, OrderedDynamic)
{
    //     root               //
    //    /    \              //
    // tablet1  tablet2       //
    //  /       /     \       //
    // chunk1  chunk2  chunk3 //

    auto chunk1 = CreateChunk(1, 1, 1, 1);
    auto chunk2 = CreateChunk(2, 2, 2, 2);
    auto chunk3 = CreateChunk(3, 3, 3, 3);

    auto root = CreateChunkList(EChunkListKind::OrderedDynamicRoot);
    auto tablet1 = CreateChunkList(EChunkListKind::OrderedDynamicTablet);
    auto tablet2 = CreateChunkList(EChunkListKind::OrderedDynamicTablet);

    AttachToChunkList(tablet1, std::vector<TChunkTree*>{chunk1});
    AttachToChunkList(tablet2, std::vector<TChunkTree*>{chunk2, chunk3});
    AttachToChunkList(root, std::vector<TChunkTree*>{tablet1, tablet2});

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, root);
        EXPECT_EQ(TraverseNaively(root, true, true, {}, {}), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2,
            0,
            1,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk3,
            2,
            1,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingTest, StartIndex)
{
    //         root            //
    //      /    |    \        //
    //     /     |     \       //
    // chunk1  chunk2  chunk3  //

    auto chunk1 = CreateChunk(1, 1, 1, 1);
    auto chunk2 = CreateChunk(1, 2, 2, 2);
    auto chunk3 = CreateChunk(1, 3, 3, 3);

    auto root = CreateChunkList(EChunkListKind::Static);

    AttachToChunkList(root, std::vector<TChunkTree*>{chunk1, chunk2, chunk3});

    root->Statistics().Sealed = false;
    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetRowIndex(0);

        TReadLimit upperLimit;
        upperLimit.SetRowIndex(1);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetRowIndex(1);

        TReadLimit upperLimit;
        upperLimit.SetRowIndex(2);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk2,
            1,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetRowIndex(2);

        TReadLimit upperLimit;
        upperLimit.SetRowIndex(3);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        TReadLimit upperLimitResult;
        upperLimitResult.SetRowIndex(1);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk3,
            2,
            TReadLimit(),
            upperLimitResult));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingTest, ReadFromDynamicOrderedAfterTrim)
{
    auto* root = CreateChunkList(EChunkListKind::OrderedDynamicTablet);
    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(1, 1, 1, 1);
    auto* chunk3 = CreateChunk(1, 1, 1, 1);
    auto* chunk4 = CreateChunk(1, 1, 1, 1);
    AttachToChunkList(root, {chunk1, chunk2, chunk3, chunk4});

    DetachFromChunkList(root, {chunk1});
    root->Children().erase(root->Children().begin());
    root->CumulativeStatistics().TrimFront(1);
    YT_VERIFY(root->Children().size() == 3);
    YT_VERIFY(root->CumulativeStatistics().Size() == 3);

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(1);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(2);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk2,
            1,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(1);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(2);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk2,
            1,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(3);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk2,
            1,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk3,
            2,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingTest, OrderedDynamicEmptyTablet)
{
    auto* root = CreateChunkList(EChunkListKind::OrderedDynamicRoot);
    auto* tablet1 = CreateChunkList(EChunkListKind::OrderedDynamicTablet);
    auto* tablet2 = CreateChunkList(EChunkListKind::OrderedDynamicTablet);
    auto* tablet3 = CreateChunkList(EChunkListKind::OrderedDynamicTablet);
    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(1, 1, 1, 1);
    AttachToChunkList(root, {tablet1, tablet2, tablet3});
    AttachToChunkList(tablet1, {chunk1});
    AttachToChunkList(tablet3, {chunk2});

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(2);

        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1,
            0,
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2,
            0,
            2,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeTraversingStressTest
    : public TChunkGeneratorBase
{ };

TEST_F(TChunkTreeTraversingStressTest, StaticWithoutKeys)
{
    std::mt19937 gen;
    auto* root = GenerateChunkTree(this, 3, gen, {}, {
        EChunkListKind::Static,
        EChunkListKind::Static,
        EChunkListKind::Static,
    })->AsChunkList();

    const auto& statistics = GetChunkTreeStatistics(root);

    auto generateOrderedPair = [&] (int bound) {
        int lhs = gen() % bound;
        int rhs = gen() % bound;
        if (lhs > rhs) {
            std::swap(lhs, rhs);
        }
        return std::make_pair(lhs, rhs);
    };

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    for (int iter = 0; iter < 100; ++iter) {
        auto chunkIndices = generateOrderedPair(statistics.ChunkCount);
        auto rowIndices = generateOrderedPair(statistics.RowCount);

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(chunkIndices.first);
        lowerLimit.SetRowIndex(rowIndices.first);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(chunkIndices.second);
        upperLimit.SetRowIndex(rowIndices.second);

        auto expected = TraverseNaively(root, true, false, lowerLimit, upperLimit);

        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        EXPECT_EQ(expected, visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingStressTest, SortedDynamic)
{
    std::mt19937 gen;
    std::function<int()> keyYielder = CreateBasicKeyYielder();
    auto* root = GenerateChunkTree(this, 2, gen, keyYielder, {
        EChunkListKind::SortedDynamicRoot,
        EChunkListKind::SortedDynamicTablet,
    })->AsChunkList();

    const auto& statistics = GetChunkTreeStatistics(root);

    auto generateOrderedPair = [&] (int bound) {
        int lhs = gen() % bound;
        int rhs = gen() % bound;
        if (lhs > rhs) {
            std::swap(lhs, rhs);
        }
        return std::make_pair(lhs, rhs);
    };

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    for (int iter = 0; iter < 100; ++iter) {
        {
            auto chunkIndices = generateOrderedPair(statistics.ChunkCount);

            TReadLimit lowerLimit;
            lowerLimit.SetChunkIndex(chunkIndices.first);

            TReadLimit upperLimit;
            upperLimit.SetChunkIndex(chunkIndices.second);

            auto expected = TraverseNaively(root, false, false, lowerLimit, upperLimit);

            auto visitor = New<TTestChunkVisitor>();
            TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

            EXPECT_EQ(expected, visitor->GetChunkInfos());
        }

        {
            auto keys = generateOrderedPair(5 * 5 * 2 * 10);

            TReadLimit lowerLimit;
            lowerLimit.SetKey(BuildKey(ToString(keys.first)));

            TReadLimit upperLimit;
            upperLimit.SetKey(BuildKey(ToString(keys.second)));

            auto expected = TraverseNaively(root, false, false, lowerLimit, upperLimit);

            auto visitor = New<TTestChunkVisitor>();
            TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

            EXPECT_EQ(expected, visitor->GetChunkInfos());
        }
    }
}

TEST_F(TChunkTreeTraversingStressTest, SortedDynamicThreeLevel)
{
    std::mt19937 gen;
    std::function<int()> keyYielder = CreateBasicKeyYielder();
    auto* root = GenerateChunkTree(this, 2, gen, keyYielder, {
        EChunkListKind::SortedDynamicRoot,
        EChunkListKind::SortedDynamicTablet,
        EChunkListKind::SortedDynamicSubtablet,
    })->AsChunkList();

    const auto& statistics = GetChunkTreeStatistics(root);

    auto generateOrderedPair = [&] (int bound) {
        int lhs = gen() % bound;
        int rhs = gen() % bound;
        if (lhs > rhs) {
            std::swap(lhs, rhs);
        }
        return std::make_pair(lhs, rhs);
    };

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    for (int iter = 0; iter < 100; ++iter) {
        {
            auto chunkIndices = generateOrderedPair(statistics.ChunkCount);

            TReadLimit lowerLimit;
            lowerLimit.SetChunkIndex(chunkIndices.first);

            TReadLimit upperLimit;
            upperLimit.SetChunkIndex(chunkIndices.second);

            auto expected = TraverseNaively(root, false, false, lowerLimit, upperLimit);

            auto visitor = New<TTestChunkVisitor>();
            TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

            EXPECT_EQ(expected, visitor->GetChunkInfos());
        }

        {
            auto keys = generateOrderedPair(5 * 5 * 5 * 2 * 10);

            TReadLimit lowerLimit;
            lowerLimit.SetKey(BuildKey(ToString(keys.first)));

            TReadLimit upperLimit;
            upperLimit.SetKey(BuildKey(ToString(keys.second)));

            auto expected = TraverseNaively(root, false, false, lowerLimit, upperLimit);

            auto visitor = New<TTestChunkVisitor>();
            TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

            EXPECT_EQ(expected, visitor->GetChunkInfos());
        }
    }
}

TEST_F(TChunkTreeTraversingStressTest, OrderedDynamic)
{
    std::mt19937 gen;
    auto* root = GenerateChunkTree(this, 2, gen, {}, {
        EChunkListKind::OrderedDynamicRoot,
        EChunkListKind::OrderedDynamicTablet,
    })->AsChunkList();

    const auto& statistics = GetChunkTreeStatistics(root);

    auto generateOrderedPair = [&] (int bound) {
        int lhs = gen() % bound;
        int rhs = gen() % bound;
        if (lhs > rhs) {
            std::swap(lhs, rhs);
        }
        return std::make_pair(lhs, rhs);
    };

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    for (int iter = 0; iter < 100; ++iter) {
        auto chunkIndices = generateOrderedPair(statistics.ChunkCount);

        TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(chunkIndices.first);

        TReadLimit upperLimit;
        upperLimit.SetChunkIndex(chunkIndices.second);

        auto expected = TraverseNaively(root, true, true, lowerLimit, upperLimit);

        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);

        EXPECT_EQ(expected, visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingStressTest, OrderedDynamicWithTabletIndex)
{
    std::mt19937 gen;
    std::uniform_real_distribution<> dist;
    auto* root = GenerateChunkTree(this, 2, gen, {}, {
        EChunkListKind::OrderedDynamicRoot,
        EChunkListKind::OrderedDynamicTablet,
    })->AsChunkList();

    int tabletCount = root->Children().size();

    std::vector<int> tabletStartRowCount(root->Children().size(), 0);

    std::vector<int> rowsPerTablet;
    rowsPerTablet.reserve(tabletCount);
    int totalRowCount = 0;
    for (int tabletIndex = 0; tabletIndex < tabletCount; ++tabletIndex) {
        int rowCount = GetChunkTreeStatistics(root->Children()[tabletIndex]).LogicalRowCount;
        rowsPerTablet.emplace_back(rowCount);
        totalRowCount += rowsPerTablet.back();
    }

    auto getRangeWithOverflow = [] (int count) {
        // Additionally check out of borders case.
        return count + (count / 10) + 2;
    };

    rowsPerTablet.resize(getRangeWithOverflow(tabletCount), 5);

    auto generateLimits = [&] () {
        int tabletIndexRange = getRangeWithOverflow(tabletCount);

        int leftTabletIndex = gen() % tabletIndexRange;
        int rightTabletIndex = gen() % tabletIndexRange;
        if (leftTabletIndex > rightTabletIndex) {
            std::swap(leftTabletIndex, rightTabletIndex);
        }

        int leftRowIndexRange = getRangeWithOverflow(rowsPerTablet[leftTabletIndex]);
        int rightRowIndexRange = getRangeWithOverflow(rowsPerTablet[rightTabletIndex]);
        int leftRowIndex = gen() % leftRowIndexRange;
        int rightRowIndex = gen() % rightRowIndexRange;

        TReadLimit lowerLimit;
        TReadLimit upperLimit;

        lowerLimit.SetTabletIndex(leftTabletIndex);
        upperLimit.SetTabletIndex(rightTabletIndex);

        if (ForgetRowIndexProbability < dist(gen)) {
            lowerLimit.SetRowIndex(leftRowIndex);
            upperLimit.SetRowIndex(rightRowIndex);
        }

        if (ForgetReadLimitProbability > dist(gen)) {
            lowerLimit = TReadLimit{};
        }
        if (ForgetReadLimitProbability > dist(gen)) {
            upperLimit = TReadLimit{};
        }

        return std::make_pair(lowerLimit, upperLimit);
    };

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    for (auto iter = 0; iter < 5000; ++iter) {
        const auto& [lowerLimit, upperLimit] = generateLimits();

        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, root, lowerLimit, upperLimit);
        auto expected = TraverseNaively(root, true, true, lowerLimit, upperLimit, tabletStartRowCount);

        EXPECT_EQ(expected, visitor->GetChunkInfos());

        if (iter % 100 == 0) {
            int tabletIndexToTrim = gen() % tabletCount;
            const auto& tabletToTrim = root->Children()[tabletIndexToTrim]->AsChunkList();

            int oldChildCount = tabletToTrim->Children().size();
            if (oldChildCount == 1) {
                continue;
            }

            tabletStartRowCount[tabletIndexToTrim] = tabletToTrim->CumulativeStatistics().GetCurrentSum(0).RowCount;

            // These functions implement DetachFromChunkList with force detach.
            tabletToTrim->Children().erase(tabletToTrim->Children().begin());
            tabletToTrim->CumulativeStatistics().TrimFront(1);
            tabletToTrim->SetTrimmedChildCount(0);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
