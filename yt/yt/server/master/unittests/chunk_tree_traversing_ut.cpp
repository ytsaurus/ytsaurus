#include "chunk_helpers.h"
#include "helpers.h"

#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_view.h>
#include <yt/yt/server/master/chunk_server/chunk_tree_traverser.h>
#include <yt/yt/server/master/chunk_server/helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/test_framework/framework.h>

#include <random>

namespace NYT::NChunkServer {
namespace {

using namespace NTesting;

using namespace NCellMaster;
using namespace NChunkClient::NProto;
using namespace NCypressClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TLegacyReadLimit;
using NChunkClient::TReadLimit;
using NChunkClient::ReadLimitToLegacyReadLimit;
using NChunkClient::ReadLimitFromLegacyReadLimit;

////////////////////////////////////////////////////////////////////////////////

static constexpr double ForgetRowIndexProbability = 0.1;
static constexpr double ForgetReadLimitProbability = 0.5;

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TLegacyReadLimit& lhs, const TLegacyReadLimit& rhs)
{
    return lhs.AsProto().DebugString() == rhs.AsProto().DebugString();
}

////////////////////////////////////////////////////////////////////////////////

struct TChunkInfo
{
public:
    TChunkInfo(
        TChunk* chunk,
        std::optional<i64> rowIndex = {},
        std::optional<int> tabletIndex = {},
        TLegacyReadLimit lowerLimit = {},
        TLegacyReadLimit upperLimit = {})
        : Chunk(chunk)
        , RowIndex(rowIndex)
        , TabletIndex(tabletIndex)
        , LowerLimit(lowerLimit)
        , UpperLimit(upperLimit)
    { }

    TChunk* Chunk;
    std::optional<i64> RowIndex;
    std::optional<int> TabletIndex;
    TLegacyReadLimit LowerLimit;
    TLegacyReadLimit UpperLimit;
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
    os << "ChunkInfo(Id=" << ToString(chunkInfo.Chunk->GetId())
       << ", RowIndex=" << ToString(chunkInfo.RowIndex)
       << ", TabletIndex=" << ToString(chunkInfo.TabletIndex)
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
    bool OnChunk(
        TChunk* chunk,
        TChunkList* /*parent*/,
        std::optional<i64> rowIndex,
        std::optional<int> tabletIndex,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit,
        const TChunkViewModifier* /*modifier*/) override
    {
        // COMPAT(max42).
        auto legacyLowerLimit = ReadLimitToLegacyReadLimit(lowerLimit);
        auto legacyUpperLimit = ReadLimitToLegacyReadLimit(upperLimit);

        ChunkInfos.insert(TChunkInfo(chunk, rowIndex, tabletIndex, legacyLowerLimit, legacyUpperLimit));
        return true;
    }

    bool OnChunkView(TChunkView*) override
    {
        return false;
    }

    bool OnDynamicStore(
        TDynamicStore*,
        std::optional<i32>,
        const TReadLimit&,
        const TReadLimit&) override
    {
        return true;
    }

    void OnFinish(const TError& error) override
    {
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    const std::set<TChunkInfo>& GetChunkInfos() const
    {
        return ChunkInfos;
    }

private:
    std::set<TChunkInfo> ChunkInfos;
};

////////////////////////////////////////////////////////////////////////////////

class TTestTraverserContext
    : public IChunkTraverserContext
{
public:
    bool IsSynchronous() const override
    {
        return true;
    }

    IInvokerPtr GetInvoker() const override
    {
        YT_ABORT();
    }

    void OnTimeSpent(TDuration /*time*/) override
    { }

    TFuture<TUnsealedChunkStatistics> GetUnsealedChunkStatistics(TChunk* chunk) override
    {
        const auto& statistics = GetOrCrash(ChunkIdToUnsealedChunkStatistics_, chunk->GetId());
        return MakeFuture<TUnsealedChunkStatistics>(statistics);
    }

    void SetUnsealedChunkStatistics(TChunk* chunk, TUnsealedChunkStatistics statistics)
    {
        ChunkIdToUnsealedChunkStatistics_[chunk->GetId()] = statistics;
    }

private:
    THashMap<TChunkId, TUnsealedChunkStatistics> ChunkIdToUnsealedChunkStatistics_;
};

////////////////////////////////////////////////////////////////////////////////

class TNaiveChunkTreeTraverser
{
public:
    std::set<TChunkInfo> Traverse(
        TChunkList* chunkList,
        bool calculateRowIndex,
        bool calculateTabletIndex,
        const TLegacyReadLimit& lowerLimit = TLegacyReadLimit{},
        const TLegacyReadLimit& upperLimit = TLegacyReadLimit{},
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
    TLegacyReadLimit LowerLimit_;
    TLegacyReadLimit UpperLimit_;

    std::set<TChunkInfo> Result_;
    int ChunkCount_ = 0;
    i64 RowCount_ = 0;
    std::optional<int> TabletIndex_;

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
        if (UpperLimit_.HasRowIndex()) {
            return UpperLimit_.GetRowIndex();
        } else if (UpperLimit_.HasTabletIndex()) {
            return 0;
        } else {
            return std::numeric_limits<i64>::max();
        }
    }

    TLegacyOwningKey LowerKeyLimit() const
    {
        return LowerLimit_.HasLegacyKey() ? LowerLimit_.GetLegacyKey() : MinKey();
    }

    TLegacyOwningKey UpperKeyLimit() const
    {
        return UpperLimit_.HasLegacyKey() ? UpperLimit_.GetLegacyKey() : MaxKey();
    }

    i32 LowerTabletIndexLimit() const
    {
        return LowerLimit_.HasTabletIndex() ? LowerLimit_.GetTabletIndex() : 0;
    }

    i32 UpperTabletIndexLimit() const
    {
        return UpperLimit_.HasTabletIndex() ? UpperLimit_.GetTabletIndex() : std::numeric_limits<i32>::max();
    }

    TLegacyOwningKey GetNextPivotKey(TChunkList* tabletChunkList)
    {
        auto* parent = GetUniqueParent(tabletChunkList);
        int index = parent->ChildToIndex()[tabletChunkList];
        return index + 1 == std::ssize(parent->Children())
            ? MaxKey()
            : parent->Children()[index + 1]->AsChunkList()->GetPivotKey();;
    }

    bool HasBoundaryKeys(TChunk* chunk)
    {
        if (!chunk->ChunkMeta()->HasExtension<NTableClient::NProto::TBoundaryKeysExt>()) {
            return false;
        }
        // It is assumed that min and max keys either both exist or both do not.
        return static_cast<bool>(GetMinKeyOrThrow(chunk));
    }

    bool TraverseChunkList(
        TChunkList* chunkList,
        TLegacyOwningKey lowerKeyLimit = MinKey(),
        TLegacyOwningKey upperKeyLimit = MaxKey())
    {
        auto updateTabletIndex = [&] () {
            if (chunkList->GetKind() == EChunkListKind::OrderedDynamicRoot) {
                ++*TabletIndex_;
                // Is actually used only for trimmed chunks in ordered tables.
                RowCount_ = *TabletIndex_ < std::ssize(TabletStartRowCount_) ? TabletStartRowCount_[*TabletIndex_] : 0;
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
                if (readRange.LowerLimit().HasLegacyKey()) {
                    lowerKeyLimit = ChooseMaxKey(lowerKeyLimit, readRange.LowerLimit().GetLegacyKey());
                }
                if (readRange.UpperLimit().HasLegacyKey()) {
                    upperKeyLimit = ChooseMinKey(upperKeyLimit, readRange.UpperLimit().GetLegacyKey());
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
        const TLegacyOwningKey& lowerKeyLimit = MinKey(),
        const TLegacyOwningKey& upperKeyLimit = MaxKey())
    {
        int newChunkCount = ChunkCount_ + 1;
        int newRowCount = RowCount_ + GetChunkTreeStatistics(chunk).LogicalRowCount;

        auto finally = Finally([&] {
            ChunkCount_ = newChunkCount;
            RowCount_ = newRowCount;
        });

        TLegacyReadLimit correctLowerLimit;
        TLegacyReadLimit correctUpperLimit;

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
                    correctLowerLimit.SetLegacyKey(lowerKeyLimit);
                }
                if (upperKeyLimit <= GetUpperBoundKeyOrThrow(chunk)) {
                    correctUpperLimit.SetLegacyKey(upperKeyLimit);
                }
            }
        }

        Result_.emplace(
            chunk,
            CalculateRowIndex_ ? std::optional(RowCount_) : std::nullopt,
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
    const TLegacyReadLimit& lowerLimit = {},
    const TLegacyReadLimit& upperLimit = {},
    const std::vector<int>& tabletStartRowCount = {})
{
    TNaiveChunkTreeTraverser naiveTraverser;
    return naiveTraverser.Traverse(
        chunkList,
        calculateRowIndex,
        calculateTabletIndex,
        lowerLimit,
        upperLimit,
        tabletStartRowCount);
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
        TLegacyOwningKey minKey;
        TLegacyOwningKey maxKey;
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
        std::vector<TChunkTree*> items{
            chunk2,
            chunk3
        };
        AttachToChunkList(listB, items);
    }

    {
        std::vector<TChunkTree*> items{
            chunk1,
            listB
        };
        AttachToChunkList(listA, items);
    }

    auto context = GetSyncChunkTraverserContext();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(context, visitor, listA, TReadLimit{} /*lowerLimit*/, {} /*upperLimit*/, {} /*keyColumnCount*/);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1,
                0),
            TChunkInfo(
                chunk2,
                1),
            TChunkInfo(
                chunk3,
                3)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());

        EXPECT_EQ(TraverseNaively(listA, true), visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetRowIndex(2);

        TLegacyReadLimit upperLimit;
        upperLimit.SetRowIndex(5);

        TraverseChunkTree(context, visitor, listA, lowerLimit, upperLimit, {} /*keyColumnCount*/);

        TLegacyReadLimit correctLowerLimit;
        correctLowerLimit.SetRowIndex(1);

        TLegacyReadLimit correctUpperLimit;
        correctUpperLimit.SetRowIndex(2);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk2,
                1,
                {} /*tabletIndex*/,
                correctLowerLimit,
                TLegacyReadLimit()),
            TChunkInfo(
                chunk3,
                3,
                {} /*tabletIndex*/,
                TLegacyReadLimit(),
                correctUpperLimit)
        };
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
        std::vector<TChunkTree*> items{
            empty1,
            chunk1,
            empty2,
            chunk2,
            empty3
        };
        AttachToChunkList(list, items);
    }

    auto context = GetSyncChunkTraverserContext();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(context, visitor, list, TReadLimit{} /*lowerLimit*/, {} /*upperLimit*/, {} /*keyColumnCount*/);
        EXPECT_EQ(TraverseNaively(list, true, {}, {}), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1,
                0),
            TChunkInfo(
                chunk2,
                1)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetRowIndex(1);

        TLegacyReadLimit upperLimit;
        upperLimit.SetRowIndex(2);

        TraverseChunkTree(context, visitor, list, lowerLimit, upperLimit, {} /*keyColumnCount*/);
        EXPECT_EQ(TraverseNaively(list, true, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        TLegacyReadLimit correctLowerLimit;

        TLegacyReadLimit correctUpperLimit;
        correctUpperLimit.SetRowIndex(1);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk2,
                1,
                {} /*tabletIndex*/,
                correctLowerLimit,
                correctUpperLimit)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
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

    auto context = GetSyncChunkTraverserContext();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(context, visitor, root, TReadLimit{} /*lowerLimit*/, {} /*upperLimit*/, MakeComparator(1));
        EXPECT_EQ(TraverseNaively(root, false, {}, {}), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult{
            TChunkInfo(chunk1),
            TChunkInfo(chunk2),
            TChunkInfo(chunk3)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TLegacyReadLimit upperLimit;
        upperLimit.SetChunkIndex(2);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));
        EXPECT_EQ(TraverseNaively(root, false, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult{
            TChunkInfo(chunk1),
            TChunkInfo(chunk2)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(2);

        TLegacyReadLimit upperLimit;
        upperLimit.SetChunkIndex(3);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));
        EXPECT_EQ(TraverseNaively(root, false, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult{
            TChunkInfo(chunk3)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetLegacyKey(BuildKey("1"));

        TLegacyReadLimit upperLimit;
        upperLimit.SetLegacyKey(BuildKey("5"));

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));
        EXPECT_EQ(TraverseNaively(root, false, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1),
            TChunkInfo(
                chunk2,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                TLegacyReadLimit(),
                upperLimit),
            TChunkInfo(
                chunk3)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
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

    auto context = GetSyncChunkTraverserContext();

    {
        auto stores = EnumerateStoresInChunkTree(root);
        std::vector<TChunkTree*> correct{
            view1,
            chunk2,
            view2
        };

        EXPECT_EQ(correct, stores);
    }

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(context, visitor, root, TReadLimit{} /*lowerLimit*/, {} /*upperLimit*/, MakeComparator(1));

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                TLegacyReadLimit(BuildKey("1")),
                TLegacyReadLimit(BuildKey("4"))),
            TChunkInfo(
                chunk2),
            TChunkInfo(
                chunk1,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                TLegacyReadLimit(BuildKey("7")),
                TLegacyReadLimit(BuildKey("10")))
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TLegacyReadLimit upperLimit;
        upperLimit.SetChunkIndex(2);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                TLegacyReadLimit(BuildKey("1")),
                TLegacyReadLimit(BuildKey("4"))),
            TChunkInfo(
                chunk2)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(2);

        TLegacyReadLimit upperLimit;
        upperLimit.SetChunkIndex(3);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                TLegacyReadLimit(BuildKey("7")),
                TLegacyReadLimit(BuildKey("10")))
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetLegacyKey(BuildKey("1"));

        TLegacyReadLimit upperLimit;
        upperLimit.SetLegacyKey(BuildKey("5"));

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                TLegacyReadLimit(BuildKey("1")),
                TLegacyReadLimit(BuildKey("4"))),
            TChunkInfo(
                chunk2,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                TLegacyReadLimit(),
                TLegacyReadLimit(BuildKey("5")))
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetLegacyKey(BuildKey("2"));

        TLegacyReadLimit upperLimit;
        upperLimit.SetLegacyKey(BuildKey("8"));

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                TLegacyReadLimit(BuildKey("2")),
                TLegacyReadLimit(BuildKey("4"))),
            TChunkInfo(
                chunk2),
            TChunkInfo(
                chunk1,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                TLegacyReadLimit(BuildKey("7")),
                TLegacyReadLimit(BuildKey("8")))
        };
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

    TLegacyReadLimit limit2;
    limit2.SetLegacyKey(BuildKey("2"));

    TLegacyReadLimit limit4;
    limit4.SetLegacyKey(BuildKey("4"));

    auto context = GetSyncChunkTraverserContext();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(context, visitor, root, TReadLimit{} /*lowerLimit*/, {} /*upperLimit*/, MakeComparator(1));
        EXPECT_EQ(TraverseNaively(root, false, {}, {}), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                TLegacyReadLimit(),
                limit2),
            TChunkInfo(
                chunk,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                limit2,
                limit4),
            TChunkInfo(
                chunk,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                limit4,
                TLegacyReadLimit()),
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetLegacyKey(BuildKey("2"));

        TLegacyReadLimit upperLimit;
        upperLimit.SetLegacyKey(BuildKey("4"));

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));
        EXPECT_EQ(TraverseNaively(root, false, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                limit2,
                limit4)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetLegacyKey(BuildKey("1"));

        TLegacyReadLimit upperLimit;
        upperLimit.SetLegacyKey(BuildKey("5"));

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));
        EXPECT_EQ(TraverseNaively(root, false, false, lowerLimit, upperLimit), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                lowerLimit,
                limit2),
            TChunkInfo(
                chunk,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                limit2,
                limit4),
            TChunkInfo(
                chunk,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                limit4,
                upperLimit)
        };
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

    auto context = GetSyncChunkTraverserContext();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(context, visitor, root, TReadLimit{} /*lowerLimit*/, {} /*upperLimit*/, {} /*keyColumnCount*/);
        EXPECT_EQ(TraverseNaively(root, true, true, {}, {}), visitor->GetChunkInfos());

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1,
                0,
                0),
            TChunkInfo(
                chunk2,
                0,
                1),
            TChunkInfo(
                chunk3,
                2,
                1)
        };
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

    auto context = GetSyncChunkTraverserContext();

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetRowIndex(0);

        TLegacyReadLimit upperLimit;
        upperLimit.SetRowIndex(1);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1,
                0)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetRowIndex(1);

        TLegacyReadLimit upperLimit;
        upperLimit.SetRowIndex(2);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk2,
                1)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetRowIndex(2);

        TLegacyReadLimit upperLimit;
        upperLimit.SetRowIndex(3);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk3,
                2)
        };
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

    DetachFromChunkList(root, {chunk1}, EChunkDetachPolicy::OrderedTabletPrefix);
    root->Children().erase(root->Children().begin());
    root->CumulativeStatistics().TrimFront(1);
    YT_VERIFY(root->Children().size() == 3);
    YT_VERIFY(root->CumulativeStatistics().Size() == 3);

    auto context = GetSyncChunkTraverserContext();

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TLegacyReadLimit upperLimit;
        upperLimit.SetChunkIndex(1);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

        std::set<TChunkInfo> correctResult;
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TLegacyReadLimit upperLimit;
        upperLimit.SetChunkIndex(2);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk2,
                1)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(1);

        TLegacyReadLimit upperLimit;
        upperLimit.SetChunkIndex(2);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk2,
                1)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TLegacyReadLimit upperLimit;
        upperLimit.SetChunkIndex(3);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk2,
                1),
            TChunkInfo(
                chunk3,
                2)
        };
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

    auto context = GetSyncChunkTraverserContext();

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(0);

        TLegacyReadLimit upperLimit;
        upperLimit.SetChunkIndex(2);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1,
                0,
                0),
            TChunkInfo(
                chunk2,
                0,
                2)
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingTest, SemiSealedJournal)
{
    // NB: Sealed journal chunks in tests have 100 rows by default.
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    auto* chunk1 = CreateJournalChunk(/*sealed*/ true, /*overlayed*/ false);
    auto* chunk2 = CreateJournalChunk(/*sealed*/ false, /*overlayed*/ false);
    AttachToChunkList(root, {chunk1});
    AttachToChunkList(root, {chunk2});

    auto context = New<TTestTraverserContext>();
    {
        IChunkTraverserContext::TUnsealedChunkStatistics statistics{
            .RowCount = 100
        };
        context->SetUnsealedChunkStatistics(chunk2, statistics);
    }

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetRowIndex(10);

        TLegacyReadLimit upperLimit;
        upperLimit.SetRowIndex(120);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk1,
                /*rowIndex*/ 0,
                /*tabletIndex*/ std::nullopt,
                TLegacyReadLimit().SetRowIndex(10),
                TLegacyReadLimit().SetRowIndex(100)),
            TChunkInfo(
                chunk2,
                /*rowIndex*/ 100,
                /*tabletIndex*/ std::nullopt,
                TLegacyReadLimit().SetRowIndex(0),
                TLegacyReadLimit().SetRowIndex(20)),
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetRowIndex(110);

        TLegacyReadLimit upperLimit;
        upperLimit.SetRowIndex(250);

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

        std::set<TChunkInfo> correctResult{
            TChunkInfo(
                chunk2,
                /*rowIndex*/ 100,
                /*tabletIndex*/ std::nullopt,
                TLegacyReadLimit().SetRowIndex(10),
                TLegacyReadLimit().SetRowIndex(100)),
        };
        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST_F(TChunkTreeTraversingTest, UnconfirmedChunk)
{
    auto chunk = CreateUnconfirmedChunk();
    auto chunkList = CreateChunkList();
    AttachToChunkList(chunkList, {chunk});

    auto context = GetSyncChunkTraverserContext();
    auto visitor = New<TTestChunkVisitor>();
    EXPECT_THROW_WITH_SUBSTRING(
        TraverseChunkTree(
            context,
            visitor,
            chunkList,
            /*lowerLimit*/ TReadLimit{},
            /*upperLimit*/ TReadLimit{},
            /*keyColumnCount*/ {}),
        "Cannot traverse an object containing an unconfirmed chunk");
}

TEST_F(TChunkTreeTraversingTest, SortedHunkChunk)
{
    auto* mainRoot = CreateChunkList(EChunkListKind::SortedDynamicRoot);
    auto* mainTablet1 = CreateChunkList(EChunkListKind::SortedDynamicTablet);
    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    AttachToChunkList(mainRoot, {mainTablet1});
    AttachToChunkList(mainTablet1, {chunk1});

    auto* hunkRoot = CreateChunkList(EChunkListKind::HunkRoot);
    auto* hunkTablet1 = CreateChunkList(EChunkListKind::Hunk);
    auto* hunkChunk1 = CreateChunk(1, 1, 1, 1, {}, {}, EChunkType::Hunk);
    AttachToChunkList(hunkRoot, {hunkTablet1});
    AttachToChunkList(hunkTablet1, {hunkChunk1});

    auto context = GetSyncChunkTraverserContext();
    auto visitor = New<TTestChunkVisitor>();

    TChunkLists roots{
        {EChunkListContentType::Main, mainRoot},
        {EChunkListContentType::Hunk, hunkRoot},
    };

    TraverseChunkTree(context, visitor, roots);

    std::set<TChunkInfo> correctResult{
        TChunkInfo(chunk1),
        TChunkInfo(hunkChunk1),
    };
    EXPECT_EQ(correctResult, visitor->GetChunkInfos());
}

////////////////////////////////////////////////////////////////////////////////

class TTraverseWithKeyColumnCount
    : public TChunkTreeTraversingTest
    , public ::testing::WithParamInterface<std::tuple<
        int, TString, TString, std::optional<TLegacyReadLimit>, std::optional<TLegacyReadLimit>>>
{ };

TEST_P(TTraverseWithKeyColumnCount, TestStatic)
{
    const auto& params = GetParam();
    auto keyColumnCount = std::get<0>(params);

    auto chunk1 = CreateChunk(1, 1, 1, 1, BuildKey("0"), BuildKey("0"));
    auto chunk2 = CreateChunk(2, 2, 2, 2, BuildKey("1"), BuildKey("2"));

    auto root = CreateChunkList(EChunkListKind::Static);

    AttachToChunkList(root, std::vector<TChunkTree*>{chunk1, chunk2});

    auto context = GetSyncChunkTraverserContext();

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetLegacyKey(BuildKey(std::get<1>(params)));

        TLegacyReadLimit upperLimit;
        upperLimit.SetLegacyKey(BuildKey(std::get<2>(params)));

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(keyColumnCount));

        std::set<TChunkInfo> correctResult;

        if (auto firstChunk = std::get<3>(params)) {
            correctResult.insert(TChunkInfo(
                chunk1,
                0,
                {} /*tabletIndex*/,
                *firstChunk,
                TLegacyReadLimit()));
        }

        if (auto secondChunk = std::get<4>(params)) {
            correctResult.insert(TChunkInfo(
                chunk2,
                1,
                {} /*tabletIndex*/,
                TLegacyReadLimit(),
                *secondChunk));
        }

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST_P(TTraverseWithKeyColumnCount, TestDynamic)
{
    const auto& params = GetParam();
    auto keyColumnCount = std::get<0>(params);

    auto* root = CreateChunkList(EChunkListKind::SortedDynamicRoot);
    auto* tablet = CreateChunkList(EChunkListKind::SortedDynamicTablet);
    auto* chunk1 = CreateChunk(1, 1, 1, 1, BuildKey("0"), BuildKey("0"));
    auto* chunk2 = CreateChunk(2, 2, 2, 2, BuildKey("1"), BuildKey("2"));

    AttachToChunkList(root, {tablet});
    AttachToChunkList(tablet, {chunk1, chunk2});

    auto context = GetSyncChunkTraverserContext();

    {
        auto visitor = New<TTestChunkVisitor>();

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetLegacyKey(BuildKey(std::get<1>(params)));

        TLegacyReadLimit upperLimit;
        upperLimit.SetLegacyKey(BuildKey(std::get<2>(params)));

        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(keyColumnCount));

        std::set<TChunkInfo> correctResult;

        if (auto firstChunk = std::get<3>(params)) {
            correctResult.insert(TChunkInfo(
                chunk1,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                *firstChunk,
                TLegacyReadLimit()));
        }

        if (auto secondChunk = std::get<4>(params)) {
            correctResult.insert(TChunkInfo(
                chunk2,
                {} /*rowIndex*/,
                {} /*tabletIndex*/,
                TLegacyReadLimit(),
                *secondChunk));
        }

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTraverseWithKeyColumnCount,
    TTraverseWithKeyColumnCount,
    ::testing::Values(
        std::tuple(
            2,
            "0;<type=min>#",
            "2;<type=min>#",
            TLegacyReadLimit(),
            TLegacyReadLimit(BuildKey("2"))),
        std::tuple(
            2,
            "0;<type=null>#",
            "2;<type=null>#",
            TLegacyReadLimit(),
            TLegacyReadLimit(BuildKey("2;<type=null>#"))),
        std::tuple(
            2,
            "0;<type=max>#",
            "2;<type=max>#",
            std::nullopt,
            TLegacyReadLimit())));

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
        return std::pair(lhs, rhs);
    };

    auto context = GetSyncChunkTraverserContext();

    for (int iter = 0; iter < 100; ++iter) {
        auto chunkIndices = generateOrderedPair(statistics.ChunkCount);
        auto rowIndices = generateOrderedPair(statistics.RowCount);

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(chunkIndices.first);
        lowerLimit.SetRowIndex(rowIndices.first);

        TLegacyReadLimit upperLimit;
        upperLimit.SetChunkIndex(chunkIndices.second);
        upperLimit.SetRowIndex(rowIndices.second);

        auto expected = TraverseNaively(root, true, false, lowerLimit, upperLimit);

        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

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
        return std::pair(lhs, rhs);
    };

    auto context = GetSyncChunkTraverserContext();

    for (int iter = 0; iter < 100; ++iter) {
        {
            auto chunkIndices = generateOrderedPair(statistics.ChunkCount);

            TLegacyReadLimit lowerLimit;
            lowerLimit.SetChunkIndex(chunkIndices.first);

            TLegacyReadLimit upperLimit;
            upperLimit.SetChunkIndex(chunkIndices.second);

            auto expected = TraverseNaively(root, false, false, lowerLimit, upperLimit);

            auto visitor = New<TTestChunkVisitor>();
            TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));

            EXPECT_EQ(expected, visitor->GetChunkInfos());
        }

        {
            auto keys = generateOrderedPair(5 * 5 * 2 * 10);

            TLegacyReadLimit lowerLimit;
            lowerLimit.SetLegacyKey(BuildKey(ToString(keys.first)));

            TLegacyReadLimit upperLimit;
            upperLimit.SetLegacyKey(BuildKey(ToString(keys.second)));

            auto expected = TraverseNaively(root, false, false, lowerLimit, upperLimit);

            auto visitor = New<TTestChunkVisitor>();
            TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));

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
        return std::pair(lhs, rhs);
    };

    auto context = GetSyncChunkTraverserContext();

    for (int iter = 0; iter < 100; ++iter) {
        {
            auto chunkIndices = generateOrderedPair(statistics.ChunkCount);

            TLegacyReadLimit lowerLimit;
            lowerLimit.SetChunkIndex(chunkIndices.first);

            TLegacyReadLimit upperLimit;
            upperLimit.SetChunkIndex(chunkIndices.second);

            auto expected = TraverseNaively(root, false, false, lowerLimit, upperLimit);

            auto visitor = New<TTestChunkVisitor>();
            TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));

            EXPECT_EQ(expected, visitor->GetChunkInfos());
        }

        {
            auto keys = generateOrderedPair(5 * 5 * 5 * 2 * 10);

            TLegacyReadLimit lowerLimit;
            lowerLimit.SetLegacyKey(BuildKey(ToString(keys.first)));

            TLegacyReadLimit upperLimit;
            upperLimit.SetLegacyKey(BuildKey(ToString(keys.second)));

            auto expected = TraverseNaively(root, false, false, lowerLimit, upperLimit);

            auto visitor = New<TTestChunkVisitor>();
            TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, MakeComparator(1));

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
        return std::pair(lhs, rhs);
    };

    auto context = GetSyncChunkTraverserContext();

    for (int iter = 0; iter < 100; ++iter) {
        auto chunkIndices = generateOrderedPair(statistics.ChunkCount);

        TLegacyReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(chunkIndices.first);

        TLegacyReadLimit upperLimit;
        upperLimit.SetChunkIndex(chunkIndices.second);

        auto expected = TraverseNaively(root, true, true, lowerLimit, upperLimit);

        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);

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
    for (int tabletIndex = 0; tabletIndex < tabletCount; ++tabletIndex) {
        int rowCount = GetChunkTreeStatistics(root->Children()[tabletIndex]).LogicalRowCount;
        rowsPerTablet.emplace_back(rowCount);
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

        TLegacyReadLimit lowerLimit;
        TLegacyReadLimit upperLimit;

        lowerLimit.SetTabletIndex(leftTabletIndex);
        upperLimit.SetTabletIndex(rightTabletIndex);

        if (ForgetRowIndexProbability < dist(gen)) {
            lowerLimit.SetRowIndex(leftRowIndex);
            upperLimit.SetRowIndex(rightRowIndex);
        }

        if (ForgetReadLimitProbability > dist(gen)) {
            lowerLimit = TLegacyReadLimit{};
        }
        if (ForgetReadLimitProbability > dist(gen)) {
            upperLimit = TLegacyReadLimit{};
        }

        return std::pair(lowerLimit, upperLimit);
    };

    auto context = GetSyncChunkTraverserContext();

    for (auto iter = 0; iter < 5000; ++iter) {
        const auto [lowerLimit, upperLimit] = generateLimits();

        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(context, visitor, root, lowerLimit, upperLimit, {} /*keyColumnCount*/);
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
