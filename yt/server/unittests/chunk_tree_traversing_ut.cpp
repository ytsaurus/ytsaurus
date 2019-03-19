#include <yt/core/test_framework/framework.h>

#include "chunk_helpers.h"

#include <yt/server/master/chunk_server/chunk.h>
#include <yt/server/master/chunk_server/chunk_list.h>
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

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/yson/string.h>

namespace NYT::NChunkServer {

namespace {

using namespace NTesting;

using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TReadLimit;

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
            , LowerLimit(lowerLimit)
            , UpperLimit(upperLimit)
    { }

    TChunk* Chunk;
    i64 RowIndex;
    TReadLimit LowerLimit;
    TReadLimit UpperLimit;
};

bool operator < (const TChunkInfo& lhs, const TChunkInfo& rhs)
{
    return lhs.Chunk->GetId() < rhs.Chunk->GetId();
}

bool operator == (const TChunkInfo& lhs, const TChunkInfo& rhs)
{
    return lhs.Chunk->GetId() == rhs.Chunk->GetId()
        && lhs.RowIndex == rhs.RowIndex
        && lhs.LowerLimit == rhs.LowerLimit
        && lhs.UpperLimit == rhs.UpperLimit;
}

std::ostream& operator << (std::ostream& os, const TChunkInfo& chunkInfo)
{
    os << "ChunkInfo(Id=" << ToString(chunkInfo.Chunk->GetId())
       << ", RowIndex=" << chunkInfo.RowIndex
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
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit) override
    {
        ChunkInfos.insert(TChunkInfo(chunk, rowIndex, lowerLimit, upperLimit));
        return true;
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

class TChunkTreeTraversingTest
    : public TChunkGeneratorBase
{ };

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
