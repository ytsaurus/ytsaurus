#include <yt/core/test_framework/framework.h>

#include <yt/server/chunk_server/chunk.h>
#include <yt/server/chunk_server/chunk_list.h>
#include <yt/server/chunk_server/chunk_tree_traverser.h>
#include <yt/server/chunk_server/helpers.h>

#include <yt/ytlib/chunk_client/chunk_meta.pb.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/helpers.h>

#include <yt/core/actions/invoker_util.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NChunkServer {
namespace {

using namespace NChunkClient::NProto;
using namespace NObjectClient;
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

void AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children)
{
    NChunkServer::AttachToChunkList(
        chunkList,
        children.data(),
        children.data() + children.size());
}

TGuid GenerateId(EObjectType type)
{
    static i64 counter = 0;
    return MakeId(type, 0, counter++, 0);
}

std::unique_ptr<TChunk> CreateChunk(
    i64 rowCount,
    i64 compressedDataSize,
    i64 uncompressedDataSize,
    i64 dataWeight,
    TOwningKey minKey = TOwningKey(),
    TOwningKey maxKey = TOwningKey())
{
    auto chunk = std::make_unique<TChunk>(GenerateId(EObjectType::Chunk));
    chunk->RefObject();

    TChunkMeta chunkMeta;
    chunkMeta.set_type(static_cast<int>(EChunkType::Table));

    TMiscExt miscExt;
    miscExt.set_row_count(rowCount);
    miscExt.set_uncompressed_data_size(uncompressedDataSize);
    miscExt.set_compressed_data_size(compressedDataSize);
    miscExt.set_data_weight(dataWeight);
    SetProtoExtension(chunkMeta.mutable_extensions(), miscExt);

    TBoundaryKeysExt boundaryKeysExt;
    ToProto(boundaryKeysExt.mutable_min(), minKey);
    ToProto(boundaryKeysExt.mutable_max(), maxKey);
    SetProtoExtension(chunkMeta.mutable_extensions(), boundaryKeysExt);

    NChunkClient::NProto::TChunkInfo chunkInfo;

    chunk->Confirm(&chunkInfo, &chunkMeta);

    return chunk;
}

std::unique_ptr<TChunkList> CreateChunkList(EChunkListKind kind = EChunkListKind::Static)
{
    auto chunkList = std::make_unique<TChunkList>(GenerateId(EObjectType::ChunkList));
    chunkList->SetKind(kind);
    chunkList->RefObject();
    return chunkList;
}

TUnversionedOwningRow BuildKey(const TString& yson)
{
    return NTableClient::YsonToKey(yson);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TraverseChunkTree, Simple)
{
    //     listA           //
    //    /     \          //
    // chunk1   listB      //
    //         /     \     //
    //     chunk2   chunk3 //

    auto chunk1 = CreateChunk(1, 1, 1, 1);
    auto chunk2 = CreateChunk(2, 2, 2, 2);
    auto chunk3 = CreateChunk(3, 3, 3, 3);

    auto listA = CreateChunkList();
    auto listB = CreateChunkList();

    {
        std::vector<TChunkTree*> items;
        items.push_back(chunk2.get());
        items.push_back(chunk3.get());
        AttachToChunkList(listB.get(), items);
    }

    {
        std::vector<TChunkTree*> items;
        items.push_back(chunk1.get());
        items.push_back(listB.get());
        AttachToChunkList(listA.get(), items);
    }

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, listA.get());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1.get(),
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2.get(),
            1,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk3.get(),
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

        TraverseChunkTree(callbacks, visitor, listA.get(), lowerLimit, upperLimit);

        TReadLimit correctLowerLimit;
        correctLowerLimit.SetRowIndex(1);

        TReadLimit correctUpperLimit;
        correctUpperLimit.SetRowIndex(2);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk2.get(),
            1,
            correctLowerLimit,
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk3.get(),
            3,
            TReadLimit(),
            correctUpperLimit));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST(TraverseChunkTree, WithEmptyChunkLists)
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
        items.push_back(empty1.get());
        items.push_back(chunk1.get());
        items.push_back(empty2.get());
        items.push_back(chunk2.get());
        items.push_back(empty3.get());
        AttachToChunkList(list.get(), items);
    }

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, list.get());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1.get(),
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2.get(),
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

        TraverseChunkTree(callbacks, visitor, list.get(), lowerLimit, upperLimit);

        TReadLimit correctLowerLimit;

        TReadLimit correctUpperLimit;
        correctUpperLimit.SetRowIndex(1);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk2.get(),
            1,
            correctLowerLimit,
            correctUpperLimit));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST(TraverseChunkTree, SortedDynamic)
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

    AttachToChunkList(tablet1.get(), std::vector<TChunkTree*>{chunk1.get()});
    AttachToChunkList(tablet2.get(), std::vector<TChunkTree*>{chunk2.get(), chunk3.get()});
    AttachToChunkList(root.get(), std::vector<TChunkTree*>{tablet1.get(), tablet2.get()});

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, root.get());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1.get(),
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2.get(),
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk3.get(),
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

        TraverseChunkTree(callbacks, visitor, root.get(), lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1.get(),
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2.get(),
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

        TraverseChunkTree(callbacks, visitor, root.get(), lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk3.get(),
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

        TraverseChunkTree(callbacks, visitor, root.get(), lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1.get(),
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2.get(),
            0,
            TReadLimit(),
            upperLimit));
        correctResult.insert(TChunkInfo(
            chunk3.get(),
            0,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

TEST(TraverseChunkTree, SortedDynamicChunkShared)
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

    AttachToChunkList(tablet1.get(), std::vector<TChunkTree*>{chunk.get()});
    AttachToChunkList(tablet2.get(), std::vector<TChunkTree*>{chunk.get()});
    AttachToChunkList(tablet3.get(), std::vector<TChunkTree*>{chunk.get()});
    AttachToChunkList(root.get(), std::vector<TChunkTree*>{tablet1.get(), tablet2.get(), tablet3.get()});

    TReadLimit limit2;
    limit2.SetKey(BuildKey("2"));

    TReadLimit limit4;
    limit4.SetKey(BuildKey("4"));

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, root.get());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk.get(),
            0,
            TReadLimit(),
            limit2));
        correctResult.insert(TChunkInfo(
            chunk.get(),
            0,
            limit2,
            limit4));
        correctResult.insert(TChunkInfo(
            chunk.get(),
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

        TraverseChunkTree(callbacks, visitor, root.get(), lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk.get(),
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

        TraverseChunkTree(callbacks, visitor, root.get(), lowerLimit, upperLimit);

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk.get(),
            0,
            lowerLimit,
            limit2));
        correctResult.insert(TChunkInfo(
            chunk.get(),
            0,
            limit2,
            limit4));
        correctResult.insert(TChunkInfo(
            chunk.get(),
            0,
            limit4,
            upperLimit));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}


TEST(TraverseChunkTree, OrderedDynamic)
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

    AttachToChunkList(tablet1.get(), std::vector<TChunkTree*>{chunk1.get()});
    AttachToChunkList(tablet2.get(), std::vector<TChunkTree*>{chunk2.get(), chunk3.get()});
    AttachToChunkList(root.get(), std::vector<TChunkTree*>{tablet1.get(), tablet2.get()});

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, root.get());

        std::set<TChunkInfo> correctResult;
        correctResult.insert(TChunkInfo(
            chunk1.get(),
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk2.get(),
            0,
            TReadLimit(),
            TReadLimit()));
        correctResult.insert(TChunkInfo(
            chunk3.get(),
            0,
            TReadLimit(),
            TReadLimit()));

        EXPECT_EQ(correctResult, visitor->GetChunkInfos());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NChunkServer
} // namespace NYT
