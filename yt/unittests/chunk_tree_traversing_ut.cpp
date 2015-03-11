#include "stdafx.h"
#include "framework.h"

#include <server/chunk_server/chunk_tree_traversing.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk.h>
#include <server/chunk_server/helpers.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <core/misc/protobuf_helpers.h>

#include <core/actions/invoker_util.h>

#include <core/profiling/profile_manager.h>


namespace NYT {
namespace NChunkServer {
namespace {

using namespace NObjectClient;
using namespace NChunkClient::NProto;

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
       << ", LowerLimit_=(" << chunkInfo.LowerLimit.AsProto().DebugString() << ")"
       << ", UpperLimit_=(" << chunkInfo.UpperLimit.AsProto().DebugString() << ")"
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

    virtual void OnError(const TError& error) override
    {
        GTEST_FAIL();
    }

    virtual void OnFinish() override
    { }

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
        const_cast<TChunkTree**>(children.data()),
        const_cast<TChunkTree**>(children.data() + children.size()),
        [] (TChunkTree* /*chunk*/) { });
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
    i64 dataWeight)
{
    std::unique_ptr<TChunk> chunk(new TChunk(GenerateId(EObjectType::Chunk)));

    TChunkMeta chunkMeta;
    chunkMeta.set_type(static_cast<int>(EChunkType::Table)); // this makes chunk confirmed

    TMiscExt miscExt;
    miscExt.set_row_count(rowCount);
    miscExt.set_uncompressed_data_size(uncompressedDataSize);
    miscExt.set_compressed_data_size(compressedDataSize);
    miscExt.set_data_weight(dataWeight);
    SetProtoExtension<TMiscExt>(chunkMeta.mutable_extensions(), miscExt);

    NChunkClient::NProto::TChunkInfo chunkInfo;

    chunk->Confirm(&chunkInfo, &chunkMeta);

    return chunk;
}

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

    TChunkList listA(GenerateId(EObjectType::ChunkList));
    TChunkList listB(GenerateId(EObjectType::ChunkList));

    {
        std::vector<TChunkTree*> chunks;
        chunks.push_back(chunk2.get());
        chunks.push_back(chunk3.get());
        AttachToChunkList(&listB, chunks);
    }

    {
        std::vector<TChunkTree*> chunks;
        chunks.push_back(chunk1.get());
        chunks.push_back(&listB);
        AttachToChunkList(&listA, chunks);
    }

    auto callbacks = GetNonpreemptableChunkTraverserCallbacks();

    {
        auto visitor = New<TTestChunkVisitor>();
        TraverseChunkTree(callbacks, visitor, &listA);

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

        TraverseChunkTree(callbacks, visitor, &listA, lowerLimit, upperLimit);

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NChunkServer
} // namespace NYT
