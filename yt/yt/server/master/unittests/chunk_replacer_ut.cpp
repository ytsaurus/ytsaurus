#include "chunk_helpers.h"
#include "helpers.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_tree_traverser.h>
#include <yt/yt/server/master/chunk_server/helpers.h>
#include <yt/yt/server/master/chunk_server/chunk_replacer.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkServer {
namespace {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient::NProto;
using namespace NTesting;

////////////////////////////////////////////////////////////////////////////////

class TTestChunkReplacerCallbacks
    : public IChunkReplacerCallbacks
{
public:
    explicit TTestChunkReplacerCallbacks(TChunkGeneratorBase* chunkGenerator)
        : ChunkGenerator_(chunkGenerator)
    { }

    void AttachToChunkList(
        TChunkList* chunkList,
        TRange<TChunkTree*> children) override
    {
        NChunkServer::AttachToChunkList(
            chunkList,
            children);
    }

    TChunkList* CreateChunkList(EChunkListKind /*kind*/) override
    {
        return ChunkGenerator_->CreateChunkList();
    }

    void RefObject(TObject* /*object*/) override
    { }

    void UnrefObject(TObject* /*object*/) override
    { }

private:
    TChunkGeneratorBase* const ChunkGenerator_;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkReplacerTest
    : public TChunkGeneratorBase
{ };

TEST_F(TChunkReplacerTest, Simple)
{
    //           rootList             //
    //      /     /     \     \       //
    // chunk1  chunk2  chunk3 chunk4  //

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    auto* chunk4 = CreateChunk(4, 4, 4, 4);

    auto* rootList = CreateChunkList();
    AttachToChunkList(rootList, {chunk1, chunk2, chunk3, chunk4});

    auto* newChunk = CreateChunk(5, 5, 5, 5);

    auto oldChunks = EnumerateChunksInChunkTree(rootList);
    {
        TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>(this));
        ASSERT_TRUE(replacer.FindChunkList(rootList, rootList->GetId()));

        std::vector ids{chunk2->GetId(), chunk3->GetId()};
        ASSERT_TRUE(replacer.ReplaceChunkSequence(newChunk, ids));

        auto* newList = replacer.Finish();
        ASSERT_TRUE(newList);
        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector({chunk1, newChunk, chunk4}));
    }

    {
        TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>(this));
        ASSERT_TRUE(replacer.FindChunkList(rootList, rootList->GetId()));

        std::vector ids{chunk1->GetId(), chunk2->GetId(), chunk3->GetId()};
        ASSERT_TRUE(replacer.ReplaceChunkSequence(newChunk, ids));

        auto* newList = replacer.Finish();
        ASSERT_TRUE(newList);
        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector({newChunk, chunk4}));
    }

    {
        TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>(this));
        ASSERT_TRUE(replacer.FindChunkList(rootList, rootList->GetId()));

        std::vector ids{chunk2->GetId(), chunk3->GetId(), chunk4->GetId()};
        ASSERT_TRUE(replacer.ReplaceChunkSequence(newChunk, ids));

        auto* newList = replacer.Finish();
        ASSERT_TRUE(newList);
        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector({chunk1, newChunk}));
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

TEST_F(TChunkReplacerTest, TwoReplaces)
{
    //               rootList                //
    //      /     /     \      \      \      //
    // chunk1  chunk2  chunk3  chunk4  chunk5 //

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    auto* chunk4 = CreateChunk(4, 4, 4, 4);
    auto* chunk5 = CreateChunk(5, 5, 5, 5);

    auto* rootList = CreateChunkList();
    AttachToChunkList(rootList, {chunk1, chunk2, chunk3, chunk4, chunk5});

    auto oldChunks = EnumerateChunksInChunkTree(rootList);
    {
        TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>(this));
        ASSERT_TRUE(replacer.FindChunkList(rootList, rootList->GetId()));

        auto* newChunk1 = CreateChunk(6, 6, 6, 6);
        std::vector ids1{chunk1->GetId(), chunk2->GetId()};
        ASSERT_TRUE(replacer.ReplaceChunkSequence(newChunk1, ids1));

        auto* newChunk2 = CreateChunk(7, 7, 7, 7);
        std::vector ids2{chunk4->GetId(), chunk5->GetId()};
        ASSERT_TRUE(replacer.ReplaceChunkSequence(newChunk2, ids2));

        auto* newList = replacer.Finish();
        ASSERT_TRUE(newList);
        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector({newChunk1, chunk3, newChunk2}));
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

TEST_F(TChunkReplacerTest, ReplaceContinuesFromLastPlace)
{
    //                   rootList                     //
    //      /     /     /       \      \      \       //
    // chunk1  chunk2  chunk3  chunk4  chunk1  chunk2 //

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    auto* chunk4 = CreateChunk(4, 4, 4, 4);

    auto* rootList = CreateChunkList();
    AttachToChunkList(rootList, {chunk1, chunk2, chunk3, chunk4, chunk1, chunk2});

    auto oldChunks = EnumerateChunksInChunkTree(rootList);
    {
        TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>(this));
        ASSERT_TRUE(replacer.FindChunkList(rootList, rootList->GetId()));

        auto* newChunk1 = CreateChunk(5, 5, 5, 5);
        std::vector ids1{chunk3->GetId(), chunk4->GetId()};
        ASSERT_TRUE(replacer.ReplaceChunkSequence(newChunk1, ids1));

        auto* newChunk2 = CreateChunk(6, 6, 6, 6);
        std::vector ids2{chunk1->GetId(), chunk2->GetId()};
        ASSERT_TRUE(replacer.ReplaceChunkSequence(newChunk2, ids2));

        auto* newList = replacer.Finish();
        ASSERT_TRUE(newList);
        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector({chunk1, chunk2, newChunk1, newChunk2}));
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

TEST_F(TChunkReplacerTest, DeepTree)
{
    //         rootList         //
    //            |             //
    //          list1           //
    //     /      |      \      //
    //  chunk1  chunk2  chunk3  //

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    auto* chunk3 = CreateChunk(3, 3, 3, 3);

    auto* list1 = CreateChunkList();
    AttachToChunkList(list1, {chunk1, chunk2, chunk3});

    auto* rootList = CreateChunkList();
    AttachToChunkList(rootList, {list1});

    auto oldChunks = EnumerateChunksInChunkTree(rootList);
    {
        TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>(this));
        ASSERT_TRUE(replacer.FindChunkList(rootList, list1->GetId()));

        auto* newChunk = CreateChunk(4, 4, 4, 4);
        std::vector ids{chunk1->GetId(), chunk2->GetId(), chunk3->GetId()};
        ASSERT_TRUE(replacer.ReplaceChunkSequence(newChunk, ids));

        auto* newList = replacer.Finish();
        ASSERT_TRUE(newList);
        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector({newChunk}));
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

TEST_F(TChunkReplacerTest, Subtrees)
{
    //                   rootList                //
    //             /                 \           //
    //          list1               list2        //
    //     /      |      \         /      \      //
    //  chunk1  chunk2  chunk3   chunk4  chunk5  //

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    auto* chunk4 = CreateChunk(4, 4, 4, 4);
    auto* chunk5 = CreateChunk(5, 5, 5, 5);

    auto* list1 = CreateChunkList();
    AttachToChunkList(list1, {chunk1, chunk2, chunk3});

    auto* list2 = CreateChunkList();
    AttachToChunkList(list2, {chunk4, chunk5});

    auto* rootList = CreateChunkList();
    AttachToChunkList(rootList, {list1, list2});

    auto oldChunks = EnumerateChunksInChunkTree(rootList);
    {
        TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>(this));
        ASSERT_TRUE(replacer.FindChunkList(rootList, list1->GetId()));

        auto* newChunk = CreateChunk(6, 6, 6, 6);
        std::vector ids{chunk2->GetId(), chunk3->GetId()};
        ASSERT_TRUE(replacer.ReplaceChunkSequence(newChunk, ids));

        auto* newList = replacer.Finish();
        ASSERT_TRUE(newList);
        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector({chunk1, newChunk, chunk4, chunk5}));
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

TEST_F(TChunkReplacerTest, SubtreesAndChunks1)
{
    //               rootList                //
    //      /     /     \      \      \      //
    // chunk1  chunk2  chunk3  chunk4  list1 //
    //                                   |   //
    //                                chunk5 //

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    auto* chunk4 = CreateChunk(4, 4, 4, 4);

    auto* list1 = CreateChunkList();
    auto* chunk5 = CreateChunk(5, 5, 5, 5);
    auto* rootList = CreateChunkList();

    AttachToChunkList(list1, {chunk5});
    AttachToChunkList(rootList, {chunk1, chunk2, chunk3, chunk4, list1});

    auto oldChunks = EnumerateChunksInChunkTree(rootList);
    {
        TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>(this));
        ASSERT_TRUE(replacer.FindChunkList(rootList, rootList->GetId()));

        auto* newChunk = CreateChunk(6, 6, 6, 6);
        std::vector ids{chunk2->GetId(), chunk3->GetId()};
        ASSERT_TRUE(replacer.ReplaceChunkSequence(newChunk, ids));

        auto* newList = replacer.Finish();
        ASSERT_TRUE(newList);
        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector({chunk1, newChunk, chunk4, chunk5}));
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

TEST_F(TChunkReplacerTest, SubtreesAndChunks2)
{
    //               rootList              //
    //      /     /     \      \           //
    // chunk1  chunk2  chunk3  list1       //
    //                        /     \      //
    //                    chunk4    chunk5 //

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    auto* chunk3 = CreateChunk(3, 3, 3, 3);

    auto* list1 = CreateChunkList();
    auto* chunk4 = CreateChunk(4, 4, 4, 4);
    auto* chunk5 = CreateChunk(5, 5, 5, 5);
    auto* rootList = CreateChunkList();

    AttachToChunkList(list1, {chunk4, chunk5});
    AttachToChunkList(rootList, {chunk1, chunk2, chunk3, list1});

    auto oldChunks = EnumerateChunksInChunkTree(rootList);
    {
        TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>(this));
        ASSERT_TRUE(replacer.FindChunkList(rootList, list1->GetId()));

        auto* newChunk = CreateChunk(6, 6, 6, 6);
        std::vector ids{chunk4->GetId(), chunk5->GetId()};
        ASSERT_TRUE(replacer.ReplaceChunkSequence(newChunk, ids));

        auto* newList = replacer.Finish();
        ASSERT_TRUE(newList);
        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector({chunk1, chunk2, chunk3, newChunk}));
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

TEST_F(TChunkReplacerTest, Fails)
{
    //           rootList             //
    //      /     /     \     \       //
    // chunk1  chunk2  chunk3 chunk4  //

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    auto* chunk4 = CreateChunk(4, 4, 4, 4);

    auto* rootList = CreateChunkList();
    AttachToChunkList(rootList, {chunk1, chunk2, chunk3, chunk4});

    auto* newChunk = CreateChunk(5, 5, 5, 5);

    auto oldChunks = EnumerateChunksInChunkTree(rootList);
    {
        TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>(this));
        ASSERT_FALSE(replacer.FindChunkList(rootList, chunk1->GetId()));

        std::vector ids{chunk1->GetId()};
        ASSERT_FALSE(replacer.ReplaceChunkSequence(newChunk, ids));

        auto* newList = replacer.Finish();
        ASSERT_FALSE(newList);
    }

    {
        TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>(this));
        ASSERT_TRUE(replacer.FindChunkList(rootList, rootList->GetId()));

        std::vector ids{chunk3->GetId(), chunk2->GetId()};
        ASSERT_FALSE(replacer.ReplaceChunkSequence(newChunk, ids));

        auto* newList = replacer.Finish();
        ASSERT_TRUE(newList);
        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, oldChunks);
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
