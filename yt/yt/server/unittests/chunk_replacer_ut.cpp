#include "chunk_helpers.h"

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
using namespace NChunkClient::NProto;
using namespace NTesting;

////////////////////////////////////////////////////////////////////////////////

class TTestChunkReplacerCallbacks
    : public IChunkReplacerCallbacks
{
public:
    virtual void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children) override
    {
        NChunkServer::AttachToChunkList(
            chunkList,
            children.data(),
            children.data() + children.size());
    }

    virtual void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child) override
    {
        NChunkServer::AttachToChunkList(
            chunkList,
            &child,
            &child + 1);
    }

    virtual void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd) override
    {
        NChunkServer::AttachToChunkList(
            chunkList,
            childrenBegin,
            childrenEnd);
    }

    virtual bool IsMutationLoggingEnabled() override
    {
        return false;
    }
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

    TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>());
    auto oldChunks = EnumerateChunksInChunkTree(rootList);

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk2->GetId(), chunk3->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({chunk1, newChunk, chunk4}));
    }

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk1->GetId(), chunk2->GetId(), chunk3->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({newChunk, chunk4}));
    }

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk2->GetId(), chunk3->GetId(), chunk4->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({chunk1, newChunk}));
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

TEST_F(TChunkReplacerTest, Chain)
{
    //               rootList      //
    //               /      \      //
    //            list2   chunk4   //
    //          /      \           //
    //       list1   chunk3        //
    //     /      \                //
    //  chunk1  chunk2             //

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    auto* chunk4 = CreateChunk(4, 4, 4, 4);

    auto* list1 = CreateChunkList();
    AttachToChunkList(list1, {chunk1, chunk2});
    
    auto* list2 = CreateChunkList();
    AttachToChunkList(list2, {list1, chunk3});

    auto* rootList = CreateChunkList();
    AttachToChunkList(rootList, {list2, chunk4});

    auto* newChunk = CreateChunk(5, 5, 5, 5);

    TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>());
    auto oldChunks = EnumerateChunksInChunkTree(rootList);

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk2->GetId(), chunk3->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({chunk1, newChunk, chunk4}));
    }

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk1->GetId(), chunk2->GetId(), chunk3->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({newChunk, chunk4}));
    }

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk2->GetId(), chunk3->GetId(), chunk4->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({chunk1, newChunk}));
    }

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk1->GetId(), chunk2->GetId(), chunk3->GetId(), chunk4->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({newChunk}));
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

    auto* newChunk = CreateChunk(6, 6, 6, 6);

    TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>());
    auto oldChunks = EnumerateChunksInChunkTree(rootList);

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk2->GetId(), chunk3->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({chunk1, newChunk, chunk4, chunk5}));
    }

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk2->GetId(), chunk3->GetId(), chunk4->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({chunk1, newChunk, chunk5}));
    }

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk1->GetId(), chunk2->GetId(), chunk3->GetId(), chunk4->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({newChunk, chunk5}));
    }

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk2->GetId(), chunk3->GetId(), chunk4->GetId(), chunk5->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({chunk1, newChunk}));
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

TEST_F(TChunkReplacerTest, DeepTree)
{
    //         root            //
    //      /    |    \        //
    //     /     |     \       //
    // chunk1  chunk2  chunk3  //

    auto chunk1 = CreateChunk(1, 1, 1, 1);
    auto chunk2 = CreateChunk(1, 2, 2, 2);
    auto chunk3 = CreateChunk(1, 3, 3, 3);

    auto rootList = CreateChunkList(EChunkListKind::Static);
    AttachToChunkList(rootList, std::vector<TChunkTree*>{chunk1, chunk2, chunk3});

    auto* newChunk = CreateChunk(6, 6, 6, 6);

    TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>());
    auto oldChunks = EnumerateChunksInChunkTree(rootList);

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk2->GetId(), chunk3->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({chunk1, newChunk}));
    }

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk1->GetId(), chunk2->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({newChunk, chunk3}));
    }

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk1->GetId(), chunk2->GetId(), chunk3->GetId()});
        ASSERT_TRUE(replacer.Replace(rootList, newList, newChunk, ids));

        auto chunks = EnumerateChunksInChunkTree(newList);
        EXPECT_EQ(chunks, std::vector<TChunk*>({newChunk}));
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

TEST_F(TChunkReplacerTest, FailUnexpected)
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

    TChunkReplacer replacer(New<TTestChunkReplacerCallbacks>());
    auto oldChunks = EnumerateChunksInChunkTree(rootList);

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk2->GetId(), chunk4->GetId()});
        ASSERT_FALSE(replacer.Replace(rootList, newList, newChunk, ids));
    }

    {
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk3->GetId(), chunk4->GetId(), chunk4->GetId()});
        ASSERT_FALSE(replacer.Replace(rootList, newList, newChunk, ids));
    }

    {
        auto* chunk5 = CreateChunk(5, 5, 5, 5);
        auto* newList = CreateChunkList();
        std::vector<TChunkId> ids({chunk5->GetId()});
        ASSERT_FALSE(replacer.Replace(rootList, newList, newChunk, ids));
    }

    EXPECT_EQ(oldChunks, EnumerateChunksInChunkTree(rootList));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
