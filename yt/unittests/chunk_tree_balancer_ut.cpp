#include "stdafx.h"
#include "framework.h"

#include <server/chunk_server/chunk_tree_balancer.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk.h>
#include <server/chunk_server/helpers.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/object_client/helpers.h>

#include <core/misc/protobuf_helpers.h>


namespace NYT {
namespace NChunkServer {
namespace {

using namespace NObjectClient;
using namespace NChunkClient::NProto;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

void AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children)
{
    NChunkServer::AttachToChunkList(
        chunkList,
        const_cast<TChunkTree**>(children.data()),
        const_cast<TChunkTree**>(children.data() + children.size()),
        [] (TChunkTree* tree) { tree->RefObject(); });
}

TGuid GenerateId(EObjectType type)
{
    static i64 counter = 0;
    return MakeId(type, 0, counter++, 0);
}

TGuid GenerateChunkId()
{
    return GenerateId(EObjectType::Chunk);
}

TGuid GenerateChunkListId()
{
    return GenerateId(EObjectType::ChunkList);
}

std::unique_ptr<TChunk> CreateChunk()
{
    std::unique_ptr<TChunk> chunk(new TChunk(GenerateChunkId()));

    TChunkMeta chunkMeta;
    chunkMeta.set_type(EChunkType::Table);

    TMiscExt miscExt;
    SetProtoExtension<TMiscExt>(chunkMeta.mutable_extensions(), miscExt);

    NChunkClient::NProto::TChunkInfo chunkInfo;

    chunk->Confirm(&chunkInfo, &chunkMeta);

    return chunk;
}

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeBalancerCallbacksMock
    : public IChunkTreeBalancerCallbacks
{
public:
    explicit TChunkTreeBalancerCallbacksMock(std::vector<std::unique_ptr<TChunkList>>* chunkLists)
        : ChunkLists_(chunkLists)
    { }

    virtual void RefObject(NObjectServer::TObjectBase* object) override
    {
        object->RefObject();
    }

    virtual void UnrefObject(NObjectServer::TObjectBase* object) override
    {
        object->UnrefObject();
    }

    virtual TChunkList* CreateChunkList() override
    {
        auto chunkList = std::unique_ptr<TChunkList>(new TChunkList(GenerateChunkListId()));
        ChunkLists_->push_back(std::move(chunkList));
        return ChunkLists_->back().get();
    }

    virtual void ClearChunkList(TChunkList* chunkList) override
    {
        for (auto* child : chunkList->Children()) {
            ResetChunkTreeParent(chunkList, child);
            UnrefObject(child);
        }
        chunkList->Children().clear();
        ResetChunkListStatistics(chunkList);
    }

    virtual void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children) override
    {
        NChunkServer::AttachToChunkList(
            chunkList,
            const_cast<TChunkTree**>(children.data()),
            const_cast<TChunkTree**>(children.data() + children.size()),
            [] (TChunkTree* /*chunk*/) { });
    }

    virtual void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child) override
    {
        NChunkServer::AttachToChunkList(
            chunkList,
            &child,
            &child + 1,
            [] (TChunkTree* /*chunk*/) { });
    }

    virtual void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree** childrenBegin,
        TChunkTree** childrenEnd) override
    {
        NChunkServer::AttachToChunkList(
            chunkList,
            childrenBegin,
            childrenEnd,
            [] (TChunkTree* /*chunk*/) { });
    }

private:
    std::vector<std::unique_ptr<TChunkList>>* ChunkLists_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(ChunkTreeBalancer, Chain)
{
    const int ChainSize = 5;

    std::vector<std::unique_ptr<TChunkList>> chunkListStorage;
    auto bootstrap = New<TChunkTreeBalancerCallbacksMock>(&chunkListStorage);

    auto chunk = CreateChunk();

    std::vector<TChunkList*> chunkListChain;
    for (int i = 0; i < ChainSize; ++i) {
        chunkListChain.push_back(bootstrap->CreateChunkList());
    }
    for (int i = 0; i + 1 < ChainSize; ++i) {
        AttachToChunkList(chunkListChain[i], {chunkListChain[i + 1]});
    }
    AttachToChunkList(chunkListChain.back(), {chunk.get()});

    auto root = chunkListChain.front();
    bootstrap->RefObject(root);

    TChunkTreeBalancer balancer(bootstrap);

    EXPECT_EQ(ChainSize, root->Statistics().ChunkListCount);
    ASSERT_TRUE(balancer.IsRebalanceNeeded(root));
    balancer.Rebalance(root);
    EXPECT_EQ(2, root->Statistics().ChunkListCount);
}

TEST(ChunkTreeBalancer, ManyChunkLists)
{
    const int ChunkListCount = 5;

    std::vector<std::unique_ptr<TChunk>> chunkStorage;
    std::vector<std::unique_ptr<TChunkList>> chunkListStorage;
    auto bootstrap = New<TChunkTreeBalancerCallbacksMock>(&chunkListStorage);
    auto createChunk = [&] () -> TChunk* {
        chunkStorage.push_back(std::move(CreateChunk()));
        return chunkStorage.back().get();
    };

    std::vector<TChunkTree*> chunkLists;
    auto root = bootstrap->CreateChunkList();
    bootstrap->RefObject(root);
    for (int i = 0; i < ChunkListCount; ++i) {
        auto chunkList = bootstrap->CreateChunkList();
        AttachToChunkList(chunkList, {createChunk()});
        chunkLists.push_back(chunkList);
    }
    AttachToChunkList(root, chunkLists);

    TChunkTreeBalancer balancer(bootstrap);

    EXPECT_EQ(ChunkListCount + 1, root->Statistics().ChunkListCount);
    ASSERT_TRUE(balancer.IsRebalanceNeeded(root));
    balancer.Rebalance(root);
    EXPECT_EQ(2, root->Statistics().ChunkListCount);
}

TEST(ChunkTreeBalancer, EmptyChunkLists)
{
    const int ChunkListCount = 5;

    std::vector<std::unique_ptr<TChunkList>> chunkListStorage;
    auto bootstrap = New<TChunkTreeBalancerCallbacksMock>(&chunkListStorage);

    std::vector<TChunkTree*> chunkLists;
    auto root = bootstrap->CreateChunkList();
    bootstrap->RefObject(root);
    for (int i = 0; i < ChunkListCount; ++i) {
        auto chunkList = bootstrap->CreateChunkList();
        AttachToChunkList(chunkList, {bootstrap->CreateChunkList()});
        chunkLists.push_back(chunkList);
    }
    AttachToChunkList(root, chunkLists);

    TChunkTreeBalancer balancer(bootstrap);

    EXPECT_EQ(2 * ChunkListCount + 1, root->Statistics().ChunkListCount);
    ASSERT_TRUE(balancer.IsRebalanceNeeded(root));
    balancer.Rebalance(root);
    EXPECT_EQ(1, root->Statistics().ChunkListCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NChunkServer
} // namespace NYT
