#include "helpers.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/master/chunk_server/config.h>
#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_tree_balancer.h>
#include <yt/yt/server/master/chunk_server/helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkServer {
namespace {

using namespace NObjectClient;
using namespace NChunkClient::NProto;

using NChunkClient::TLegacyReadLimit;

////////////////////////////////////////////////////////////////////////////////

void AttachToChunkListAndRef(
    TChunkList* chunkList,
    TRange<TChunkTree*> children)
{
    NChunkServer::AttachToChunkList(
        chunkList,
        children);
    for (auto* child : children) {
        child->RefObject();
    }
}

std::unique_ptr<TChunk> CreateChunk()
{
    auto chunk = TPoolAllocator::New<TChunk>(GenerateChunkId());

    TChunkMeta chunkMeta;
    chunkMeta.set_type(static_cast<int>(EChunkType::Table));

    TMiscExt miscExt;
    SetProtoExtension<TMiscExt>(chunkMeta.mutable_extensions(), miscExt);

    NChunkClient::NProto::TChunkInfo chunkInfo;

    chunk->Confirm(chunkInfo, chunkMeta);

    return chunk;
}

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeBalancerCallbacksMock
    : public IChunkTreeBalancerCallbacks
    , public TBootstrapMock
{
public:
    explicit TChunkTreeBalancerCallbacksMock(std::vector<std::unique_ptr<TChunkList>>* chunkLists)
        : Config_(New<TDynamicChunkTreeBalancerConfig>())
        , ChunkLists_(chunkLists)
    {
        SetupMasterSmartpointers();
    }

    ~TChunkTreeBalancerCallbacksMock()
    {
        ResetMasterSmartpointers();
    }

    const TDynamicChunkTreeBalancerConfigPtr& GetConfig() const override
    {
        return Config_;
    }

    void RefObject(NObjectServer::TObject* object) override
    {
        object->RefObject();
    }

    void UnrefObject(NObjectServer::TObject* object) override
    {
        object->UnrefObject();
    }

    void FlushObjectUnrefs() override
    { }

    int GetObjectRefCounter(NObjectServer::TObject* object) override
    {
        return object->GetObjectRefCounter();
    }

    void ScheduleRequisitionUpdate(TChunkTree* /*chunkTree*/) override
    { }

    TChunkList* CreateChunkList() override
    {
        auto chunkList = TPoolAllocator::New<TChunkList>(GenerateChunkListId());
        ChunkLists_->push_back(std::move(chunkList));
        return ChunkLists_->back().get();
    }

    void ClearChunkList(TChunkList* chunkList) override
    {
        for (auto* child : chunkList->Children()) {
            ResetChunkTreeParent(chunkList, child);
            UnrefObject(child);
        }
        chunkList->Children().clear();
        ResetChunkListStatistics(chunkList);
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        TRange<TChunkTree*> children) override
    {
        NChunkServer::AttachToChunkList(
            chunkList,
            children);
    }

private:
    const TDynamicChunkTreeBalancerConfigPtr Config_;
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
        AttachToChunkListAndRef(chunkListChain[i], {chunkListChain[i + 1]});
    }
    AttachToChunkListAndRef(chunkListChain.back(), {chunk.get()});

    auto root = chunkListChain.front();
    bootstrap->RefObject(root);

    TChunkTreeBalancer balancer(bootstrap);

    EXPECT_EQ(ChainSize, root->Statistics().ChunkListCount);
    ASSERT_TRUE(balancer.IsRebalanceNeeded(root, EChunkTreeBalancerMode::Strict));
    ASSERT_TRUE(balancer.IsRebalanceNeeded(root, EChunkTreeBalancerMode::Permissive));
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
        chunkStorage.push_back(CreateChunk());
        return chunkStorage.back().get();
    };

    std::vector<TChunkTree*> chunkLists;
    auto root = bootstrap->CreateChunkList();
    bootstrap->RefObject(root);
    for (int i = 0; i < ChunkListCount; ++i) {
        auto chunkList = bootstrap->CreateChunkList();
        AttachToChunkListAndRef(chunkList, {createChunk()});
        chunkLists.push_back(chunkList);
    }
    AttachToChunkListAndRef(root, chunkLists);

    TChunkTreeBalancer balancer(bootstrap);

    EXPECT_EQ(ChunkListCount + 1, root->Statistics().ChunkListCount);
    ASSERT_TRUE(balancer.IsRebalanceNeeded(root, EChunkTreeBalancerMode::Strict));
    ASSERT_TRUE(balancer.IsRebalanceNeeded(root, EChunkTreeBalancerMode::Permissive));
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
        AttachToChunkListAndRef(chunkList, {bootstrap->CreateChunkList()});
        chunkLists.push_back(chunkList);
    }
    AttachToChunkListAndRef(root, chunkLists);

    TChunkTreeBalancer balancer(bootstrap);

    EXPECT_EQ(2 * ChunkListCount + 1, root->Statistics().ChunkListCount);
    ASSERT_TRUE(balancer.IsRebalanceNeeded(root, EChunkTreeBalancerMode::Strict));
    ASSERT_TRUE(balancer.IsRebalanceNeeded(root, EChunkTreeBalancerMode::Permissive));
    balancer.Rebalance(root);
    EXPECT_EQ(1, root->Statistics().ChunkListCount);
}

TEST(ChunkTreeBalancer, PermissiveMode)
{
    // If Permissive or Strict mode values were changed these parameters might need to be changed as well.
    constexpr int ChunkListCount = 2;
    constexpr int ChunkCount = 35;

    std::vector<std::unique_ptr<TChunk>> chunkStorage;
    std::vector<std::unique_ptr<TChunkList>> chunkListStorage;
    auto bootstrap = New<TChunkTreeBalancerCallbacksMock>(&chunkListStorage);
    auto createChunk = [&] () -> TChunk* {
        chunkStorage.push_back(CreateChunk());
        return chunkStorage.back().get();
    };

    std::vector<TChunkTree*> chunkLists;
    auto root = bootstrap->CreateChunkList();
    bootstrap->RefObject(root);
    for (int i = 0; i < ChunkListCount; ++i) {
        auto subRoot = bootstrap->CreateChunkList();
        for (int j = 0; j < ChunkCount; ++j) {
            AttachToChunkListAndRef(subRoot, {createChunk()});
        }
        chunkLists.push_back(subRoot);
    }
    AttachToChunkListAndRef(root, chunkLists);

    TChunkTreeBalancer balancer(bootstrap);

    EXPECT_EQ(ChunkCount * 2, root->Statistics().ChunkCount);
    EXPECT_EQ(ChunkListCount + 1, root->Statistics().ChunkListCount);
    ASSERT_TRUE(balancer.IsRebalanceNeeded(root, EChunkTreeBalancerMode::Strict));
    ASSERT_FALSE(balancer.IsRebalanceNeeded(root, EChunkTreeBalancerMode::Permissive));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
