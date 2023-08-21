#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/p2p.h>

namespace NYT::NDataNode {
namespace {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

class TP2PTest
    : public ::testing::Test
{
public:
    TActionQueuePtr ActionQueue = New<TActionQueue>("P2PTest");

    TP2PConfigPtr Config;
    TP2PBlockCachePtr Cache;
    TP2PSnooperPtr Snooper;

    TP2PTest()
    {
        Config = New<TP2PConfig>();
        Config->Enabled = true;

        Cache = New<TP2PBlockCache>(
            Config,
            ActionQueue->GetInvoker(),
            GetNullMemoryUsageTracker());
        Snooper = New<TP2PSnooper>(Config);
    }

    ~TP2PTest()
    {
        ActionQueue->Shutdown();
    }
};

TEST_F(TP2PTest, LookupBlocks)
{
    auto chunk0 = TChunkId::Create();
    auto block3 = TBlock{TSharedRef::FromString("hello")};

    auto blocks = Cache->LookupBlocks(chunk0, {2, 3});
    ASSERT_EQ(2u, blocks.size());
    ASSERT_FALSE(blocks[0]);
    ASSERT_FALSE(blocks[1]);

    Cache->HoldBlocks(chunk0, {3}, {block3});

    blocks = Cache->LookupBlocks(chunk0, {2, 3});
    ASSERT_EQ(2u, blocks.size());
    ASSERT_FALSE(blocks[0]);
    ASSERT_EQ(blocks[1].Data.ToVector(), block3.Data.ToVector());
}

TEST_F(TP2PTest, SnoopNoNodes)
{
    auto chunk0 = TChunkId::Create();
    auto block0 = TBlock{TSharedRef::FromString("b0")};
    std::vector<TBlock> blocks{block0};

    for (int i = 0; i <= Config->HotBlockThreshold; i++) {
        ASSERT_TRUE(Snooper->OnBlockRead(chunk0, {0}, &blocks).empty());
    }
}

TEST_F(TP2PTest, LargeBlockRead)
{
    std::vector<TNodeId> peers = {42, 43, 44, 45, 46};
    Snooper->SetEligiblePeers(peers);

    auto chunk0 = TChunkId::Create();

    auto bigBlock = TBlock{TSharedRef::FromString(TString(Config->MaxBlockSize + 100, 'x'))};
    std::vector<TBlock> blocks{bigBlock};

    for (int i = 0; i < Config->HotBlockThreshold; i++) {
        ASSERT_TRUE(Snooper->OnBlockRead(chunk0, {0}, &blocks).empty());
        ASSERT_TRUE(blocks[0]);
    }

    auto suggestions = Snooper->OnBlockRead(chunk0, {0}, &blocks);
    ASSERT_TRUE(suggestions.empty());
    ASSERT_FALSE(blocks[0]);
}

TEST_F(TP2PTest, MaxBytesPerNode)
{
    std::vector<TNodeId> peers = {42, 43, 44, 45, 46};
    Snooper->SetEligiblePeers(peers);

    auto chunk0 = TChunkId::Create();

    auto bigBlock = TBlock{TSharedRef::FromString(TString(Config->MaxDistributedBytes / 2 + 100, 'x'))};

    std::vector<TBlock> blocks{bigBlock};
    for (int i = 0; i < Config->HotBlockThreshold; i++) {
        ASSERT_TRUE(Snooper->OnBlockRead(chunk0, {0}, &blocks).empty());
    }

    auto suggestions = Snooper->OnBlockRead(chunk0, {0}, &blocks);
    ASSERT_FALSE(suggestions.empty());

    peers = {142, 143, 144, 145, 146};
    Snooper->SetEligiblePeers(peers);

    blocks = {bigBlock};
    for (int i = 0; i < Config->SecondHotBlockThreshold; i++) {
        ASSERT_TRUE(Snooper->OnBlockRead(chunk0, {1}, &blocks).empty());
    }

    auto secondSuggestions = Snooper->OnBlockRead(chunk0, {0}, &blocks);
    ASSERT_FALSE(secondSuggestions.empty());
    ASSERT_EQ(secondSuggestions[0].Peers, suggestions[0].Peers);

    blocks = {bigBlock};
    for (int i = 0; i < Config->SecondHotBlockThreshold; i++) {
        ASSERT_TRUE(Snooper->OnBlockRead(chunk0, {2}, &blocks).empty());
    }
    auto differentSuggestions = Snooper->OnBlockRead(chunk0, {2}, &blocks);
    ASSERT_FALSE(differentSuggestions.empty());
    ASSERT_NE(differentSuggestions[0].Peers, suggestions[0].Peers);
}

TEST_F(TP2PTest, BlockRedistribution)
{
    std::vector<TNodeId> peers = {42, 43, 44, 45, 46};
    Snooper->SetEligiblePeers(peers);

    auto chunk0 = TChunkId::Create();
    auto block2 = TBlock{TSharedRef::FromString("b2")};

    std::vector<TBlock> blocks{block2};
    for (int i = 0; i < Config->HotBlockThreshold; i++) {
        ASSERT_TRUE(Snooper->OnBlockRead(chunk0, {2}, &blocks).empty());
    }
    auto suggestions = Snooper->OnBlockRead(chunk0, {2}, &blocks);
    ASSERT_FALSE(suggestions.empty());

    peers = {142, 143, 144, 145, 146};
    Snooper->SetEligiblePeers(peers);

    for (int i = 0; i < Config->BlockRedistributionTicks; i++) {
        i64 tick;
        std::vector<TP2PSnooper::TQueuedBlock> queued;
        Snooper->FinishTick(&tick, &queued);
    }

    blocks = {block2};
    for (int i = 0; i < Config->SecondHotBlockThreshold; i++) {
        ASSERT_TRUE(Snooper->OnBlockRead(chunk0, {2}, &blocks).empty());
    }
    auto newSuggestions = Snooper->OnBlockRead(chunk0, {2}, &blocks);
    ASSERT_FALSE(newSuggestions.empty());
    ASSERT_NE(newSuggestions[0].Peers, suggestions[0].Peers);
}

TEST_F(TP2PTest, Snoop)
{
    auto chunk0 = TChunkId::Create();

    auto block2 = TBlock{TSharedRef::FromString("b2")};
    auto block3 = TBlock{TSharedRef::FromString("b3")};
    auto block4 = TBlock{TSharedRef::FromString("b4")};

    std::vector<TNodeId> peers = {42, 43, 44, 45, 46};
    Snooper->SetEligiblePeers(peers);

    std::vector<TBlock> blocks{block2, block3};
    for (int i = 0; i < Config->HotBlockThreshold; i++) {
        ASSERT_TRUE(Snooper->OnBlockRead(chunk0, {2, 3}, &blocks).empty());
    }

    auto suggestions = Snooper->OnBlockRead(chunk0, {2, 3}, &blocks);
    ASSERT_FALSE(suggestions.empty());
    ASSERT_EQ(1u, Snooper->GetHotChunks().size());
    for (auto s : suggestions) {
        ASSERT_EQ(1u, s.P2PIteration);
        ASSERT_TRUE(s.BlockIndex == 2 || s.BlockIndex == 3);
        ASSERT_THAT(s.Peers, IsSubsetOf(peers));
    }

    ASSERT_FALSE(blocks[0]);
    ASSERT_FALSE(blocks[1]);

    blocks = {block4};
    for (int i = 0; i < Config->SecondHotBlockThreshold; i++) {
        ASSERT_TRUE(Snooper->OnBlockRead(chunk0, {4}, &blocks).empty());
    }

    suggestions = Snooper->OnBlockRead(chunk0, {4}, &blocks);
    ASSERT_FALSE(suggestions.empty());
    ASSERT_EQ(1u, Snooper->GetHotChunks().size());
    for (auto s : suggestions) {
        ASSERT_EQ(1u, s.P2PIteration);
        ASSERT_TRUE(s.BlockIndex == 4);
        ASSERT_THAT(s.Peers, IsSubsetOf(peers));
    }

    suggestions = Snooper->OnBlockProbe(chunk0, {4});
    ASSERT_FALSE(suggestions.empty());
    for (auto s : suggestions) {
        ASSERT_EQ(1u, s.P2PIteration);
        ASSERT_TRUE(s.BlockIndex == 4);
        ASSERT_THAT(s.Peers, IsSubsetOf(peers));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDataNode
