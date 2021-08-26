#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/chunk_server/new_replicator/replicator_state.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NChunkServer::NReplicator {
namespace {

using namespace NConcurrency;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

struct TReplicatorStateProxy
    : public IReplicatorStateProxy
{
    virtual const IInvokerPtr& GetChunkInvoker(EChunkThreadQueue /*queue*/) const override
    {
        return Queue->GetInvoker();
    }

    virtual const TDynamicClusterConfigPtr& GetDynamicConfig() const override
    {
        return DynamicConfig;
    }

    virtual bool CheckThreadAffinity() const override
    {
        return false;
    }

    TActionQueuePtr Queue = New<TActionQueue>();

    TDynamicClusterConfigPtr DynamicConfig = New<TDynamicClusterConfig>();
};

////////////////////////////////////////////////////////////////////////////////

class TReplicatorTest
    : public ::testing::Test
{
protected:
    TReplicatorStateProxy* Proxy_;

    IReplicatorStatePtr ReplicatorState_;

    virtual void SetUp() override
    {
        auto proxy = std::make_unique<TReplicatorStateProxy>();
        Proxy_ = proxy.get();

        ReplicatorState_ = CreateReplicatorState(std::move(proxy));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TReplicatorTest, TestDynamicConfigReplication)
{
    Proxy_->DynamicConfig->ChunkManager->MaxChunksPerFetch = 123;

    ReplicatorState_->Load();

    EXPECT_EQ(ReplicatorState_->GetDynamicConfig()->ChunkManager->MaxChunksPerFetch, 123);

    auto newConfig = New<TDynamicClusterConfig>();
    newConfig->ChunkManager->MaxChunksPerFetch = 234;
    ReplicatorState_->UpdateDynamicConfig(newConfig);
    ReplicatorState_->SyncWithUpstream();

    EXPECT_EQ(ReplicatorState_->GetDynamicConfig()->ChunkManager->MaxChunksPerFetch, 234);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // NYT::NChunkServer::NReplicator
