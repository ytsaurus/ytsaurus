#include "bootstrap.h"

#include "config.h"
#include "slot_manager.h"
#include "shortcut_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/cellar_node/bootstrap.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

namespace NYT::NChaosNode {

using namespace NClusterNode;
using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
    , public TBootstrapBase
{
public:
    explicit TBootstrap(NClusterNode::IBootstrap* bootstrap)
        : TBootstrapBase(bootstrap)
        , ClusterNodeBootstrap_(bootstrap)
    { }

    void Initialize() override
    {
        SnapshotStoreReadPool_ = New<TThreadPool>(
            GetConfig()->ChaosNode->SnapshotStoreReadPoolSize,
            "ShortcutRead");

        SlotManager_ = CreateSlotManager(GetConfig()->ChaosNode, this);
        SlotManager_->Initialize();
    }

    void Run() override
    { }

    const IInvokerPtr& GetTransactionTrackerInvoker() const override
    {
        return GetCellarNodeBootstrap()->GetTransactionTrackerInvoker();
    }

    const NCellarAgent::ICellarManagerPtr& GetCellarManager() const override
    {
        return GetCellarNodeBootstrap()->GetCellarManager();
    }

    const IShortcutSnapshotStorePtr& GetShortcutSnapshotStore() const override
    {
        return ShortcutSnapshotStore_;
    }

    const IInvokerPtr& GetSnapshotStoreReadPoolInvoker() const override
    {
        return SnapshotStoreReadPool_->GetInvoker();
    }

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;
    const IShortcutSnapshotStorePtr ShortcutSnapshotStore_ = CreateShortcutSnapshotStore();
 
    TThreadPoolPtr SnapshotStoreReadPool_;
    ISlotManagerPtr SlotManager_;

    NCellarNode::IBootstrap* GetCellarNodeBootstrap() const override
    {
        return ClusterNodeBootstrap_->GetCellarNodeBootstrap();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return std::make_unique<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
