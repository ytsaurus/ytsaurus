#include "bootstrap.h"

#include "config.h"
#include "slot_manager.h"

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

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

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
