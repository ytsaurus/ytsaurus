#include "public.h"
#include "chaos_slot.h"
#include "slot_provider.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/cellar_agent/occupier.h>

#include <yt/yt/server/lib/chaos_node/config.h>

namespace NYT::NChaosNode {

using namespace NCellarAgent;

////////////////////////////////////////////////////////////////////////////////

class TChaosSlotProvider
    : public ICellarOccupierProvider
{
public:
    TChaosSlotProvider(
        TChaosNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
    { }

    virtual ICellarOccupierPtr CreateCellarOccupier(int index) override
    {
        return CreateChaosSlot(index, Config_, Bootstrap_);
    }

private:
    const TChaosNodeConfigPtr Config_;
    NClusterNode::TBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

ICellarOccupierProviderPtr CreateChaosCellarOccupierProvider(
    TChaosNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap)
{
    return New<TChaosSlotProvider>(std::move(config), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChaosNode::NYT
