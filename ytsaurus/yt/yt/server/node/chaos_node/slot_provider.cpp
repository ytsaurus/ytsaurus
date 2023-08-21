#include "public.h"
#include "bootstrap.h"
#include "chaos_slot.h"
#include "slot_provider.h"

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
        IBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
    { }

    ICellarOccupierPtr CreateCellarOccupier(int index) override
    {
        return CreateChaosSlot(index, Config_, Bootstrap_);
    }

private:
    const TChaosNodeConfigPtr Config_;
    IBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

ICellarOccupierProviderPtr CreateChaosCellarOccupierProvider(
    TChaosNodeConfigPtr config,
    IBootstrap* bootstrap)
{
    return New<TChaosSlotProvider>(std::move(config), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChaosNode::NYT
