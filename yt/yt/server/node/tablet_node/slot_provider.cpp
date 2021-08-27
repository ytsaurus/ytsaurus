#include "private.h"
#include "tablet_slot.h"
#include "slot_provider.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/cellar_agent/occupier.h>

#include <yt/yt/server/lib/tablet_node/config.h>

namespace NYT::NTabletNode {

using namespace NCellarAgent;

////////////////////////////////////////////////////////////////////////////////

class TTabletSlotProvider
    : public ICellarOccupierProvider
{
public:
    TTabletSlotProvider(
        TTabletNodeConfigPtr config,
        IBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
    { }

    ICellarOccupierPtr CreateCellarOccupier(int index) override
    {
        return CreateTabletSlot(index, Config_, Bootstrap_);
    }

private:
    const TTabletNodeConfigPtr Config_;
    IBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

ICellarOccupierProviderPtr CreateTabletSlotOccupierProvider(
    TTabletNodeConfigPtr config,
    IBootstrap* bootstrap)
{
    return New<TTabletSlotProvider>(std::move(config), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
