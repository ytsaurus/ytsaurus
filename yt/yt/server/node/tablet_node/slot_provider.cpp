#include "private.h"
#include "tablet_slot.h"
#include "slot_provider.h"

#include <yt/server/node/cluster_node/bootstrap.h>

#include <yt/server/lib/cellar_agent/occupier.h>

#include <yt/server/lib/tablet_node/config.h>

namespace NYT::NTabletNode {

using namespace NCellarAgent;

////////////////////////////////////////////////////////////////////////////////

class TTabletSlotProvider
    : public ICellarOccupierProvider
{
public:
    TTabletSlotProvider(
        TTabletNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
    { }

    virtual ICellarOccupierPtr CreateCellarOccupier(int index) override
    {
        return New<TTabletSlot>(index, Config_, Bootstrap_);
    }

private:
    const TTabletNodeConfigPtr Config_;
    NClusterNode::TBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

ICellarOccupierProviderPtr CreateTabletSlotOccupierProvider(
    TTabletNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap)
{
    return New<TTabletSlotProvider>(std::move(config), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
