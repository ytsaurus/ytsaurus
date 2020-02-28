#include "rack_type_handler.h"
#include "node_tracker.h"
#include "rack.h"
#include "rack_proxy.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/server/master/object_server/type_handler_detail.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

namespace NYT::NNodeTrackerServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NNodeTrackerClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TRackTypeHandler
    : public TObjectTypeHandlerWithMapBase<TRack>
{
public:
    TRackTypeHandler(
        NCellMaster::TBootstrap* bootstrap,
        TEntityMap<TRack>* map)
        : TObjectTypeHandlerWithMapBase(bootstrap, map)
    { }

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Rack;
    }

    virtual TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto name = attributes->GetAndRemove<TString>("name");
        return Bootstrap_->GetNodeTracker()->CreateRack(name, hintId);
    }

private:
    virtual TCellTagList DoGetReplicationCellTags(const TRack* /*rack*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual IObjectProxyPtr DoGetProxy(TRack* rack, TTransaction* transaction) override
    {
        return CreateRackProxy(Bootstrap_, &Metadata_, rack);
    }

    virtual void DoZombifyObject(TRack* rack) override
    {
        TObjectTypeHandlerWithMapBase::DoZombifyObject(rack);
        Bootstrap_->GetNodeTracker()->ZombifyRack(rack);
    }
};

IObjectTypeHandlerPtr CreateRackTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    TEntityMap<TRack>* map)
{
    return New<TRackTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
