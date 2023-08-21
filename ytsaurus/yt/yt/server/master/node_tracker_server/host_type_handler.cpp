#include "host_type_handler.h"

#include "host.h"
#include "host_proxy.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/master/object_server/type_handler_detail.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class THostTypeHandler
    : public TObjectTypeHandlerWithMapBase<THost>
{
public:
    THostTypeHandler(
        TBootstrap* bootstrap,
        TEntityMap<THost>* map)
        : TObjectTypeHandlerWithMapBase(bootstrap, map)
    { }

    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::Creatable |
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Removable;
    }

    EObjectType GetType() const override
    {
        return EObjectType::Host;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto name = attributes->GetAndRemove<TString>("name");
        return Bootstrap_->GetNodeTracker()->CreateHost(name, hintId);
    }

private:
    TCellTagList DoGetReplicationCellTags(const THost* /*host*/) override
    {
        return AllSecondaryCellTags();
    }

    IObjectProxyPtr DoGetProxy(THost* host, TTransaction* /*transaction*/) override
    {
        return CreateHostProxy(Bootstrap_, &Metadata_, host);
    }

    void DoZombifyObject(THost* host) override
    {
        Bootstrap_->GetNodeTracker()->ZombifyHost(host);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateHostTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<THost>* map)
{
    return New<THostTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
