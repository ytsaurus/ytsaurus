#include "data_center_type_handler.h"
#include "node_tracker.h"
#include "data_center.h"
#include "data_center_proxy.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/server/master/object_server/type_handler_detail.h>

namespace NYT::NNodeTrackerServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NNodeTrackerClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TDataCenterTypeHandler
    : public TObjectTypeHandlerWithMapBase<TDataCenter>
{
public:
    TDataCenterTypeHandler(
        NCellMaster::TBootstrap* bootstrap,
        TEntityMap<TDataCenter>* map)
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
        return EObjectType::DataCenter;
    }

    virtual TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto name = attributes->GetAndRemove<TString>("name");
        return Bootstrap_->GetNodeTracker()->CreateDataCenter(name, hintId);
    }

private:
    virtual TCellTagList DoGetReplicationCellTags(const TDataCenter* /*dc*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual IObjectProxyPtr DoGetProxy(TDataCenter* dc, TTransaction* /*transaction*/) override
    {
        return CreateDataCenterProxy(Bootstrap_, &Metadata_, dc);
    }

    virtual void DoZombifyObject(TDataCenter* dc) override
    {
        TObjectTypeHandlerWithMapBase::DoZombifyObject(dc);
        Bootstrap_->GetNodeTracker()->ZombifyDataCenter(dc);
    }
};

IObjectTypeHandlerPtr CreateDataCenterTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    TEntityMap<TDataCenter>* map)
{
    return New<TDataCenterTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
