#include "cypress_integration.h"

#include "area.h"
#include "tamed_cell_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>
#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>
#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/lib/misc/object_helpers.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCellServer {

using namespace NYPath;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NCellarClient;

////////////////////////////////////////////////////////////////////////////////

class TVirtualAreaMap
    : public TVirtualMapBase
{
public:
    explicit TVirtualAreaMap(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

private:
    TBootstrap* const Bootstrap_;

    virtual std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        return ConvertToStrings(GetValues(cellManager->Areas(), sizeLimit), TObjectIdFormatter());
    }

    virtual bool IsValid(TObject* object) const
    {
        return object->GetType() == EObjectType::Area;
    }

    virtual i64 GetSize() const override
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        return std::ssize(cellManager->Areas());
    }

    virtual IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* area = cellManager->Areas().Find(TAreaId::FromString(key));
        if (!IsObjectAlive(area)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(area);
    }
};

INodeTypeHandlerPtr CreateAreaMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::AreaMap,
        BIND([=] (INodePtr /*owningNode*/) -> IYPathServicePtr {
            return New<TVirtualAreaMap>(bootstrap);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
