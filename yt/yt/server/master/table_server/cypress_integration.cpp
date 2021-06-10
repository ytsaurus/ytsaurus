#include "cypress_integration.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TVirtualMasterTableSchemaMap
    : public TVirtualMulticellMapBase
{
public:
    using TVirtualMulticellMapBase::TVirtualMulticellMapBase;

private:
    virtual std::vector<TObjectId> GetKeys(i64 sizeLimit) const override
    {
        const auto& tableManager = Bootstrap_->GetTableManager();
        return NYT::GetKeys(tableManager->MasterTableSchemas(), sizeLimit);
    }

    virtual bool IsValid(TObject* object) const override
    {
        return IsObjectAlive(object);
    }

    virtual i64 GetSize() const override
    {
        const auto& tableManager = Bootstrap_->GetTableManager();
        return tableManager->MasterTableSchemas().GetSize();
    }

    virtual NYPath::TYPath GetWellKnownPath() const override
    {
        return "//sys/master_table_schemas";
    }
};

INodeTypeHandlerPtr CreateMasterTableSchemaMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::MasterTableSchemaMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualMasterTableSchemaMap>(bootstrap, std::move(owningNode));
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
