#include "network_project_type_handler.h"
#include "type_handler_detail.h"
#include "network_project.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TNetworkProjectTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TNetworkProjectTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::NetworkProject)
    {
        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("project_id")
                    ->SetAttribute(TNetworkProject::TSpec::ProjectIdSchema)
                    ->SetMandatory()
            });

        StatusAttributeSchema_
            ->SetComposite();
    }

    virtual const TDbTable* GetTable() override
    {
        return &NetworkProjectsTable;
    }

    virtual const TDbField* GetIdField() override
    {
        return &NetworkProjectsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TNetworkProject>(id, this, session);
    }
};

std::unique_ptr<IObjectTypeHandler> CreateNetworkProjectTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TNetworkProjectTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

