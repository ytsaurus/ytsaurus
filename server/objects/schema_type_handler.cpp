#include "schema_type_handler.h"
#include "type_handler_detail.h"
#include "schema.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TSchemaTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TSchemaTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Schema)
    {
        SpecAttributeSchema_
            ->SetComposite();

        StatusAttributeSchema_
            ->SetComposite();
    }

    virtual const TDBTable* GetTable() override
    {
        return &SchemasTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &SchemasTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YCHECK(!parentId);
        return std::unique_ptr<TObject>(new TSchema(id, this, session));
    }
};

std::unique_ptr<IObjectTypeHandler> CreateSchemaTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TSchemaTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

