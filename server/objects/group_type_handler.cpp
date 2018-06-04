#include "group_type_handler.h"
#include "subject_type_handler_detail.h"
#include "group.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TGroupTypeHandler
    : public TSubjectTypeHandlerBase
{
public:
    explicit TGroupTypeHandler(NMaster::TBootstrap* bootstrap)
        : TSubjectTypeHandlerBase(bootstrap, EObjectType::Group)
    {
        SpecAttributeSchema_
            ->SetAttribute(TGroup::SpecSchema);

        StatusAttributeSchema_
            ->SetComposite();
    }

    virtual const TDBTable* GetTable() override
    {
        return &GroupsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &GroupsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YCHECK(!parentId);
        return std::unique_ptr<TObject>(new TGroup(id, this, session));
    }
};

std::unique_ptr<IObjectTypeHandler> CreateGroupTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TGroupTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

