#include "user_type_handler.h"
#include "subject_type_handler_detail.h"
#include "user.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TUserTypeHandler
    : public TSubjectTypeHandlerBase
{
public:
    explicit TUserTypeHandler(NMaster::TBootstrap* bootstrap)
        : TSubjectTypeHandlerBase(bootstrap, EObjectType::User)
    {
        SpecAttributeSchema_
            ->SetComposite();

        StatusAttributeSchema_
            ->SetComposite();
    }

    virtual const TDBTable* GetTable() override
    {
        return &UsersTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &UsersTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YCHECK(!parentId);
        return std::unique_ptr<TObject>(new TUser(id, this, session));
    }
};

std::unique_ptr<IObjectTypeHandler> CreateUserTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TUserTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

