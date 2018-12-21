#include "user.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TUser, TUser::TSpec> TUser::SpecSchema{
    &UsersTable.Fields.Spec,
    [] (TUser* user) { return &user->Spec(); }
};

TUser::TUser(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TSubject(id, TObjectId(), typeHandler, session)
    , Spec_(this, &SpecSchema)
{ }

EObjectType TUser::GetType() const
{
    return EObjectType::User;
}

bool TUser::IsBuiltin() const
{
    return GetId() == RootUserId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
