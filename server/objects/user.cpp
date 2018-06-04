#include "user.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

TUser::TUser(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TSubject(id, TObjectId(), typeHandler, session)
{ }

EObjectType TUser::GetType() const
{
    return EObjectType::User;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
