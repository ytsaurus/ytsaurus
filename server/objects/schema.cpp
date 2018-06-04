#include "schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

TSchema::TSchema(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
{ }

EObjectType TSchema::GetType() const
{
    return EObjectType::Schema;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
