#include "schema.h"

namespace NYP::NServer::NObjects {

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

} // namespace NYP::NServer::NObjects
