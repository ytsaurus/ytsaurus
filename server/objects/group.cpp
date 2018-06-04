#include "group.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TGroup, TGroup::TSpec> TGroup::SpecSchema{
    &GroupsTable.Fields.Spec,
    [] (TGroup* group) { return &group->Spec(); }
};

TGroup::TGroup(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TSubject(id, TObjectId(), typeHandler, session)
    , Spec_(this, &SpecSchema)
{ }

EObjectType TGroup::GetType() const
{
    return EObjectType::Group;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
