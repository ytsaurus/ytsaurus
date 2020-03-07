#include "daemon_set.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TDaemonSet, TObjectId> TDaemonSet::PodSetIdSchema{
    &DaemonSetsTable.Fields.Meta_PodSetId,
    [] (TDaemonSet* daemonSet) { return &daemonSet->PodSetId(); }
};

const TScalarAttributeSchema<TDaemonSet, TDaemonSet::TSpec> TDaemonSet::SpecSchema{
    &DaemonSetsTable.Fields.Spec,
    [] (TDaemonSet* daemonSet) { return &daemonSet->Spec(); }
};

const TScalarAttributeSchema<TDaemonSet, TDaemonSet::TStatus> TDaemonSet::StatusSchema{
    &DaemonSetsTable.Fields.Status,
    [] (TDaemonSet* daemonSet) { return &daemonSet->Status(); }
};

TDaemonSet::TDaemonSet(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , PodSetId_(this, &PodSetIdSchema)
    , Spec_(this, &SpecSchema)
    , Status_(this, &StatusSchema)
{ }

EObjectType TDaemonSet::GetType() const
{
    return EObjectType::DaemonSet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
