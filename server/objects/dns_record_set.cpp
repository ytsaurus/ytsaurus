#include "dns_record_set.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TDnsRecordSet, TDnsRecordSet::TSpec> TDnsRecordSet::SpecSchema{
    &DnsRecordSetsTable.Fields.Spec,
    [] (TDnsRecordSet* recordSet) { return &recordSet->Spec();}
};

TDnsRecordSet::TDnsRecordSet(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Spec_(this, &SpecSchema)
{ }

EObjectType TDnsRecordSet::GetType() const
{
    return EObjectType::DnsRecordSet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

