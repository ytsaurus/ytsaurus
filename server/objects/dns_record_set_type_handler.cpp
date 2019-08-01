#include "virtual_service_type_handler.h"
#include "type_handler_detail.h"
#include "dns_record_set.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TDnsRecordSetTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TDnsRecordSetTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::DnsRecordSet)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->SetAttribute(TDnsRecordSet::SpecSchema);
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TDnsRecordSet>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &DnsRecordSetsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &DnsRecordSetsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TDnsRecordSet(id, this, session));
    }
};

std::unique_ptr<IObjectTypeHandler> CreateDnsRecordSetTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TDnsRecordSetTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
