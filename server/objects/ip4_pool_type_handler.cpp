#include "ip4_pool_type_handler.h"

#include "db_schema.h"
#include "ip4_pool.h"
#include "type_handler_detail.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TIP4PoolsTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TIP4PoolsTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::IP4Pool)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->SetAttribute(TIP4Pool::SpecSchema);

        StatusAttributeSchema_
            ->SetAttribute(TIP4Pool::StatusSchema);
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TIP4Pool>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &IP4PoolsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &IP4PoolsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TIP4Pool(id, this, session));
    }
};

std::unique_ptr<IObjectTypeHandler> CreateIP4PoolsTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TIP4PoolsTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

