#include "daemon_set_type_handler.h"

#include "daemon_set.h"
#include "db_schema.h"
#include "type_handler_detail.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TDaemonSetTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TDaemonSetTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::DaemonSet)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("pod_set_id")
                    ->SetAttribute(TDaemonSet::PodSetIdSchema)
                    ->SetUpdatable()
            });

        SpecAttributeSchema_
            ->SetAttribute(TDaemonSet::SpecSchema)
            ->SetUpdatable();

        StatusAttributeSchema_
            ->SetAttribute(TDaemonSet::StatusSchema)
            ->SetUpdatable();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TDaemonSet>();
    }

    virtual const TDBField* GetIdField() override
    {
        return &DaemonSetsTable.Fields.Meta_Id;
    }

    virtual const TDBTable* GetTable() override
    {
        return &DaemonSetsTable;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TDaemonSet>(id, this, session);
    }
};

std::unique_ptr<IObjectTypeHandler> CreateDaemonSetTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TDaemonSetTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
