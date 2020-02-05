#include "horizontal_pod_autoscaler_type_handler.h"
#include "type_handler_detail.h"
#include "replica_set.h"
#include "horizontal_pod_autoscaler.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class THorizontalPodAutoscalerTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit THorizontalPodAutoscalerTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::HorizontalPodAutoscaler)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                ParentIdAttributeSchema_ = MakeAttributeSchema("replica_set_id")
                    ->SetParentIdAttribute()
                    ->SetMandatory()
            });

        SpecAttributeSchema_
            ->SetAttribute(THorizontalPodAutoscaler::SpecSchema)
            ->SetExtensible();

        StatusAttributeSchema_
            ->SetAttribute(THorizontalPodAutoscaler::StatusSchema)
            ->SetUpdatable()
            ->SetExtensible();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::THorizontalPodAutoscaler>();
    }

    virtual EObjectType GetParentType() override
    {
        return EObjectType::ReplicaSet;
    }

    virtual TObject* GetParent(TObject* object) override
    {
        return object->As<THorizontalPodAutoscaler>()->ReplicaSet().Load();
    }

    virtual const TDBField* GetIdField() override
    {
        return &HorizontalPodAutoscalersTable.Fields.Meta_Id;
    }

    virtual const TDBField* GetParentIdField() override
    {
        return &HorizontalPodAutoscalersTable.Fields.Meta_ReplicaSetId;
    }

    virtual const TDBTable* GetTable() override
    {
        return &HorizontalPodAutoscalersTable;
    }

    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override
    {
        return &parent->As<TReplicaSet>()->HorizontalPodAutoscaler();
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        return std::make_unique<THorizontalPodAutoscaler>(id, parentId, this, session);
    }
};

std::unique_ptr<IObjectTypeHandler> CreateHorizontalPodAutoscalerTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new THorizontalPodAutoscalerTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
