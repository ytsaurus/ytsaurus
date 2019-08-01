#include "resource_type_handler.h"
#include "type_handler_detail.h"
#include "pod_set.h"
#include "dynamic_resource.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TDynamicResourceTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TDynamicResourceTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::DynamicResource)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                ParentIdAttributeSchema_ = MakeAttributeSchema("pod_set_id")
                    ->SetParentIdAttribute()
                    ->SetMandatory()
            });

        SpecAttributeSchema_
            ->SetAttribute(TDynamicResource::SpecSchema)
            ->SetUpdatable()
            ->SetExtensible();

        StatusAttributeSchema_
            ->SetAttribute(TDynamicResource::StatusSchema)
            ->SetUpdatable()
            ->SetExtensible();

    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TDynamicResource>();
    }

    virtual EObjectType GetParentType() override
    {
        return EObjectType::PodSet;
    }

    virtual const TDBField* GetIdField() override
    {
        return &DynamicResourcesTable.Fields.Meta_Id;
    }

    virtual const TDBField* GetParentIdField() override
    {
        return &DynamicResourcesTable.Fields.Meta_PodSetId;
    }

    virtual const TDBTable* GetTable() override
    {
        return &DynamicResourcesTable;
    }

    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override
    {
        return &parent->As<TPodSet>()->DynamicResources();
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        return std::make_unique<TDynamicResource>(id, parentId, this, session);
    }
};

std::unique_ptr<IObjectTypeHandler> CreateDynamicResourceTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TDynamicResourceTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

