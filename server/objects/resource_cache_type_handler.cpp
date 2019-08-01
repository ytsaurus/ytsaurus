#include "resource_type_handler.h"
#include "type_handler_detail.h"
#include "pod_set.h"
#include "resource_cache.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TResourceCacheTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TResourceCacheTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::ResourceCache)
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
            ->SetAttribute(TResourceCache::SpecSchema)
            ->SetExtensible();

        StatusAttributeSchema_
            ->SetAttribute(TResourceCache::StatusSchema)
            ->SetUpdatable()
            ->SetExtensible();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TResourceCache>();
    }

    virtual EObjectType GetParentType() override
    {
        return EObjectType::PodSet;
    }

    virtual TObject* GetParent(TObject* object) override
    {
        return object->As<TResourceCache>()->PodSet().Load();
    }

    virtual const TDBField* GetIdField() override
    {
        return &ResourceCachesTable.Fields.Meta_Id;
    }

    virtual const TDBField* GetParentIdField() override
    {
        return &ResourceCachesTable.Fields.Meta_PodSetId;
    }

    virtual const TDBTable* GetTable() override
    {
        return &ResourceCachesTable;
    }

    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override
    {
        return &parent->As<TPodSet>()->ResourceCache();
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        return std::make_unique<TResourceCache>(id, parentId, this, session);
    }
};

std::unique_ptr<IObjectTypeHandler> CreateResourceCacheTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TResourceCacheTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

