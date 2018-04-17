#include "resource_type_handler.h"
#include "type_handler_detail.h"
#include "resource.h"
#include "node.h"
#include "transaction.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TResourceTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TResourceTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Resource)
    {
        MetaAttributeSchema_
            ->AddChildren({
                ParentIdAttributeSchema_ = MakeAttributeSchema("node_id")
                    ->SetParentAttribute()
                    ->SetMandatory(),
                MakeAttributeSchema("kind")
                    ->SetAttribute(TResource::KindSchema)
            });

        StatusAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("scheduled_allocations")
                    ->SetAttribute(TResource::TStatus::ScheduledAllocationsSchema),

                MakeAttributeSchema("actual_allocations")
                    ->SetAttribute(TResource::TStatus::ActualAllocationsSchema)
            });

        SpecAttributeSchema_
            ->SetAttribute(TResource::SpecSchema
                .SetValidator(ValidateSpec))
            ->SetMandatory();
    }

    virtual EObjectType GetParentType() override
    {
        return EObjectType::Node;
    }

    virtual const TDbTable* GetTable() override
    {
        return &ResourcesTable;
    }

    virtual const TDbField* GetIdField() override
    {
        return &ResourcesTable.Fields.Meta_Id;
    }

    virtual const TDbField* GetParentIdField() override
    {
        return &ResourcesTable.Fields.Meta_NodeId;
    }

    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override
    {
        return &parent->As<TNode>()->Resources();
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        return std::make_unique<TResource>(id, parentId, this, session);
    }

private:
    static void ValidateSpec(const TResource::TSpec& oldSpec, const TResource::TSpec& newSpec)
    {
        if (oldSpec.has_kind() &&
            newSpec.has_kind() &&
            oldSpec.kind() != newSpec.kind())
        {
            THROW_ERROR_EXCEPTION("Changing resource kind is forbidden");
        }
    }

    virtual void BeforeObjectCreated(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* resource = object->As<TResource>();
        transaction->ScheduleValidateNodeResources(resource->Node().Load());
    }

    virtual void BeforeObjectRemoved(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectRemoved(transaction, object);

        auto* resource = object->As<TResource>();
        resource->Status().ScheduledAllocations().ScheduleLoad();
        resource->Status().ActualAllocations().ScheduleLoad();
        if (!resource->Status().ScheduledAllocations().Load().empty() ||
            !resource->Status().ActualAllocations().Load().empty())
        {
            THROW_ERROR_EXCEPTION("Cannot remove resource %Qv since it is being used",
                resource->GetId());
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreateResourceTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TResourceTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

