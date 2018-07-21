#include "resource_type_handler.h"
#include "type_handler_detail.h"
#include "resource.h"
#include "node.h"
#include "transaction.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NAccessControl;

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

    virtual const TDBTable* GetTable() override
    {
        return &ResourcesTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &ResourcesTable.Fields.Meta_Id;
    }

    virtual const TDBField* GetParentIdField() override
    {
        return &ResourcesTable.Fields.Meta_NodeId;
    }

    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override
    {
        return &parent->As<TNode>()->Resources();
    }

    virtual TObject* GetAccessControlParent(TObject* object) override
    {
        return object->As<TResource>()->Node().Load();
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        return std::make_unique<TResource>(id, parentId, this, session);
    }

private:
    virtual std::vector<EAccessControlPermission> GetDefaultPermissions() override
    {
        return {};
    }

    static EResourceKind DeriveKind(TResource* resource, const TResource::TSpec& spec)
    {
        TNullable<EResourceKind> maybeKind;
        auto setKind = [&] (EResourceKind kind) {
            if (maybeKind) {
                THROW_ERROR_EXCEPTION("Resource %Qv has multiple kinds",
                    resource->GetId());
            }
            maybeKind = kind;
        };
        if (spec.has_cpu()) {
            setKind(EResourceKind::Cpu);
        }
        if (spec.has_memory()) {
            setKind(EResourceKind::Memory);
        }
        if (spec.has_disk()) {
            setKind(EResourceKind::Disk);
        }
        if (!maybeKind) {
            THROW_ERROR_EXCEPTION("Resource %Qv is of an unrecognized kind",
                resource->GetId());
        }
        return *maybeKind;
    }

    static void ValidateSpec(
        const TTransactionPtr& transaction,
        TResource* resource,
        const TResource::TSpec& spec)
    {
        auto oldKind = resource->Kind().Load();
        auto newKind = DeriveKind(resource, spec);
        if (oldKind != EResourceKind::Undefined && oldKind != newKind) {
            THROW_ERROR_EXCEPTION("Changing kind of resource %Qv from %Qlv to %Qlv is forbidden",
                resource->GetId(),
                oldKind,
                newKind);
        }
    }

    virtual void BeforeObjectCreated(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* resource = object->As<TResource>();
        transaction->ScheduleValidateNodeResources(resource->Node().Load());
        resource->Kind() = EResourceKind::Undefined;
    }

    virtual void AfterObjectCreated(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectCreated(transaction, object);

        auto* resource = object->As<TResource>();
        resource->Kind() = DeriveKind(resource, resource->Spec().Load());
    }

    virtual void BeforeObjectRemoved(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectRemoved(transaction, object);

        auto* resource = object->As<TResource>();
        if (!resource->Status().ScheduledAllocations().Load().empty()) {
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

