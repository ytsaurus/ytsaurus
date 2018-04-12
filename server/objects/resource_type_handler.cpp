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

    virtual void AfterObjectCreated(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectCreated(transaction, object);

        auto* resource = object->As<TResource>();
        auto& spec = resource->Spec();

        // COMPAT(babenko)
        if (!spec->has_cpu() &&
            !spec->has_memory() &&
            !spec->has_disk())
        {
            switch (static_cast<EResourceKind>(spec->kind())) {
                case EResourceKind::Cpu: {
                    auto* cpuSpec = spec->mutable_cpu();
                    cpuSpec->set_total_capacity(spec->total_capacity());
                    cpuSpec->set_cpu_to_vcpu_factor(1);
                    break;
                }
                case EResourceKind::Memory: {
                    auto* memorySpec = spec->mutable_memory();
                    memorySpec->set_total_capacity(spec->total_capacity());
                    break;
                }
                case EResourceKind::Disk: {
                    auto* diskSpec = spec->mutable_disk();
                    diskSpec->set_total_capacity(spec->total_capacity());
                    diskSpec->set_storage_class("hdd");
                    diskSpec->set_total_volume_slots(100);
                    diskSpec->set_device("/dev/xyz");
                    break;
                }
                default:
                    Y_UNREACHABLE();
            }
        }

        if (spec->has_cpu()) {
            resource->Kind() = EResourceKind::Cpu;
            spec->set_kind(NClient::NApi::NProto::RK_CPU);
        } else if (spec->has_memory()) {
            resource->Kind() = EResourceKind::Memory;
            spec->set_kind(NClient::NApi::NProto::RK_MEMORY);
        } else if (spec->has_disk()) {
            resource->Kind() = EResourceKind::Disk;
            spec->set_kind(NClient::NApi::NProto::RK_DISK);
        } else {
            THROW_ERROR_EXCEPTION("Resource %Qv is of an unrecogznied kind",
                resource->GetId());
        }
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

