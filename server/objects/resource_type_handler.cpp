#include "resource_type_handler.h"
#include "type_handler_detail.h"
#include "resource.h"
#include "node.h"
#include "transaction.h"
#include "db_schema.h"

#include <yp/server/scheduler/helpers.h>

#include <yt/core/ytree/fluent.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;
using namespace NScheduler;

using namespace NYT::NYson;
using namespace NYT::NYTree;

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////

class TResourceTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TResourceTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Resource)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                ParentIdAttributeSchema_ = MakeAttributeSchema("node_id")
                    ->SetParentIdAttribute()
                    ->SetMandatory(),
                MakeAttributeSchema("kind")
                    ->SetAttribute(TResource::KindSchema)
            });

        StatusAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("scheduled_allocations")
                    ->SetAttribute(TResource::TStatus::ScheduledAllocationsSchema),

                MakeAttributeSchema("actual_allocations")
                    ->SetAttribute(TResource::TStatus::ActualAllocationsSchema),

                MakeAttributeSchema("used")
                    ->SetPreevaluator<TResource>(std::bind(&TResourceTypeHandler::PreevaluateUsedStatistics, this, _1, _2))
                    ->SetEvaluator<TResource>(std::bind(&TResourceTypeHandler::EvaluateUsedStatistics, this, _1, _2, _3)),

                MakeAttributeSchema("free")
                    ->SetPreevaluator<TResource>(std::bind(&TResourceTypeHandler::PreevaluateFreeStatistics, this, _1, _2))
                    ->SetEvaluator<TResource>(std::bind(&TResourceTypeHandler::EvaluateFreeStatistics, this, _1, _2, _3)),
            });

        SpecAttributeSchema_
            ->SetAttribute(TResource::SpecSchema
                .SetValidator(ValidateSpec))
            ->SetMandatory();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TResource>();
    }

    virtual EObjectType GetParentType() override
    {
        return EObjectType::Node;
    }

    virtual TObject* GetParent(TObject* object) override
    {
        return object->As<TResource>()->Node().Load();
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
        std::optional<EResourceKind> optionalKind;
        auto setKind = [&] (EResourceKind kind) {
            if (optionalKind) {
                THROW_ERROR_EXCEPTION("Resource %Qv has multiple kinds",
                    resource->GetId());
            }
            optionalKind = kind;
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
        if (spec.has_slot()) {
            setKind(EResourceKind::Slot);
        }
        if (!optionalKind) {
            THROW_ERROR_EXCEPTION("Resource %Qv is of an unrecognized kind",
                resource->GetId());
        }
        return *optionalKind;
    }

    static void ValidateDiskSpec(
        const TResource::TSpec::TDiskSpec& diskSpec)
    {
        auto validate = [] (double value, TStringBuf name) {
            if (value < 1e-8 || value > 1e8) {
                THROW_ERROR_EXCEPTION("Parameter %v must be in range [1e-8, 1e8], but the actual value is %.9lf",
                    name,
                    value);
            }
        };
        if (diskSpec.has_read_bandwidth_factor()) {
            validate(diskSpec.read_bandwidth_factor(), "/spec/disk/read_bandwidth_factor");
        }
        if (diskSpec.has_write_bandwidth_factor()) {
            validate(diskSpec.write_bandwidth_factor(), "/spec/disk/write_bandwidth_factor");
        }
        if (diskSpec.has_read_operation_rate_divisor()) {
            validate(diskSpec.read_operation_rate_divisor(), "/spec/disk/read_operation_rate_divisor");
        }
        if (diskSpec.has_write_operation_rate_divisor()) {
            validate(diskSpec.write_operation_rate_divisor(), "/spec/disk/write_operation_rate_divisor");
        }
    }

    static void ValidateSpec(
        TTransaction* /*transaction*/,
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
        if (newKind == EResourceKind::Disk) {
            ValidateDiskSpec(spec.disk());
        }
    }

    virtual void BeforeObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* resource = object->As<TResource>();
        transaction->ScheduleValidateNodeResources(resource->Node().Load());
        resource->Kind() = EResourceKind::Undefined;
    }

    virtual void AfterObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectCreated(transaction, object);

        auto* resource = object->As<TResource>();
        auto* spec = resource->Spec().Get();
        auto kind = DeriveKind(resource, *spec);
        resource->Kind() = kind;
        if (kind == EResourceKind::Cpu && !spec->cpu().has_cpu_to_vcpu_factor()) {
            spec->mutable_cpu()->set_cpu_to_vcpu_factor(1.0);
        }
    }

    virtual void BeforeObjectRemoved(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectRemoved(transaction, object);

        auto* resource = object->As<TResource>();
        if (!resource->Status().ScheduledAllocations().Load().empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove resource %Qv since it is being used",
                resource->GetId());
        }
    }


    void PreevaluateUsedStatistics(TTransaction* /*transaction*/, TResource* resource)
    {
        resource->Kind().ScheduleLoad();
        resource->Status().ScheduledAllocations().ScheduleLoad();
        resource->Status().ActualAllocations().ScheduleLoad();
    }

    void EvaluateUsedStatistics(TTransaction* /*transaction*/, TResource* resource, IYsonConsumer* consumer)
    {
        auto allocationStatistics = ComputeTotalAllocationStatistics(
            resource->Status().ScheduledAllocations().Load(),
            resource->Status().ActualAllocations().Load());
        const auto& usedCapacities = allocationStatistics.Capacities;
        auto usedStatistics = ResourceCapacitiesToStatistics(
            usedCapacities,
            resource->Kind().Load());
        WriteProtobufMessage(consumer, usedStatistics);
    }

    void PreevaluateFreeStatistics(TTransaction* transaction, TResource* resource)
    {
        PreevaluateUsedStatistics(transaction, resource);
        resource->Spec().ScheduleLoad();
    }

    void EvaluateFreeStatistics(TTransaction* /*transaction*/, TResource* resource, IYsonConsumer* consumer)
    {
        auto allocationStatistics = ComputeTotalAllocationStatistics(
            resource->Status().ScheduledAllocations().Load(),
            resource->Status().ActualAllocations().Load());
        const auto& usedCapacities = allocationStatistics.Capacities;
        auto totalCapacities = GetResourceCapacities(resource->Spec().Load());
        auto freeCapacities = SubtractWithClamp(totalCapacities, usedCapacities);
        auto freeStatistics = ResourceCapacitiesToStatistics(
            freeCapacities,
            resource->Kind().Load());
        WriteProtobufMessage(consumer, freeStatistics);
    }
};

std::unique_ptr<IObjectTypeHandler> CreateResourceTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TResourceTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

