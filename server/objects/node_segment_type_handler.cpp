#include "node_segment_type_handler.h"

#include "config.h"
#include "db_schema.h"
#include "node_segment.h"
#include "pod.h"
#include "pod_set.h"
#include "type_handler_detail.h"

namespace NYP::NServer::NObjects {

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TNodeSegmentTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    TNodeSegmentTypeHandler(NMaster::TBootstrap* bootstrap, TNodeSegmentTypeHandlerConfigPtr config)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::NodeSegment)
        , Config_(std::move(config))
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->SetAttribute(TNodeSegment::SpecSchema
                .SetInitializer(InitializeSpec))

            ->SetValidator<TNodeSegment>(std::bind(&TNodeSegmentTypeHandler::ValidateSpec, this, _1, _2));

        StatusAttributeSchema_
            ->SetAttribute(TNodeSegment::StatusSchema);
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TNodeSegment>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &NodeSegmentsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &NodeSegmentsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TNodeSegment(id, this, session));
    }

    virtual void BeforeObjectRemoved(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectRemoved(transaction, object);

        auto* segment = object->As<TNodeSegment>();
        const auto& podSets = segment->PodSets().Load();
        if (!podSets.empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove node segment %Qv since it has %v pod set(s) assigned",
                segment->GetId(),
                podSets.size());
        }
    }

private:
    const TNodeSegmentTypeHandlerConfigPtr Config_;

    static void InitializeSpec(
        TTransaction* /*transaction*/,
        TNodeSegment* /*segment*/,
        NClient::NApi::NProto::TNodeSegmentSpec* spec)
    {
        if (!spec->has_enable_unsafe_porto()) {
            spec->set_enable_unsafe_porto(true);
        }
    }

    void ValidateSpec(TTransaction* /*transaction*/, TNodeSegment* nodeSegment)
    {
        const auto& spec = nodeSegment->Spec();

        if (spec.IsChanged()) {
            const auto& specLoaded = spec.Load();
            const auto& specLoadedOld = spec.LoadOld();

            if (!specLoaded.enable_unsafe_porto() &&
                specLoadedOld.enable_unsafe_porto())
            {
                auto podSets = nodeSegment->PodSets().Load();
                for (auto* podSet : podSets) {
                    auto pods = podSet->Pods().Load();
                    for (auto* pod : pods) {
                        ValidateIssPodSpecSafe(pod);
                    }
                }
            }

            if (specLoaded.pod_constraints().has_vcpu_guarantee_to_limit_ratio()) {
                const auto& vcpuConstraints = specLoaded.pod_constraints().vcpu_guarantee_to_limit_ratio();
                const auto& vcpuConstraintsOld = specLoadedOld.pod_constraints().vcpu_guarantee_to_limit_ratio();

                if ((vcpuConstraints.multiplier() != vcpuConstraintsOld.multiplier() || !vcpuConstraintsOld.has_multiplier()) &&
                    (vcpuConstraints.multiplier() < 1.0 || vcpuConstraints.multiplier() > Config_->PodVcpuGuaranteeToLimitRatioConstraint->MaxMultiplier))
                {
                    THROW_ERROR_EXCEPTION("Invalid vcpu guarantee to limit ratio constraint: multiplier expected to be in range [1.0, %v], but got %v",
                        Config_->PodVcpuGuaranteeToLimitRatioConstraint->MaxMultiplier,
                        vcpuConstraints.multiplier());
                }
                if (vcpuConstraints.additive() != vcpuConstraintsOld.additive() &&
                    vcpuConstraints.additive() > Config_->PodVcpuGuaranteeToLimitRatioConstraint->MaxAdditive)
                {
                    THROW_ERROR_EXCEPTION("Invalid vcpu guarantee to limit ratio constraint: additive expected to be in range [0, %v], but got %v",
                        Config_->PodVcpuGuaranteeToLimitRatioConstraint->MaxAdditive,
                        vcpuConstraints.additive());
                }
            }
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreateNodeSegmentTypeHandler(
    NMaster::TBootstrap* bootstrap,
    TNodeSegmentTypeHandlerConfigPtr config)
{
    return std::unique_ptr<IObjectTypeHandler>(new TNodeSegmentTypeHandler(bootstrap, std::move(config)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

