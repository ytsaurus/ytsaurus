#include "pod_disruption_budget_type_handler.h"
#include "pod_disruption_budget.h"
#include "pod_set.h"
#include "type_handler_detail.h"
#include "db_schema.h"

#include <yp/server/master/bootstrap.h>

namespace NYP::NServer::NObjects {

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TPodDisruptionBudgetTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TPodDisruptionBudgetTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::PodDisruptionBudget)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        StatusAttributeSchema_
            ->SetAttribute(TPodDisruptionBudget::StatusSchema);

        SpecAttributeSchema_
            ->SetAttribute(TPodDisruptionBudget::SpecSchema)
            ->SetUpdatable()
            ->SetUpdateHandler<TPodDisruptionBudget>(std::bind(&TPodDisruptionBudgetTypeHandler::OnSpecUpdated, this, _1, _2))
            ->SetValidator<TPodDisruptionBudget>(std::bind(&TPodDisruptionBudgetTypeHandler::ValidateSpec, this, _1, _2));
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TPodDisruptionBudget>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &PodDisruptionBudgetsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &PodDisruptionBudgetsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TPodDisruptionBudget(id, this, session));
    }

    virtual void BeforeObjectCreated(TTransaction* transaction, TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* podDisruptionBudget = object->As<TPodDisruptionBudget>();
        podDisruptionBudget->FreezeUntilSync(
            "Pod disruption budget is created and frozen until the first synchronization");
    }

    virtual void BeforeObjectRemoved(TTransaction* transaction, TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectRemoved(transaction, object);

        auto* podDisruptionBudget = object->As<TPodDisruptionBudget>();
        const auto& podSets = podDisruptionBudget->PodSets().Load();
        if (!podSets.empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove pod disruption budget %Qv since it is used in %v pod set(s)",
                podDisruptionBudget->GetId(),
                podSets.size());
        }
    }

private:
    void OnSpecUpdated(TTransaction* /*transaction*/, TPodDisruptionBudget* podDisruptionBudget)
    {
        podDisruptionBudget->FreezeUntilSync(
            "Pod disruption budget is frozen until the next synchronization due to spec update");
    }

    void ValidateSpec(TTransaction* /*transaction*/, TPodDisruptionBudget* podDisruptionBudget)
    {
        auto validate = [] (auto value, TStringBuf name) {
            if (value < 0) {
                THROW_ERROR_EXCEPTION("Parameter %v must be non-negative, but the actual value is %v",
                    name,
                    value);
            }
        };
        if (podDisruptionBudget->Spec().IsChanged()) {
            const auto& spec = podDisruptionBudget->Spec().Load();
            validate(spec.max_pods_unavailable(), "max_pods_unavailable");
            validate(spec.max_pod_disruptions_between_syncs(), "max_pod_disruptions_between_syncs");
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreatePodDisruptionBudgetTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TPodDisruptionBudgetTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

