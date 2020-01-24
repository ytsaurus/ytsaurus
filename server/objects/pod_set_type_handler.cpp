#include "pod_set_type_handler.h"

#include "account.h"
#include "config.h"
#include "db_schema.h"
#include "node_segment.h"
#include "pod.h"
#include "pod_disruption_budget.h"
#include "pod_set.h"
#include "type_handler_detail.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/access_control/access_control_manager.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TPodSetTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    TPodSetTypeHandler(NMaster::TBootstrap* bootstrap, TPodSetTypeHandlerConfigPtr config)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::PodSet)
        , Config_(std::move(config))
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("antiaffinity_constraints")
                    ->SetAttribute(TPodSet::TSpec::AntiaffinityConstraintsSchema)
                    ->SetUpdatable(),

                MakeAttributeSchema("node_segment_id")
                    ->SetAttribute(TPodSet::TSpec::NodeSegmentSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetValidator<TPodSet>(std::bind(&TPodSetTypeHandler::ValidateNodeSegment, this, _1, _2)),

                MakeAttributeSchema("account_id")
                    ->SetAttribute(TPodSet::TSpec::AccountSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetUpdateHandler<TPodSet>(std::bind(&TPodSetTypeHandler::OnAccountUpdated, this, _1, _2))
                    ->SetValidator<TPodSet>(std::bind(&TPodSetTypeHandler::ValidateAccount, this, _1, _2)),

                MakeAttributeSchema("pod_disruption_budget_id")
                    ->SetAttribute(TPodSet::TSpec::PodDisruptionBudgetSchema)
                    ->SetUpdatable()
                    ->SetUpdateHandler<TPodSet>(
                        std::bind(&TPodSetTypeHandler::OnPodDisruptionBudgetUpdated, this, _1, _2)),

                MakeAttributeSchema("node_filter")
                    ->SetAttribute(TPodSet::TSpec::NodeFilterSchema)
                    ->SetUpdatable(),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TPodSet::TSpec::EtcSchema)
                    ->SetUpdatable()
                    ->SetValidator<TPodSet>(std::bind(&TPodSetTypeHandler::ValidateEtc, this, _1, _2)),
            });

        StatusAttributeSchema_
            ->SetComposite();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TPodSet>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &PodSetsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &PodSetsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TPodSet(id, this, session));
    }

private:
    const TPodSetTypeHandlerConfigPtr Config_;

    virtual std::vector<EAccessControlPermission> GetDefaultPermissions() override
    {
        auto result = TObjectTypeHandlerBase::GetDefaultPermissions();
        result.push_back(EAccessControlPermission::SshAccess);
        result.push_back(EAccessControlPermission::RootSshAccess);
        result.push_back(EAccessControlPermission::ReadSecrets);
        return result;
    }

    virtual void BeforeObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* podSet = object->As<TPodSet>();
        auto* tmpAccount = transaction->GetAccount(TmpAccountId);

        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(tmpAccount, EAccessControlPermission::Use);

        tmpAccount->PodSets().Add(podSet);

        auto* defaultNodeSegment = transaction->GetNodeSegment(DefaultNodeSegmentId);
        defaultNodeSegment->PodSets().Add(podSet);
    }

    virtual void AfterObjectRemoved(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectRemoved(transaction, object);

        auto* podSet = object->As<TPodSet>();

        auto* podDisruptionBudget = podSet->Spec().PodDisruptionBudget().Load();
        if (podDisruptionBudget) {
            podDisruptionBudget->FreezeUntilSync(
                Format("Pod disruption budget is frozen until the next synchronization "
                       "due to remove of budgeting pod set %Qv",
                    podSet->GetId()));
        }
    }

    void ValidateAccount(TTransaction* /*transaction*/, TPodSet* podSet)
    {
        auto* account = podSet->Spec().Account().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(account, EAccessControlPermission::Use);
    }

    void ValidateEtc(TTransaction* /*transaction*/, TPodSet* podSet)
    {
        const auto& podSetSpecEtcOld = podSet->Spec().Etc().LoadOld();
        const auto& podSetSpecEtc = podSet->Spec().Etc().Load();

        if (!podSetSpecEtcOld.violate_node_segment_constraints().vcpu_guarantee_to_limit_ratio() &&
            podSetSpecEtc.violate_node_segment_constraints().vcpu_guarantee_to_limit_ratio())
        {
            auto* nodeSegment = podSet->Spec().NodeSegment().Load();
            const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
            accessControlManager->ValidatePermission(
                nodeSegment,
                EAccessControlPermission::Use,
                "/access/pod_constraints/violate/vcpu_guarantee_to_limit_ratio");
        }
    }

    void ValidateNodeSegment(TTransaction* /*transaction*/, TPodSet* podSet)
    {
        if (!Config_->AllowNodeSegmentChange && podSet->Spec().NodeSegment().LoadOld() && podSet->Spec().NodeSegment().IsChanged()) {
            THROW_ERROR_EXCEPTION("Changing of node segment is not allowed");
        }
    }

    void OnAccountUpdated(TTransaction* transaction, TPodSet* podSet)
    {
        for (auto* pod : podSet->Pods().Load()) {
            transaction->ScheduleValidateAccounting(pod);
        }
    }

    void OnPodDisruptionBudgetUpdated(TTransaction* /*transaction*/, TPodSet* podSet)
    {
        auto process = [podSet] (TPodDisruptionBudget* podDisruptionBudget) {
            if (podDisruptionBudget) {
                podDisruptionBudget->FreezeUntilSync(
                    Format("Pod disruption budget is frozen until the next synchronization "
                           "due to update of budgeting pod set %Qv",
                        podSet->GetId()));
            }
        };
        process(podSet->Spec().PodDisruptionBudget().LoadOld());
        process(podSet->Spec().PodDisruptionBudget().Load());
    }
};

std::unique_ptr<IObjectTypeHandler> CreatePodSetTypeHandler(NMaster::TBootstrap* bootstrap, TPodSetTypeHandlerConfigPtr config)
{
    return std::unique_ptr<IObjectTypeHandler>(new TPodSetTypeHandler(bootstrap, std::move(config)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

