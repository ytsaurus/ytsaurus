#include "release_rule_type_handler.h"

#include "db_schema.h"
#include "deploy_ticket.h"
#include "release_rule.h"
#include "stage.h"
#include "type_handler_detail.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/access_control/access_control_manager.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TReleaseRuleTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TReleaseRuleTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::ReleaseRule)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("stage_id")
                    ->SetAttribute(TReleaseRule::TSpec::StageSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetMandatory()
                    ->SetValidator<TReleaseRule>(std::bind(&TReleaseRuleTypeHandler::ValidateStage, this, _1, _2)),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TReleaseRule::TSpec::EtcSchema)
                    ->SetUpdatable()
            })
            ->SetValidator<TReleaseRule>(ValidateSelectorAndPatches)
            ->SetUpdatable();

        StatusAttributeSchema_
            ->SetAttribute(TReleaseRule::StatusSchema)
            ->SetUpdatable();

        IdAttributeSchema_
            ->SetValidator<TReleaseRule>(ValidateId);
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TReleaseRule>();
    }

    virtual const TDBField* GetIdField() override
    {
        return &ReleaseRulesTable.Fields.Meta_Id;
    }

    virtual const TDBTable* GetTable() override
    {
        return &ReleaseRulesTable;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TReleaseRule>(id, this, session);
    }

private:
    static void ValidateReleaseRuleAndDeployPatchId(const TObjectId& id, const TString& description)
    {
        static const re2::RE2 idPattern("[A-Za-z0-9-_]+");
        static const size_t idLengthLimit = 70;
        if (!re2::RE2::FullMatch(id, idPattern)) {
            THROW_ERROR_EXCEPTION("%v %Qv must match regexp %Qv",
                description,
                id,
                idPattern.Pattern());
        }
        if (id.length() > idLengthLimit) {
            THROW_ERROR_EXCEPTION("%v %Qv length exceeds limit %lu",
                description,
                id,
                idLengthLimit);
        }
    }

    static void ValidateId(TTransaction* /*transaction*/, TReleaseRule* releaseRule)
    {
        ValidateReleaseRuleAndDeployPatchId(releaseRule->GetId(), "Release rule id");
    }

    void ValidateStage(TTransaction* /*transaction*/, TReleaseRule* releaseRule)
    {
        auto* stage = releaseRule->Spec().Stage().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(stage, EAccessControlPermission::Write);
    }

    static void ValidateSandboxSelector(const NYP::NClient::NApi::NProto::TSandboxSelector& selector, const NYP::NClient::NApi::NProto::TReleaseRuleSpec::ESelectorSource& selectorSource)
    {
        if (selectorSource == NYP::NClient::NApi::NProto::TReleaseRuleSpec::CUSTOM) {
            if (!selector.task_type() && selector.resource_types_size() == 0) {
                THROW_ERROR_EXCEPTION("At least one of task_type or resource_types must be set when using custom sandbox selector");
            }
        }
    }

    static void ValidateDockerSelector(const NYP::NClient::NApi::NProto::TDockerSelector& selector, const NYP::NClient::NApi::NProto::TReleaseRuleSpec::ESelectorSource& selectorSource)
    {
        if (selectorSource == NYP::NClient::NApi::NProto::TReleaseRuleSpec::CUSTOM) {
            if (!selector.image_name()) {
                THROW_ERROR_EXCEPTION("Image name must be set when using custom docker selector");
            }
        }
    }

    static void ValidateSelectorAndPatches(TTransaction* /*transaction*/, TReleaseRule* releaseRule)
    {
        const auto& spec = releaseRule->Spec().Etc().Load();

        if (spec.has_sandbox()) {
            ValidateSandboxSelector(spec.sandbox(), spec.selector_source());
        } else if (spec.has_docker()) {
            ValidateDockerSelector(spec.docker(), spec.selector_source());
        } else {
            THROW_ERROR_EXCEPTION("Selector must be set");
        }

        if (spec.patches_size() == 0) {
            THROW_ERROR_EXCEPTION("Deploy patches map cannot be empty");
        }
        for (const auto& [deployPatchId, deployPatch] : spec.patches()) {
            ValidateReleaseRuleAndDeployPatchId(deployPatchId, "Deploy patch id");

            if (spec.has_sandbox() && !deployPatch.has_sandbox()) {
                THROW_ERROR_EXCEPTION("Deploy patch %Qv is not sandbox resource patch",
                    deployPatchId);
            }
            if (spec.has_docker() && !deployPatch.has_docker()) {
                THROW_ERROR_EXCEPTION("Deploy patch %Qv is not docker image patch",
                    deployPatchId);
            }
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreateReleaseRuleTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TReleaseRuleTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

