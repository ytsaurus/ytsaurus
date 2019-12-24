#include "release_rule_type_handler.h"

#include "db_schema.h"
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
            ->SetValidator<TReleaseRule>(ValidateSelectorAndPatchPolicy)
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
    static void ValidateId(TTransaction* /*transaction*/, TReleaseRule* releaseRule)
    {
        const TObjectId& id = releaseRule->GetId();
        static const TString description = "Release rule id";
        static const re2::RE2 releaseRuleIdPattern("[A-Za-z0-9-_]+");
        static const size_t idLengthLimit = 70;

        if (!re2::RE2::FullMatch(id, releaseRuleIdPattern)) {
            THROW_ERROR_EXCEPTION(NClient::NApi::EErrorCode::InvalidObjectId,
                "%v %Qv must match regexp %Qv",
                description,
                id,
                releaseRuleIdPattern.Pattern());
        }
        if (id.length() > idLengthLimit) {
            THROW_ERROR_EXCEPTION(NClient::NApi::EErrorCode::InvalidObjectId,
                "%v %Qv length exceeds limit %v",
                description,
                id,
                idLengthLimit);
        }
    }

    void ValidateStage(TTransaction* /*transaction*/, TReleaseRule* releaseRule)
    {
        auto* stage = releaseRule->Spec().Stage().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(stage, EAccessControlPermission::Write);
    }

    static void ValidateSelectorAndPatchPolicy(TTransaction* /*transaction*/, TReleaseRule* releaseRule)
    {
        const auto& spec = releaseRule->Spec().Etc().Load();
        if (spec.has_sandbox()) {
            if (!spec.has_static_sandbox_resource() && !spec.has_dynamic_sandbox_resource()) {
                THROW_ERROR_EXCEPTION("Sandbox selector can be used only with static_sandbox_resource or dynamic_sandbox_resource patch policy");
            }

            const auto& sbSelector = spec.sandbox();
            if (!sbSelector.task_type() && sbSelector.resource_types_size() == 0) {
                THROW_ERROR_EXCEPTION("At least one of task_type or resource_types must be set when using sandbox selector");
            }
        }
        if (spec.has_docker()) {
            if (!spec.has_docker_image()) {
                THROW_ERROR_EXCEPTION("Docker selector can be used only with docker_image patch policy");
            }

            const auto& dSelector = spec.docker();
            if (!dSelector.image_name()) {
                THROW_ERROR_EXCEPTION("Image name must be set when using docker selector");
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

