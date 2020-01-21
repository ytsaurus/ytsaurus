#include "deploy_ticket_type_handler.h"

#include "db_schema.h"
#include "deploy_ticket.h"
#include "release.h"
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

class TDeployTicketTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TDeployTicketTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::DeployTicket)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("stage_id")
                    ->SetAttribute(TDeployTicket::TSpec::StageSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetMandatory()
                    ->SetValidator<TDeployTicket>(std::bind(&TDeployTicketTypeHandler::ValidateStage, this, _1, _2)),

                MakeAttributeSchema("release_id")
                    ->SetAttribute(TDeployTicket::TSpec::ReleaseSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetMandatory(),

                MakeAttributeSchema("release_rule_id")
                    ->SetAttribute(TDeployTicket::TSpec::ReleaseRuleSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetMandatory(),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TDeployTicket::TSpec::EtcSchema)
                    ->SetUpdatable()
            })
            ->SetValidator<TDeployTicket>(ValidatePatches)
            ->SetUpdatable();

        StatusAttributeSchema_
            ->SetAttribute(TDeployTicket::StatusSchema)
            ->SetUpdatable();

        IdAttributeSchema_
            ->SetValidator<TDeployTicket>(ValidateId);
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TDeployTicket>();
    }

    virtual const TDBField* GetIdField() override
    {
        return &DeployTicketsTable.Fields.Meta_Id;
    }

    virtual const TDBTable* GetTable() override
    {
        return &DeployTicketsTable;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TDeployTicket>(id, this, session);
    }

private:
    static void ValidateDeployTicketAndDeployPatchId(const TObjectId& id, const TString& description)
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
    static void ValidateId(TTransaction* /*transaction*/, TDeployTicket* deployTicket)
    {
        ValidateDeployTicketAndDeployPatchId(deployTicket->GetId(), "Deploy ticket id");
    }

    void ValidateStage(TTransaction* /*transaction*/, TDeployTicket* deployTicket)
    {
        auto* stage = deployTicket->Spec().Stage().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(stage, EAccessControlPermission::Write);
    }

    static void ValidatePatches(TTransaction* /*transaction*/, TDeployTicket* deployTicket)
    {
        const auto& spec = deployTicket->Spec().Etc().Load();
        if (spec.patches_size() == 0) {
            THROW_ERROR_EXCEPTION("Deploy patches map cannot be empty");
        }
        for (const auto& idAndDeployPatch : spec.patches()) {
            ValidateDeployTicketAndDeployPatchId(idAndDeployPatch.first, "Deploy patch id");
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreateDeployTicketTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TDeployTicketTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

