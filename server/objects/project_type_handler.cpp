#include "project_type_handler.h"

#include "account.h"
#include "db_schema.h"
#include "project.h"
#include "type_handler_detail.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/access_control/access_control_manager.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TProjectTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TProjectTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Project)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("owner_id")
                    ->SetAttribute(TProject::OwnerIdSchema)
                    ->SetUpdatable()
                    ->SetMandatory()
                    ->SetValidator<TProject>(std::bind(&TProjectTypeHandler::ValidateOwnerId, this, _1, _2)),
            });

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("account_id")
                    ->SetAttribute(TProject::TSpec::AccountSchema
                        .SetNullable(false))
                    ->SetMandatory()
                    ->SetValidator<TProject>(std::bind(&TProjectTypeHandler::ValidateAccount, this, _1, _2)),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TProject::TSpec::EtcSchema)
                    ->SetUpdatable()
                    ->SetValidator<TProject>(std::bind(&TProjectTypeHandler::ValidateEtc, this, _1, _2))
            })
            ->SetUpdatable();

        StatusAttributeSchema_
            ->SetAttribute(TProject::StatusSchema);

        IdAttributeSchema_
            ->SetValidator<TProject>(ValidateProjectId);
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TProject>();
    }

    virtual const TDBField* GetIdField() override
    {
        return &ProjectsTable.Fields.Meta_Id;
    }

    virtual const TDBTable* GetTable() override
    {
        return &ProjectsTable;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TProject>(id, this, session);
    }

private:
    void ValidateAccount(TTransaction* /*transaction*/, TProject* project)
    {
        TObject* account = project->Spec().Account().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(account, EAccessControlPermission::Use);
    }

    void ValidateOwnerId(TTransaction* /*transaction*/, TProject* project)
    {
        if (project->OwnerId().Load().empty()) {
            THROW_ERROR_EXCEPTION("Project owner id cannot be null");
        }
    }

    void ValidateEtc(TTransaction* /*transaction*/, TProject* project)
    {
        for (const auto& userSpecificBoxType : project->Spec().Etc().Load().user_specific_box_types()) {
            ValidateId(userSpecificBoxType, "User specific box type");
        }
    }

    static void ValidateProjectId(TTransaction* /*transaction*/, TProject* project)
    {
        ValidateId(project->GetId(), "Project id");
    }

    static void ValidateId(const TString& id, const TString& description)
    {
        static const re2::RE2 idPattern("[A-Za-z0-9-_]+");
        static const size_t idLengthLimit = 70;

        if (!re2::RE2::FullMatch(id, idPattern)) {
            THROW_ERROR_EXCEPTION(NClient::NApi::EErrorCode::InvalidObjectId,
                "%v %Qv must match regexp %Qv",
                description,
                id,
                idPattern.Pattern());
        }
        if (id.length() > idLengthLimit) {
            THROW_ERROR_EXCEPTION(NClient::NApi::EErrorCode::InvalidObjectId,
                "%v %Qv length exceeds limit %v",
                description,
                id,
                idLengthLimit);
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreateProjectTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TProjectTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
