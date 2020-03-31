#include "release_type_handler.h"

#include "db_schema.h"
#include "deploy_ticket.h"
#include "release.h"
#include "type_handler_detail.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/access_control/access_control_manager.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TReleaseTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TReleaseTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Release)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("author_id")
                    ->SetAttribute(TRelease::AuthorIdSchema)
            });

        SpecAttributeSchema_
            ->AddChildren({
                MakeEtcAttributeSchema()
                    ->SetAttribute(TRelease::TSpec::EtcSchema)
                    ->SetUpdatable()
            })
            ->SetValidator<TRelease>(ValidateSpec)
            ->SetUpdatable();

        StatusAttributeSchema_
            ->SetAttribute(TRelease::StatusSchema)
            ->SetUpdatable();

        IdAttributeSchema_
            ->SetValidator<TRelease>(ValidateId);
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TRelease>();
    }

    virtual const TDBField* GetIdField() override
    {
        return &ReleasesTable.Fields.Meta_Id;
    }

    virtual const TDBTable* GetTable() override
    {
        return &ReleasesTable;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TRelease>(id, this, session);
    }

    virtual void AfterObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectCreated(transaction, object);

        auto* release = object->As<TRelease>();

        if (!release->AuthorId().Load().empty()) {
            THROW_ERROR_EXCEPTION("Field /meta/author_id cannot be set");
        }
        auto userId = Bootstrap_->GetAccessControlManager()->TryGetAuthenticatedUser();

        if (userId.empty()) {
            THROW_ERROR_EXCEPTION("Authenticated user cannot be resolved for release %Qv",
                release->GetId());
        }

        release->AuthorId() = userId;
    }

private:
    static void ValidateId(TTransaction* /*transaction*/, TRelease* release)
    {
        const TObjectId& id = release->GetId();
        static const re2::RE2 projectIdPattern("[A-Za-z0-9-_]+");
        static const size_t idLengthLimit = 70;

        if (!re2::RE2::FullMatch(id, projectIdPattern)) {
            THROW_ERROR_EXCEPTION(NClient::NApi::EErrorCode::InvalidObjectId,
                "Release id %Qv must match regexp %Qv",
                id,
                projectIdPattern.Pattern());
        }
        if (id.length() > idLengthLimit) {
            THROW_ERROR_EXCEPTION(NClient::NApi::EErrorCode::InvalidObjectId,
                "Release id %Qv length exceeds limit %v",
                id,
                idLengthLimit);
        }
    }

    static void ValidateSandboxResource(const NClient::NApi::NProto::TSandboxResource& sandboxResource)
    {
        if (!sandboxResource.type()) {
            THROW_ERROR_EXCEPTION("Sandbox resource %Qv has no type",
                sandboxResource.resource_id());
        }
        if (!sandboxResource.skynet_id()) {
            THROW_ERROR_EXCEPTION("Sandbox resource %Qv has no skynet id",
                sandboxResource.resource_id());
        }
    }

    static void ValidateSandboxRelease(const NClient::NApi::NProto::TSandboxRelease& sandboxRelease)
    {
        if (!sandboxRelease.task_id()) {
            THROW_ERROR_EXCEPTION("Task id in sandbox release cannot be null");
        }
        if (!sandboxRelease.task_type()) {
            THROW_ERROR_EXCEPTION("Task type in sandbox release cannot be null");
        }
        if (!sandboxRelease.release_type()) {
            THROW_ERROR_EXCEPTION("Release type in sandbox release cannot be null");
        }
        if (sandboxRelease.resources_size() == 0) {
            THROW_ERROR_EXCEPTION("Resources of sandbox release cannot be empty");
        }
        for (const auto& resource : sandboxRelease.resources()) {
            ValidateSandboxResource(resource);
        }
    }

    static void ValidateDockerRelease(const NClient::NApi::NProto::TDockerRelease& dockerRelease)
    {
        if (!dockerRelease.image_name()) {
            THROW_ERROR_EXCEPTION("Image name in docker release cannot be null");
        }
        if (!dockerRelease.image_tag()) {
            THROW_ERROR_EXCEPTION("Image tag in docker release cannot be null");
        }
        if (!dockerRelease.image_hash()) {
            THROW_ERROR_EXCEPTION("Image hash in docker release cannot be null");
        }
        if (!dockerRelease.release_type()) {
            THROW_ERROR_EXCEPTION("Release type in docker release cannot be null");
        }
    }

    static void ValidateSpec(TTransaction* /*transaction*/, TRelease* release)
    {
        const auto& spec = release->Spec().Etc().Load();
        if (spec.has_sandbox()) {
            ValidateSandboxRelease(spec.sandbox());
        } else if (spec.has_docker()) {
            ValidateDockerRelease(spec.docker());
        } else {
            THROW_ERROR_EXCEPTION("Release payload must be set");
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreateReleaseTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TReleaseTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

