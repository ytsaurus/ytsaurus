#include "user_type_handler.h"
#include "subject_type_handler_detail.h"
#include "user.h"
#include "db_schema.h"

#include <yp/server/access_control/public.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TUserTypeHandler
    : public TSubjectTypeHandlerBase
{
public:
    explicit TUserTypeHandler(NMaster::TBootstrap* bootstrap)
        : TSubjectTypeHandlerBase(bootstrap, EObjectType::User)
    { }

    virtual void Initialize() override
    {
        TSubjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->SetAttribute(TUser::SpecSchema)
            ->SetValidator<TUser>(std::bind(&TUserTypeHandler::ValidateSpec, this, _1, _2));

        StatusAttributeSchema_
            ->SetComposite();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TUser>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &UsersTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &UsersTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TUser(id, this, session));
    }

    virtual std::vector<EAccessControlPermission> GetDefaultPermissions() override
    {
        return {};
    }

private:
    void ValidateSpec(TTransaction* /*transaction*/, TUser* user)
    {
        const auto& userSpecNew = user->Spec().Load();
        const auto& userSpecOld = user->Spec().LoadOld();

        auto checkBound = [](auto newValue, const TString& field) {
            constexpr int lowerBound = 0;
            constexpr int upperBound = 100000;

            if (newValue < lowerBound || newValue > upperBound) {
                THROW_ERROR_EXCEPTION("Field %Qv value %v must be in range [%v, %v]",
                    field,
                    newValue,
                    lowerBound,
                    upperBound);
            }
        };

        auto requestQueueSizeLimit = userSpecNew.request_queue_size_limit();
        if (!user->DidExist() || userSpecOld.request_queue_size_limit() != requestQueueSizeLimit) {
            checkBound(requestQueueSizeLimit, "request_queue_size_limit");
        }

        auto requestWeightRateLimit = userSpecNew.request_weight_rate_limit();
        if (!user->DidExist() || userSpecOld.request_weight_rate_limit() != requestWeightRateLimit) {
            checkBound(requestWeightRateLimit, "request_weight_rate_limit");
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreateUserTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TUserTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

