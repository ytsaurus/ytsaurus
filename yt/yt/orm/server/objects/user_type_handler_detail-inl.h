#ifndef USER_TYPE_HANDLER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include user_type_handler_detail.h"
// For the sake of sane code completion.
#include "user_type_handler_detail.h"
#endif

#include "subject_type_handler_detail.h"

#include <yt/yt/orm/server/access_control/public.h>
#include <yt/yt/orm/server/master/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<TObject> TUser, std::derived_from<IObjectTypeHandler> TGeneratedUserTypeHandler>
class TUserTypeHandlerBase
    : public TSubjectTypeHandlerBase<TGeneratedUserTypeHandler>
{
    using TBase = TSubjectTypeHandlerBase<TGeneratedUserTypeHandler>;

public:
    using TBase::TBase;

    virtual void Initialize() override
    {
        TBase::Initialize();

        TBase::SpecAttributeSchema_
            ->template AddValidator<TUser>(std::bind_front(&TUserTypeHandlerBase::ValidateSpec, this));
    }

    virtual std::vector<NAccessControl::TAccessControlEntry> GetDefaultAcl() override
    {
        return {};
    }

    virtual bool IsBuiltin(const TObject* object) const override
    {
        return object->template As<TUser>()->GetId() == NRpc::RootUserName;
    }

private:
    void ValidateSpec(TTransaction* /*transaction*/, const TUser* user)
    {
        const auto& userSpecNew = user->Spec().Etc().Load();
        const auto& userSpecOld = user->Spec().Etc().LoadOld();

        auto checkBound = [] (auto newValue, const TString& field) {
            constexpr int lowerBound = 0;
            constexpr int upperBound = 100'000;

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

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<TObject> TUser, std::derived_from<IObjectTypeHandler> TGeneratedUserTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateUserTypeHandler(NMaster::IBootstrap* bootstrap, TObjectManagerConfigPtr config)
{
    return std::make_unique<TUserTypeHandlerBase<TUser, TGeneratedUserTypeHandler>>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
