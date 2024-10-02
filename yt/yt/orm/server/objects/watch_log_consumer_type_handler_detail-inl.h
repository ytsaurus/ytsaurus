#ifndef WATCH_LOG_CONSUMER_TYPE_HANDLER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include watch_log_consumer_type_handler_detail.h"
// For the sake of sane code completion.
#include "watch_log_consumer_type_handler_detail.h"
#endif

#include "continuation.h"
#include "type_handler_detail.h"
#include "watch_manager.h"

#include <yt/yt/orm/server/objects/proto/continuation_token.pb.h>

#include <yt/yt/orm/server/master/bootstrap.h>

#include <yt/yt/orm/client/objects/registry.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <
    std::derived_from<TObject> TWatchLogConsumer,
    std::derived_from<IObjectTypeHandler> TGeneratedWatchLogConsumerTypeHandler>
class TWatchLogConsumerTypeHandlerBase
    : public TGeneratedWatchLogConsumerTypeHandler
{
    using TBase = TGeneratedWatchLogConsumerTypeHandler;

public:
    using TBase::TBase;

    virtual void Initialize() override
    {
        TBase::Initialize();

        TBase::SpecAttributeSchema_
            ->template AddValidator<TWatchLogConsumer>(
                std::bind_front(&TWatchLogConsumerTypeHandlerBase::ValidateSpec, this));

        TBase::StatusAttributeSchema_
            ->template AddValidator<TWatchLogConsumer>(
                std::bind_front(&TWatchLogConsumerTypeHandlerBase::ValidateStatus, this));
    }

    virtual void ValidateCreatedObject(
        NServer::NObjects::TTransaction* /*transaction*/,
        NServer::NObjects::TObject* object) override
    {
        const auto& spec = object->As<TWatchLogConsumer>()->Spec().Etc().Load();
        THROW_ERROR_EXCEPTION_UNLESS(spec.object_type(),
            "Attribute /spec/object_type must be set");
    }

private:
    void ValidateWatchLogsConfiguredFor(const TString& objectType)
    {
        if (TBase::GetBootstrap()
            ->GetWatchManager()
            ->GetLogs(NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetTypeValueByNameOrThrow(objectType))
            .empty())
        {
            THROW_ERROR_EXCEPTION("Object type %Qv has no configured watch logs",
                objectType);
        }
    }

    void ValidateSpec(TTransaction* /*transaction*/, const TWatchLogConsumer* watchLogConsumer)
    {
        const auto& spec = watchLogConsumer->Spec().Etc().Load();
        const auto& specOld = watchLogConsumer->Spec().Etc().LoadOld();
        if (!watchLogConsumer->DidExist()) {
            if (spec.object_type()) {
                ValidateWatchLogsConfiguredFor(spec.object_type());
            }
        } else {
            if (specOld.object_type()) {
                if (specOld.object_type() != spec.object_type()) {
                    THROW_ERROR_EXCEPTION("Attribute /spec/object_type cannot be changed");
                }
            } else if (spec.object_type()) {
                ValidateWatchLogsConfiguredFor(spec.object_type());
            }
        }
    }

    void ValidateStatus(TTransaction* /*transaction*/, const TWatchLogConsumer* watchLogConsumer)
    {
        const auto& spec = watchLogConsumer->Spec().Etc().Load();
        const auto& status = watchLogConsumer->Status().Etc().Load();
        const auto& statusOld = watchLogConsumer->Status().Etc().LoadOld();
        if (status.continuation_token() && status.continuation_token() != statusOld.continuation_token()) {
            NProto::TWatchQueryContinuationToken continuationToken;

            auto objectType = NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetTypeValueByNameOrThrow(spec.object_type());
            DeserializeContinuationToken(status.continuation_token(), &continuationToken);
            THROW_ERROR_EXCEPTION_UNLESS(TObjectTypeHandlerBase::GetBootstrap()->GetWatchManager()
                ->IsLogRegistered(objectType, continuationToken.log_name()),
                NClient::EErrorCode::InvalidContinuationToken,
                "Continuation token of log consumer %Qv contains nonexistent watch log with name %Qv",
                watchLogConsumer->GetKey(),
                continuationToken.log_name());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <
    std::derived_from<TObject> TWatchLogConsumer,
    std::derived_from<IObjectTypeHandler> TGeneratedWatchLogConsumerTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateWatchLogConsumerTypeHandler(
    NMaster::IBootstrap* bootstrap,
    TObjectManagerConfigPtr config)
{
    return std::make_unique<TWatchLogConsumerTypeHandlerBase<TWatchLogConsumer, TGeneratedWatchLogConsumerTypeHandler>>(
        bootstrap,
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
