#ifndef MUTATION_INL_H_
#error "Direct inclusion of this file is not allowed, include mutation.h"
#endif

#include "hydra_manager.h"

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/message.h>
#include <yt/core/rpc/helpers.h>

#include <yt/ytlib/hydra/hydra_manager.pb.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request)
{
    return New<TMutation>(std::move(hydraManager))
        ->SetRequestData(SerializeToProtoWithEnvelope(request), request.GetTypeName());
}

template <class TRequest, class TTarget>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request,
    void (TTarget::* handler)(TRequest*),
    TTarget* target)
{
    auto requestHolder = std::make_shared<TRequest>(request);
    return CreateMutation(std::move(hydraManager), request)
        ->SetHandler(BIND([=, requestHolder = std::move(requestHolder)] (TMutationContext* mutationContext) mutable {
            auto& mutationResponse = mutationContext->Response();
            try {
                (target->*handler)(requestHolder.get());
                static auto cachedResponseMessage = NRpc::CreateResponseMessage(NProto::TVoidMutationResponse());
                mutationResponse.Data = cachedResponseMessage;
            } catch (const std::exception& ex) {
                mutationResponse.Data = NRpc::CreateErrorResponseMessage(ex);
            }
        }));
}

template <class TRequest, class TResponse>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context)
{
    return New<TMutation>(std::move(hydraManager))
        ->SetRequestData(context->GetRequestBody(), context->Request().GetTypeName())
        ->SetMutationId(NRpc::GetMutationId(context), context->IsRetry());
}

template <class TRequest, class TResponse, class TTarget>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context,
    void (TTarget::* handler)(TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>, TRequest*, TResponse*),
    TTarget* target)
{
    return CreateMutation(std::move(hydraManager), context)
        ->SetHandler(
            BIND([=] (TMutationContext* mutationContext) {
                auto& mutationResponse = mutationContext->Response();
                try {
                    TResponse response;
                    (target->*handler)(context, &context->Request(), &response);
                    mutationResponse.Data = NRpc::CreateResponseMessage(response);
                } catch (const std::exception& ex) {
                    mutationResponse.Data = NRpc::CreateErrorResponseMessage(ex);
                }
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
