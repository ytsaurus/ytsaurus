#pragma once
#ifndef MUTATION_INL_H_
#error "Direct inclusion of this file is not allowed, include mutation.h"
// For the sake of sane code completion
#include "mutation.h"
#endif

#include "hydra_manager.h"

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/object_pool.h>

#include <yt/core/rpc/message.h>
#include <yt/core/rpc/helpers.h>

#include <yt/ytlib/hydra/hydra_manager.pb.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
std::unique_ptr<TMutation> CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request)
{
    auto mutation = std::make_unique<TMutation>(std::move(hydraManager));
    mutation->SetRequestData(SerializeProtoToRefWithEnvelope(request), request.GetTypeName());
    return mutation;
}

template <class TRequest, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request,
    void (TTarget::* handler)(TRequest*),
    TTarget* target)
{
    auto mutation = CreateMutation(std::move(hydraManager), request);
    mutation->SetHandler(
        BIND([=, request = request] (TMutationContext* mutationContext) mutable {
            auto& mutationResponse = mutationContext->Response();
            try {
                (target->*handler)(&request);
                static auto cachedResponseMessage = NRpc::CreateResponseMessage(NProto::TVoidMutationResponse());
                mutationResponse.Data = cachedResponseMessage;
            } catch (const std::exception& ex) {
                mutationResponse.Data = NRpc::CreateErrorResponseMessage(ex);
            }
        }));
    return mutation;
}

template <class TRequest, class TResponse>
std::unique_ptr<TMutation> CreateMutation(
    IHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context)
{
    auto mutation = std::make_unique<TMutation>(std::move(hydraManager));
    mutation->SetRequestData(context->GetRequestBody(), context->Request().GetTypeName());
    mutation->SetMutationId(context->GetMutationId(), context->IsRetry());
    return mutation;
}

template <class TRequest, class TResponse, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    IHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context,
    void (TTarget::* handler)(const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>&, TRequest*, TResponse*),
    TTarget* target)
{
    auto mutation = CreateMutation(std::move(hydraManager), context);
    mutation->SetHandler(
        BIND([=] (TMutationContext* mutationContext) {
            auto& mutationResponse = mutationContext->Response();
            auto response = ObjectPool<TResponse>().Allocate();
            try {
                (target->*handler)(context, &context->Request(), response.get());
                mutationResponse.Data = NRpc::CreateResponseMessage(*response);
            } catch (const std::exception& ex) {
                mutationResponse.Data = NRpc::CreateErrorResponseMessage(ex);
            }
        }));
    return mutation;
}

template <class TRpcRequest, class TResponse, class THandlerRequest, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    IHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TResponse>>& context,
    const THandlerRequest& request,
    void (TTarget::* handler)(const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TResponse>>&, THandlerRequest*, TResponse*),
    TTarget* target)
{
    auto mutation = std::make_unique<TMutation>(std::move(hydraManager));
    mutation->SetRequestData(SerializeProtoToRefWithEnvelope(request), request.GetTypeName());
    mutation->SetMutationId(context->GetMutationId(), context->IsRetry());
    mutation->SetHandler(
        BIND([=, request = request] (TMutationContext* mutationContext) mutable {
            auto& mutationResponse = mutationContext->Response();
            auto response = ObjectPool<TResponse>().Allocate();
            try {
                (target->*handler)(context, &request, response.get());
                mutationResponse.Data = NRpc::CreateResponseMessage(*response);
            } catch (const std::exception& ex) {
                mutationResponse.Data = NRpc::CreateErrorResponseMessage(ex);
            }
        }));
    return mutation;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
