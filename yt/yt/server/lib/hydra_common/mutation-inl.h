#ifndef MUTATION_INL_H_
#error "Direct inclusion of this file is not allowed, include mutation.h"
// For the sake of sane code completion.
#include "mutation.h"
#endif

#include "hydra_manager.h"

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/object_pool.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
    const TRequest& request)
{
    auto mutation = std::make_unique<TMutation>(std::move(hydraManager));
    mutation->SetRequestData(SerializeProtoToRefWithEnvelope(request), request.GetTypeName());
    return mutation;
}

template <class TRequest, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
    const TRequest& request,
    void (TTarget::* handler)(TRequest*),
    TTarget* target)
{
    auto mutation = CreateMutation(std::move(hydraManager), request);
    mutation->SetHandler(
        BIND([=, request = request] (TMutationContext* mutationContext) mutable {
            try {
                (target->*handler)(&request);
                static auto cachedResponseMessage = NRpc::CreateResponseMessage(NProto::TVoidMutationResponse());
                mutationContext->SetResponseData(cachedResponseMessage);
            } catch (const std::exception& ex) {
                mutationContext->SetResponseData(ex);
            }
        }));
    return mutation;
}

template <class TRequest, class TResponse>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context)
{
    auto mutation = std::make_unique<TMutation>(std::move(hydraManager));
    const auto& header = context->GetRequestHeader();
    // COPMAT(danilalexeev): legacy RPC codecs
    TSharedRef requestData;
    if (header.has_request_codec()) {
        YT_VERIFY(header.request_codec() == ToProto<int>(NCompression::ECodec::None));
        requestData = PushEnvelope(context->GetRequestBody());
    } else {
        requestData = context->GetRequestBody();
    }
    mutation->SetRequestData(requestData, context->Request().GetTypeName());
    mutation->SetMutationId(context->GetMutationId(), context->IsRetry());
    return mutation;
}

template <class TRequest, class TResponse, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context,
    void (TTarget::* handler)(const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>&, TRequest*, TResponse*),
    TTarget* target)
{
    auto mutation = CreateMutation(std::move(hydraManager), context);
    mutation->SetHandler(
        BIND([=] (TMutationContext* mutationContext) {
            auto response = ObjectPool<TResponse>().Allocate();
            try {
                (target->*handler)(context, &context->Request(), response.get());
                mutationContext->SetResponseData(NRpc::CreateResponseMessage(*response));
            } catch (const std::exception& ex) {
                mutationContext->SetResponseData(ex);
            }
        }));
    return mutation;
}

template <class TRequest, class TResponse, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
    TRequest* request,
    TResponse* response,
    void (TTarget::* handler)(const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>&, TRequest*, TResponse*),
    TTarget* target)
{
    auto mutation = CreateMutation(std::move(hydraManager), *request);
    mutation->SetHandler(
        BIND([=] (TMutationContext* mutationContext) {
            try {
                (target->*handler)(nullptr, request, response);
                mutationContext->SetResponseData(NRpc::CreateResponseMessage(*response));
            } catch (const std::exception& ex) {
                mutationContext->SetResponseData(ex);
            }
        }));
    return mutation;
}

template <class TRpcRequest, class TResponse, class THandlerRequest, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
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
            auto response = ObjectPool<TResponse>().Allocate();
            try {
                (target->*handler)(context, &request, response.get());
                mutationContext->SetResponseData(NRpc::CreateResponseMessage(*response));
            } catch (const std::exception& ex) {
                mutationContext->SetResponseData(ex);
            }
        }));
    return mutation;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
