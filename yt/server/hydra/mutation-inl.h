#ifndef MUTATION_INL_H_
#error "Direct inclusion of this file is not allowed, include mutation.h"
#endif

#include "hydra_manager.h"

#include <core/misc/protobuf_helpers.h>

#include <core/rpc/message.h>

#include <ytlib/hydra/hydra_manager.pb.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class TResponse>
struct TMutationActionTraits
{
    static void Run(
        TCallback<TResponse(const TRequest& request)> handler,
        TMutationContext* context)
    {
        TRequest request;
        YCHECK(DeserializeFromProtoWithEnvelope(&request, context->Request().Data));

        TSharedRefArray responseMessage;
        try {
            auto response = handler.Run(request);
            responseMessage = NRpc::CreateResponseMessage(response);
        } catch (const std::exception& ex) {
            responseMessage = NRpc::CreateErrorResponseMessage(ex);
        }

        context->Response().Data = std::move(responseMessage);
    }
};

template <class TRequest>
struct TMutationActionTraits<TRequest, void>
{
    static void Run(
        TCallback<void(const TRequest& request)> handler,
        TMutationContext* context)
    {
        TRequest request;
        YCHECK(DeserializeFromProtoWithEnvelope(&request, context->Request().Data));

        TSharedRefArray responseMessage;
        try {
            handler.Run(request);
            static auto cachedResponseMessage = NRpc::CreateResponseMessage(NProto::TVoidMutationResponse());
            responseMessage = cachedResponseMessage;
        } catch (const std::exception& ex) {
            responseMessage = NRpc::CreateErrorResponseMessage(ex);
        }

        context->Response().Data = responseMessage;
    }
};

template <class TResponse>
struct TMutationActionTraits<void, TResponse>
{
    static void Run(
        TCallback<TResponse()> handler,
        TMutationContext* context)
    {
        TSharedRefArray responseMessage;
        try {
            auto response = handler.Run();
            responseMessage = NRpc::CreateResponseMessage(response);
        } catch (const std::exception& ex) {
            responseMessage = NRpc::CreateErrorResponseMessage(ex);
        }

        context->Response().Data = std::move(responseMessage);
    }
};

template <>
struct TMutationActionTraits<void, void>
{
    static void Run(
        TCallback<void()> handler,
        TMutationContext* context)
    {
        try {
            handler.Run();
        } catch (const std::exception& ex) {
            context->Response().Data = NRpc::CreateErrorResponseMessage(ex);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TResponse>
TMutationPtr TMutation::SetAction(TCallback<TResponse()> action)
{
    auto wrappedAction = BIND(&TMutationActionTraits<void, TResponse>::Run, std::move(action));
    return SetAction(std::move(wrappedAction));
}

template <class TRequest>
TMutationPtr TMutation::SetRequestData(const TRequest& request)
{
    TSharedRef requestData;
    YCHECK(SerializeToProtoWithEnvelope(request, &requestData));
    Request_.Data = std::move(requestData);
    Request_.Type = request.GetTypeName();
    return this;
}

template <class TTarget, class TRequest, class TResponse>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request,
    TTarget* target,
    TResponse (TTarget::* method)(const TRequest& request))
{
    auto handler = BIND(method, Unretained(target), request);
    auto wrappedHandler = BIND(&TMutationActionTraits<void, TResponse>::Run, std::move(handler));

    return New<TMutation>(std::move(hydraManager))
        ->SetAction(std::move(wrappedHandler))
        ->SetRequestData(request);
}

template <class TRequest>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request)
{
    return New<TMutation>(std::move(hydraManager))
        ->SetRequestData(request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
