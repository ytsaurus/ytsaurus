#ifndef COMPOSITE_AUTOMATON_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_automaton.h"
// For the sake of sane code completion.
#include "composite_automaton.h"
#endif

#include "mutation.h"

#include <yt/yt/core/misc/object_pool.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TContext>
void TCompositeAutomatonPart::RegisterSaver(
    ESyncSerializationPriority priority,
    const TString& name,
    TCallback<void(TContext&)> saver)
{
    RegisterSaver(
        priority,
        name,
        BIND_NO_PROPAGATE([=] (TSaveContext& context) {
            saver(dynamic_cast<TContext&>(context));
        }));
}

template <class TContext>
void TCompositeAutomatonPart::RegisterSaver(
    EAsyncSerializationPriority priority,
    const TString& name,
    TCallback<TCallback<void(TContext&)>()> callback)
{
    RegisterSaver(
        priority,
        name,
        BIND_NO_PROPAGATE([=] () -> TCallback<void(TSaveContext&)> {
            auto continuation = callback();
            return BIND_NO_PROPAGATE([=] (TSaveContext& context) {
                return continuation(dynamic_cast<TContext&>(context));
            });
        }));
}

template <class TContext>
void TCompositeAutomatonPart::RegisterLoader(
    const TString& name,
    TCallback<void(TContext&)> loader)
{
    TCompositeAutomatonPart::RegisterLoader(
        name,
        BIND([=] (TLoadContext& context) {
            loader(dynamic_cast<TContext&>(context));
        }));
}

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
void TCompositeAutomatonPart::RegisterMethod(
    TCallback<void(TRequest*)> callback,
    const std::vector<TString>& aliases)
{
    Automaton_->RegisterMethod(callback, aliases);
}

template <class TRpcRequest, class TRpcResponse, class THandlerRequest, class THandlerResponse>
void TCompositeAutomatonPart::RegisterMethod(
    TCallback<void(const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TRpcResponse>>&, THandlerRequest*, THandlerResponse*)> callback,
    const std::vector<TString>& aliases)
{
    Automaton_->RegisterMethod(callback, aliases);
}

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
void TCompositeAutomaton::RegisterMethod(
    TCallback<void(TRequest*)> callback,
    const std::vector<TString>& aliases)
{
    auto mutationHandler = BIND([=, this] (TMutationContext* context) {
        auto request = ObjectPool<TRequest>().Allocate();
        auto* descriptor = GetMethodDescriptor(TRequest::default_instance().GetTypeName());
        DeserializeRequestAndProfile(
            request.get(),
            context->Request().Data,
            descriptor);

        NProfiling::TWallTimer timer;

        try {
            callback(request.get());
            static auto cachedResponseMessage = NRpc::CreateResponseMessage(NProto::TVoidMutationResponse());
            context->SetResponseData(cachedResponseMessage);
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            LogHandlerError(error);
            context->SetResponseData(error);
        }
        descriptor->CumulativeExecuteTimeCounter.Add(timer.GetElapsedTime());
    });

    auto mutationName = TRequest::default_instance().GetTypeName();
    RegisterMethod(mutationName, mutationHandler);
    for (const auto& alias : aliases) {
        RegisterMethod(alias, mutationHandler);
    }
}

template <class TRpcRequest, class TRpcResponse, class THandlerRequest, class THandlerResponse>
void TCompositeAutomaton::RegisterMethod(
    TCallback<void(const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TRpcResponse>>&, THandlerRequest*, THandlerResponse*)> callback,
    const std::vector<TString>& aliases)
{
    auto mutationHandler = BIND_NO_PROPAGATE([=, this] (TMutationContext* context) {
        auto request = ObjectPool<THandlerRequest>().Allocate();
        auto response = ObjectPool<THandlerResponse>().Allocate();

        auto* descriptor = GetMethodDescriptor(THandlerRequest::default_instance().GetTypeName());
        DeserializeRequestAndProfile(
            request.get(),
            context->Request().Data,
            descriptor);

        NProfiling::TWallTimer timer;
        try {
            callback(nullptr, request.get(), response.get());
            context->SetResponseData(NRpc::CreateResponseMessage(*response));
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            LogHandlerError(error);
            context->SetResponseData(error);
        }
        descriptor->CumulativeExecuteTimeCounter.Add(timer.GetElapsedTime());
    });

    auto mutationName = THandlerRequest::default_instance().GetTypeName();
    RegisterMethod(mutationName, mutationHandler);
    for (const auto& alias : aliases) {
        RegisterMethod(alias, mutationHandler);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
