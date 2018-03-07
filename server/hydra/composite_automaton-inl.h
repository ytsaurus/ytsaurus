#pragma once
#ifndef COMPOSITE_AUTOMATON_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_automaton.h"
#endif

#include "mutation.h"

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

inline void TEntitySerializationKey::Save(TSaveContext& context) const
{
    NYT::Save(context, Index);
}

inline void TEntitySerializationKey::Load(TLoadContext& context)
{
    NYT::Load(context, Index);
}

////////////////////////////////////////////////////////////////////////////////

inline TEntitySerializationKey TSaveContext::GenerateSerializationKey()
{
    return TEntitySerializationKey(SerializationKeyIndex_++);
}

////////////////////////////////////////////////////////////////////////////////

inline TEntitySerializationKey TLoadContext::RegisterEntity(TEntityBase* entity)
{
    auto key = TEntitySerializationKey{static_cast<int>(Entities_.size())};
    Entities_.push_back(entity);
    return key;
}

template <class T>
T* TLoadContext::GetEntity(TEntitySerializationKey key) const
{
    Y_ASSERT(key.Index >= 0 && key.Index < Entities_.size());
    return static_cast<T*>(Entities_[key.Index]);
}

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
        BIND([=] (TSaveContext& context) {
            saver.Run(dynamic_cast<TContext&>(context));
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
        BIND([=] () {
            auto continuation = callback.Run();
            return BIND([=] (TSaveContext& context) {
                return continuation.Run(dynamic_cast<TContext&>(context));
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
            loader.Run(dynamic_cast<TContext&>(context));
        }));
}

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
void TCompositeAutomatonPart::RegisterMethod(
    TCallback<void(TRequest*)> callback)
{
    RegisterMethod(
        TRequest::default_instance().GetTypeName(),
        BIND([=] (TMutationContext* context) {
            TRequest request;
            DeserializeProtoWithEnvelope(&request, context->Request().Data);

            auto& mutationResponse = context->Response();

            try {
                callback.Run(&request);
                static auto cachedResponseMessage = NRpc::CreateResponseMessage(NProto::TVoidMutationResponse());
                mutationResponse.Data = cachedResponseMessage;
            } catch (const std::exception& ex) {
                auto error = TError(ex);
                LogHandlerError(error);
                mutationResponse.Data = NRpc::CreateErrorResponseMessage(error);
            }
        }));
}

template <class TRpcRequest, class TRpcResponse, class THandlerRequest, class THandlerResponse>
void TCompositeAutomatonPart::RegisterMethod(
    TCallback<void(const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TRpcResponse>>&, THandlerRequest*, THandlerResponse*)> callback)
{
    RegisterMethod(
        THandlerRequest::default_instance().GetTypeName(),
        BIND([=] (TMutationContext* context) {
            THandlerRequest request;
            DeserializeProtoWithEnvelope(&request, context->Request().Data);

            auto& mutationResponse = context->Response();

            try {
                THandlerResponse response;
                callback.Run(nullptr, &request, &response);
                mutationResponse.Data = NRpc::CreateResponseMessage(response);
            } catch (const std::exception& ex) {
                auto error = TError(ex);
                LogHandlerError(error);
                mutationResponse.Data = NRpc::CreateErrorResponseMessage(error);
            }
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
