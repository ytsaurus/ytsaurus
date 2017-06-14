#pragma once

#include "public.h"
#include "mutation_context.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/ref.h>

#include <yt/core/rpc/public.h>

#include <yt/core/logging/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TMutation
    : public TIntrinsicRefCounted
{
public:
    explicit TMutation(IHydraManagerPtr hydraManager);

    TFuture<TMutationResponse> Commit();
    TFuture<TMutationResponse> CommitAndLog(const NLogging::TLogger& logger);
    TFuture<TMutationResponse> CommitAndReply(NRpc::IServiceContextPtr context);

    TMutationPtr SetRequestData(TSharedRef data, TString type);
    TMutationPtr SetHandler(TCallback<void(TMutationContext*)> handler);
    TMutationPtr SetAllowLeaderForwarding(bool value);
    TMutationPtr SetMutationId(const NRpc::TMutationId& mutationId, bool retry);

private:
    const IHydraManagerPtr HydraManager_;

    TMutationRequest Request_;

};

DEFINE_REFCOUNTED_TYPE(TMutation)

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request);

template <class TRequest, class TTarget>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request,
    void (TTarget::* handler)(TRequest*),
    TTarget* target);

template <class TRequest, class TResponse>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context);

template <class TRequest, class TResponse, class TTarget>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context,
    void (TTarget::* handler)(const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>&, TRequest*, TResponse*),
    TTarget* target);

template <class TRpcRequest, class TResponse, class THandlerRequest, class TTarget>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TResponse>>& context,
    const THandlerRequest& request,
    void (TTarget::* handler)(const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TResponse>>&, THandlerRequest*, TResponse*),
    TTarget* target);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

#define MUTATION_INL_H_
#include "mutation-inl.h"
#undef MUTATION_INL_H_
