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
{
public:
    explicit TMutation(IHydraManagerPtr hydraManager);

    TFuture<TMutationResponse> Commit();
    TFuture<TMutationResponse> CommitAndLog(const NLogging::TLogger& logger);
    TFuture<TMutationResponse> CommitAndReply(NRpc::IServiceContextPtr context);

    void SetRequestData(TSharedRef data, TString type);
    void SetHandler(TCallback<void(TMutationContext*)> handler);
    void SetAllowLeaderForwarding(bool value);
    void SetMutationId(const NRpc::TMutationId& mutationId, bool retry);

private:
    const IHydraManagerPtr HydraManager_;

    TMutationRequest Request_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
std::unique_ptr<TMutation> CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request);

template <class TRequest, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request,
    void (TTarget::* handler)(TRequest*),
    TTarget* target);

template <class TRequest, class TResponse>
std::unique_ptr<TMutation> CreateMutation(
    IHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context);

template <class TRequest, class TResponse, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    IHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context,
    void (TTarget::* handler)(const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>&, TRequest*, TResponse*),
    TTarget* target);

template <class TRpcRequest, class TResponse, class THandlerRequest, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
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
