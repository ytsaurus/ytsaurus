#pragma once

#include "public.h"
#include "mutation_context.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/tracing/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TMutation
{
public:
    explicit TMutation(ISimpleHydraManagerPtr hydraManager);

    TFuture<TMutationResponse> Commit();
    TFuture<TMutationResponse> CommitAndLog(NLogging::TLogger logger);
    TFuture<TMutationResponse> CommitAndReply(NRpc::IServiceContextPtr context);

    void SetRequestData(TSharedRef data, TString type);
    void SetHandler(TCallback<void(TMutationContext*)> handler);
    void SetAllowLeaderForwarding(bool value);
    void SetMutationId(NRpc::TMutationId mutationId, bool retry);
    void SetEpochId(TEpochId epochId);
    void SetTraceContext(NTracing::TTraceContextPtr traceContext);
    void SetCurrentTraceContext();

    const TString& GetType() const;
    const TSharedRef& GetData() const;
    NRpc::TMutationId GetMutationId() const;
    bool IsRetry() const;

private:
    const ISimpleHydraManagerPtr HydraManager_;

    TMutationRequest Request_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
    const TRequest& request);

template <class TRequest, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
    const TRequest& request,
    void (TTarget::* handler)(TRequest*),
    TTarget* target);

template <class TRequest, class TResponse>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context);

template <class TRequest, class TResponse, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>& context,
    void (TTarget::* handler)(const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>&, TRequest*, TResponse*),
    TTarget* target);

template <class TRequest, class TResponse, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
    TRequest* request,
    TResponse* response,
    void (TTarget::* handler)(const TIntrusivePtr<NRpc::TTypedServiceContext<TRequest, TResponse>>&, TRequest*, TResponse*),
    TTarget* target);

template <class TRpcRequest, class TResponse, class THandlerRequest, class TTarget>
std::unique_ptr<TMutation> CreateMutation(
    ISimpleHydraManagerPtr hydraManager,
    const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TResponse>>& context,
    const THandlerRequest& request,
    void (TTarget::* handler)(const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TResponse>>&, THandlerRequest*, TResponse*),
    TTarget* target);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

#define MUTATION_INL_H_
#include "mutation-inl.h"
#undef MUTATION_INL_H_
