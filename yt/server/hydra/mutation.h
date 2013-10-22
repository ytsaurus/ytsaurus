#pragma once

#include "public.h"
#include "mutation_context.h"

#include <core/misc/error.h>
#include <core/misc/ref.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TMutation
    : public TIntrinsicRefCounted
{
public:
    TMutation(
        IHydraManagerPtr hydraManager,
        IInvokerPtr automatonInvoker);

    void Commit();
    bool PostCommit();

    TMutationPtr SetType(Stroka type);

    TMutationPtr SetId(const TMutationId& id);

    TMutationPtr SetRequestData(TSharedRef data);
    template <class TRequest>
    TMutationPtr SetRequestData(const TRequest& request);

    TMutationPtr SetAction(TClosure action);

    TMutationPtr OnSuccess(TClosure onSuccess);
    TMutationPtr OnSuccess(TCallback<void(const TMutationResponse&)> onSuccess);
    template <class TResponse>
    TMutationPtr OnSuccess(TCallback<void(const TResponse&)> onSuccess);

    TMutationPtr OnError(TCallback<void(const TError&)> onError);

private:
    IHydraManagerPtr HydraManager;
    IInvokerPtr AutomatonInvoker;

    TMutationRequest Request;
    TCallback<void(const TMutationResponse&)> OnSuccess_;
    TCallback<void(const TError&)> OnError_;

    void OnCommitted(TErrorOr<TMutationResponse> response);

};

////////////////////////////////////////////////////////////////////////////////

TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    IInvokerPtr automatonInvoker);

template <class TRequest>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    IInvokerPtr automatonInvoker,
    const TRequest& request);

template <class TTarget, class TRequest, class TResponse>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    IInvokerPtr automatonInvoker,
    const TRequest& request,
    TTarget* target,
    TResponse (TTarget::* method)(const TRequest& request));

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

#define MUTATION_INL_H_
#include "mutation-inl.h"
#undef MUTATION_INL_H_
