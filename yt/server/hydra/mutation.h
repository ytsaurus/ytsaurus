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
    explicit TMutation(IHydraManagerPtr hydraManager);

    TFuture<TErrorOr<TMutationResponse>> Commit();

    TMutationPtr SetId(const TMutationId& id);

    TMutationPtr SetRequestData(TSharedRef data, Stroka type);
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

    TMutationRequest Request;
    TCallback<void(const TMutationResponse&)> OnSuccess_;
    TCallback<void(const TError&)> OnError_;

    TErrorOr<TMutationResponse> OnCommitted(TErrorOr<TMutationResponse> response);

};

////////////////////////////////////////////////////////////////////////////////

TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager);

template <class TRequest>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request);

template <class TTarget, class TRequest, class TResponse>
TMutationPtr CreateMutation(
    IHydraManagerPtr hydraManager,
    const TRequest& request,
    TTarget* target,
    TResponse (TTarget::* method)(const TRequest& request));

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

#define MUTATION_INL_H_
#include "mutation-inl.h"
#undef MUTATION_INL_H_
