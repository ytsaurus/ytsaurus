#pragma once

#include "public.h"
#include "mutation_context.h"

#include <ytlib/misc/error.h>
#include <ytlib/actions/cancelable_context.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TMutation
    : public TIntrinsicRefCounted
{
public:
    explicit TMutation(IMetaStateManagerPtr metaStateManager);

    void Commit();
    void PostCommit();

    TMutationPtr SetType(const Stroka& type);

    TMutationPtr SetId(const TMutationId& id);

    TMutationPtr SetRequestData(const TSharedRef& data);
    template <class TRequest>
    TMutationPtr SetRequestData(const TRequest& request);

    TMutationPtr SetAction(TClosure action);

    TMutationPtr SetRetriable(TDuration backoffTime);

    TMutationPtr OnSuccess(TClosure onSuccess);
    TMutationPtr OnSuccess(TCallback<void(const TMutationResponse&)> onSuccess);
    template <class TResponse>
    TMutationPtr OnSuccess(TCallback<void(const TResponse&)> onSuccess);

    TMutationPtr OnError(TCallback<void(const TError&)> onError);

private:
    IMetaStateManagerPtr MetaStateManager;
    bool Started;
    bool Retriable;
    TCancelableContextPtr EpochContext;

    TMutationRequest Request;
    TDuration BackoffTime;
    TCallback<void(const TMutationResponse&)> OnSuccess_;
    TCallback<void(const TError&)> OnError_;

    void DoCommit();
    void OnCommitted(TValueOrError<TMutationResponse> response);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

#define MUTATION_INL_H_
#include "mutation-inl.h"
#undef MUTATION_INL_H_
