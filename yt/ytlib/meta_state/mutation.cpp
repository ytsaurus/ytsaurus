#include "stdafx.h"
#include "mutation.h"
#include "meta_state_manager.h"

#include <ytlib/misc/delayed_invoker.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

TMutation::TMutation(IMetaStateManagerPtr metaStateManager)
    : MetaStateManager(MoveRV(metaStateManager))
    , Started(false)
    , Retriable(false)
    , EpochContext(MetaStateManager->GetEpochContext())
{ }

void TMutation::PostCommit()
{
    auto this_ = MakeStrong(this);
    MetaStateManager
        ->GetStateInvoker()
        ->Invoke(BIND([=] () {
            if (!this_->EpochContext->IsCanceled()) {
                this_->Commit();
            }
        }));
}

void TMutation::Commit()
{
    YASSERT(!Started);
    Started = true;

    DoCommit();
}

void TMutation::DoCommit()
{
    YASSERT(Started);
    MetaStateManager->CommitMutation(Request).Subscribe(
        BIND(&TMutation::OnCommitted, MakeStrong(this)));
}

TMutationPtr TMutation::SetType(const Stroka& type)
{
    Request.Type = type;
    return this;
}

TMutationPtr TMutation::SetId(const TMutationId& id)
{
    Request.Id = id;
    return this;
}

TMutationPtr TMutation::SetRequestData(const TSharedRef& data)
{
    Request.Data = data;
    return this;
}

TMutationPtr TMutation::SetAction(TClosure action)
{
    Request.Action = MoveRV(action);
    return this;
}

TMutationPtr TMutation::SetRetriable(TDuration backoffTime)
{
    Retriable = true;
    BackoffTime = backoffTime;
    return this;
}

TMutationPtr TMutation::OnSuccess(TClosure onSuccess)
{
    YASSERT(OnSuccess_.IsNull());
    OnSuccess_ = BIND([=] (const TMutationResponse&) {
        onSuccess.Run();
    });
    return this;
}

TMutationPtr TMutation::OnSuccess(TCallback<void(const TMutationResponse&)> onSuccess)
{
    YASSERT(OnSuccess_.IsNull());
    OnSuccess_ = MoveRV(onSuccess);
    return this;
}

TMutationPtr TMutation::OnError(TCallback<void(const TError&)> onError)
{
    YASSERT(OnError_.IsNull());
    OnError_ = MoveRV(onError);
    return this;
}

void TMutation::OnCommitted(TValueOrError<TMutationResponse> result)
{
    if (result.IsOK()) {
        if (!OnSuccess_.IsNull()) {
            OnSuccess_.Run(result.Value());
        }
    } else {
        if (!OnError_.IsNull()) {
            OnError_.Run(result);
        }
        if (Retriable) {
            TDelayedInvoker::Submit(
                BIND(&TMutation::DoCommit, MakeStrong(this))
                .Via(MetaStateManager->GetStateInvoker(), EpochContext),
                BackoffTime);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
