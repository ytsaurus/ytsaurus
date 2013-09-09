#include "stdafx.h"
#include "mutation.h"
#include "meta_state_manager.h"

#include <core/concurrency/delayed_invoker.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

TMutation::TMutation(
    IMetaStateManagerPtr metaStateManager,
    IInvokerPtr stateInvoker)
    : MetaStateManager(std::move(metaStateManager))
    , StateInvoker(std::move(stateInvoker))
{ }

bool TMutation::PostCommit()
{
    return StateInvoker->Invoke(BIND(&TMutation::Commit, MakeStrong(this)));
}

void TMutation::Commit()
{
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
    Request.Action = std::move(action);
    return this;
}

TMutationPtr TMutation::OnSuccess(TClosure onSuccess)
{
    YASSERT(!OnSuccess_);
    OnSuccess_ = BIND([=] (const TMutationResponse&) {
        onSuccess.Run();
    });
    return this;
}

TMutationPtr TMutation::OnSuccess(TCallback<void(const TMutationResponse&)> onSuccess)
{
    YASSERT(!OnSuccess_);
    OnSuccess_ = std::move(onSuccess);
    return this;
}

TMutationPtr TMutation::OnError(TCallback<void(const TError&)> onError)
{
    YASSERT(!OnError_);
    OnError_ = std::move(onError);
    return this;
}

void TMutation::OnCommitted(TErrorOr<TMutationResponse> result)
{
    if (result.IsOK()) {
        if (OnSuccess_) {
            OnSuccess_.Run(result.GetValue());
        }
    } else {
        if (OnError_) {
            OnError_.Run(result);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
