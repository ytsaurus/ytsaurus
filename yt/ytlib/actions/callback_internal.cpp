#include "stdafx.h"
#include "callback_internal.h"

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

bool TCallbackBase::IsNull() const
{
    return BindState.Get() == NULL;
}

void TCallbackBase::Reset()
{
    BindState = NULL;
    UntypedInvoke = NULL;
}

bool TCallbackBase::Equals(const TCallbackBase& other) const
{
    return
        BindState.Get() == other.BindState.Get() &&
        UntypedInvoke == other.UntypedInvoke;
}

TCallbackBase::TCallbackBase(TCallbackBase&& other)
    : BindState(MoveRV(other.BindState))
    , UntypedInvoke(MoveRV(other.UntypedInvoke))
{ }

TCallbackBase::TCallbackBase(TIntrusivePtr<TBindStateBase>&& bindState)
    : BindState(MoveRV(bindState))
    , UntypedInvoke(NULL)
{
    YASSERT(!BindState || BindState->GetRefCount() == 1);
}

TCallbackBase::~TCallbackBase()
{ }

TCallbackBase& TCallbackBase::operator=(TCallbackBase& other)
{
    BindState = other.BindState;
    UntypedInvoke = other.UntypedInvoke;
    return *this;
}

TCallbackBase& TCallbackBase::operator=(TCallbackBase&& other)
{
    BindState = MoveRV(other.BindState);
    UntypedInvoke = MoveRV(other.UntypedInvoke);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT
}  // namespace NDetail
