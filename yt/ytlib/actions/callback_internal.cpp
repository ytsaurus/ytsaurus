#include "stdafx.h"
#include "callback_internal.h"

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

#ifdef ENABLE_BIND_LOCATION_TRACKING
TBindStateBase::TBindStateBase(const ::NYT::TSourceLocation& location)
    : Location_(location)
{ }
#endif

TBindStateBase::~TBindStateBase()
{ }

bool TCallbackBase::IsNull() const
{
    return BindState.Get() == NULL;
}

void TCallbackBase::Reset()
{
    BindState = NULL;
    UntypedInvoke = NULL;
}

void* TCallbackBase::GetHandle() const
{
    return (void*)((size_t)(void*)BindState.Get() ^ (size_t)(void*)UntypedInvoke);
}

void TCallbackBase::Swap(TCallbackBase& other)
{
    TIntrusivePtr<TBindStateBase> tempBindState = MoveRV(other.BindState);
    TUntypedInvokeFunction tempUntypedInvoke = MoveRV(other.UntypedInvoke);
 
    other.BindState = MoveRV(BindState);
    other.UntypedInvoke = MoveRV(UntypedInvoke);
 
    BindState = MoveRV(tempBindState);
    UntypedInvoke = MoveRV(tempUntypedInvoke);
}

bool TCallbackBase::Equals(const TCallbackBase& other) const
{
    return
        BindState.Get() == other.BindState.Get() &&
        UntypedInvoke == other.UntypedInvoke;
}

TCallbackBase::TCallbackBase(const TCallbackBase& other)
    : BindState(other.BindState)
    , UntypedInvoke(other.UntypedInvoke)
{ }

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

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT
}  // namespace NDetail
