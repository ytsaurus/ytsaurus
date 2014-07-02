#include "stdafx.h"
#include "callback_internal.h"

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TBindStateBase::TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location
#endif
    )
    : TraceContext(NTracing::GetCurrentTraceContext())
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    , Location(location)
#endif
{ }

TBindStateBase::~TBindStateBase()
{ }

TCallbackBase::operator bool() const
{
    return static_cast<bool>(BindState);
}

void TCallbackBase::Reset()
{
    BindState = nullptr;
    UntypedInvoke = nullptr;
}

void* TCallbackBase::GetHandle() const
{
    return (void*)((size_t)(void*)BindState.Get() ^ (size_t)(void*)UntypedInvoke);
}

void TCallbackBase::Swap(TCallbackBase& other)
{
    TIntrusivePtr<TBindStateBase> tempBindState = std::move(other.BindState);
    TUntypedInvokeFunction tempUntypedInvoke = std::move(other.UntypedInvoke);

    other.BindState = std::move(BindState);
    other.UntypedInvoke = std::move(UntypedInvoke);

    BindState = std::move(tempBindState);
    UntypedInvoke = std::move(tempUntypedInvoke);
}

bool TCallbackBase::operator == (const TCallbackBase& other) const
{
    return
        BindState == other.BindState &&
        UntypedInvoke == other.UntypedInvoke;
}

bool TCallbackBase::operator != (const TCallbackBase& other) const
{
    return !(*this == other);
}

TCallbackBase::TCallbackBase(TIntrusivePtr<TBindStateBase>&& bindState)
    : BindState(std::move(bindState))
    , UntypedInvoke(NULL)
{
    YASSERT(!BindState || BindState->GetRefCount() == 1);
}

TCallbackBase::~TCallbackBase()
{ }

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT
}  // namespace NDetail
