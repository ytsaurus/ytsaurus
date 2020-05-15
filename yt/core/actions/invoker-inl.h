#pragma once
#ifndef INVOKER_INL_H_
#error "Direct inclusion of this file is not allowed, include invoker.h"
// For the sake of sane code completion.
#include "invoker.h"
#endif
#undef INVOKER_INL_H_

#include "bind.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class R, class... TArgs>
TExtendedCallback<R(TArgs...)>
TExtendedCallback<R(TArgs...)>::Via(IInvokerPtr invoker) const
{
    static_assert(
        NMpl::TIsVoid<R>::Value,
        "Via() can only be used with void return type.");
    YT_ASSERT(invoker);

    auto this_ = *this;
    return BIND_DONT_CAPTURE_TRACE_CONTEXT([=] (TArgs... args) {
        invoker->Invoke(BIND_DONT_CAPTURE_TRACE_CONTEXT(this_, std::forward<TArgs>(args)...));
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
