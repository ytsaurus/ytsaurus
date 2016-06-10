#ifndef CANCELABLE_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include cancelable_context.h"
#endif
#undef CANCELABLE_CONTEXT_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TCancelableContext::PropagateTo(TFuture<T> future)
{
    PropagateTo(future.template As<void>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
