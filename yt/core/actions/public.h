#pragma once

#include <core/misc/common.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

template <class TSignature>
class TCallback;

typedef TCallback<void()> TClosure;

template <class TSignature>
class TCallbackList;

template <class T>
class TFuture;

template <>
class TFuture<void>;

template <class T>
class TPromise;

template <>
class TPromise<void>;

DECLARE_REFCOUNTED_STRUCT(IInvoker)
DECLARE_REFCOUNTED_STRUCT(IPrioritizedInvoker)

DECLARE_REFCOUNTED_CLASS(TCancelableContext)

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
