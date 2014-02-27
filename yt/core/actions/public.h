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

struct IInvoker;
typedef TIntrusivePtr<IInvoker> IInvokerPtr;

struct IPrioritizedInvoker;
typedef TIntrusivePtr<IPrioritizedInvoker> IPrioritizedInvokerPtr;

class TCancelableContext;
typedef TIntrusivePtr<TCancelableContext> TCancelableContextPtr;

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
