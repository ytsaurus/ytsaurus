#include "stdafx.h"
#include "future.h"

#include <ytlib/misc/delayed_invoker.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TFuture<void> MakeDelayed(TDuration delay)
{
    auto promise = NewPromise<void>();
    TDelayedInvoker::Submit(
        BIND([=] () mutable { promise.Set(); }),
        delay);
    return promise;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
