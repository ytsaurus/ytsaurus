#include "stdafx.h"
#include "future.h"

#include <core/concurrency/delayed_executor.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TPromise<void> NewPromise()
{
    return TPromise<void>(New< NYT::NDetail::TPromiseState<void> >(false));
}

TPromise<void> MakePromise()
{
    return TPromise<void>(New< NYT::NDetail::TPromiseState<void> >(true));
}

TFuture<void> MakeDelayed(TDuration delay)
{
    auto promise = NewPromise();
    NConcurrency::TDelayedExecutor::Submit(
        BIND([=] () mutable { promise.Set(); }),
        delay);
    return promise;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
