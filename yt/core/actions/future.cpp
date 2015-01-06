#include "stdafx.h"
#include "future.h"

#include <core/concurrency/delayed_executor.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const TFuture<void> VoidFuture = MakeFuture(TError());
const TFuture<bool> TrueFuture = MakeFuture(true);
const TFuture<bool> FalseFuture = MakeFuture(false);

///////////////////////////////////////////////////////////////////////////////

TFuture<void> MakeDelayed(TDuration delay)
{
    auto promise = NewPromise<void>();
    NConcurrency::TDelayedExecutor::Submit(
        BIND([=] () mutable {
            promise.Set();
        }),
        delay);
    promise.OnCanceled(
        BIND([=] () mutable {
            promise.TrySet(TError(NYT::EErrorCode::Canceled, "Delayed promise canceled"));
        }));
    return promise;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
