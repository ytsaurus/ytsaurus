#ifndef EXPIRATION_VERIFIER_INL_H_
#error "Direct inclusion of this file is not allowed, include expiration_verifier.h"
// For the sake of sane code completion.
#include "expiration_verifier.h"
#endif

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <util/system/type_name.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void VerifyEventualExpiration(
    TIntrusivePtr<T>&& ptr,
    NLogging::TLogger logger,
    TDuration timeout)
{
    // Tolerate null so callers can verify unconditionally, e.g.
    // VerifyEventualExpiration(std::exchange(Member_, nullptr), ...). The capture
    // initializer TypeName(*ptr) below would otherwise dereference a null pointer.
    if (!ptr) {
        return;
    }

    NConcurrency::TDelayedExecutor::Submit(
        BIND([
            weakPtr = MakeWeak(ptr),
            Logger = std::move(logger),
            typeName = TypeName(*ptr),
            timeout
        ] {
            if (weakPtr.IsExpired()) {
                return;
            }

            // The object outlived its finalization, so a (possibly cyclic) reference
            // still keeps it alive. Dump the ref-counted tracker to help pinpoint the
            // types holding it. The dump is emitted before the alert because the alert
            // may abort the process (e.g. under abort_on_alert in tests).
            TRefCountedTracker::Get()->LogDebugInfo(Logger, NLogging::ELogLevel::Error);

            YT_LOG_ALERT(
                "Object did not expire within the expected timeout; "
                "a reference cycle is likely preventing its destruction "
                "(ObjectType: %v, Timeout: %v, Pointer: %v)",
                typeName,
                timeout,
                weakPtr.Get());
        }),
        timeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
