#pragma once

#include "public.h"

#include <core/actions/public.h>
#include <core/misc/public.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Manages delayed callback execution.
class TDelayedExecutor
{
public:
    static TDelayedExecutor* Get();

    //! Submits #callback for execution after a given #delay.
    static TDelayedExecutorCookie Submit(TClosure callback, TDuration delay);

    //! Submits #callback for execution at a given #deadline.
    static TDelayedExecutorCookie Submit(TClosure callback, TInstant deadline);

    //! Cancels an earlier scheduled execution.
    /*!
     *  \returns True iff the cookie is valid.
     */
    static void Cancel(const TDelayedExecutorCookie& cookie);

    //! Cancels an earlier scheduled execution and clears the cookie.
    /*!
     *  \returns True iff the cookie is valid.
     */
    static void CancelAndClear(TDelayedExecutorCookie& cookie);

    DECLARE_SINGLETON_DEFAULT_MIXIN(TDelayedExecutor);

private:
    TDelayedExecutor();
    ~TDelayedExecutor();

    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

extern const TDelayedExecutorCookie NullDelayedExecutorCookie;

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
