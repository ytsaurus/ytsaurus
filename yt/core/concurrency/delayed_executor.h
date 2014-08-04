#pragma once

#include "public.h"

#include <core/actions/public.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Manages delayed callback execution.
class TDelayedExecutor
{
public:
    //! Submits #callback for execution after a given #delay.
    static TDelayedExecutorCookie Submit(TClosure callback, TDuration delay);

    //! Submits #callback for execution at a given #deadline.
    static TDelayedExecutorCookie Submit(TClosure callback, TInstant deadline);

    //! Cancels an earlier scheduled execution.
    /*!
     *  \returns True iff the cookie is valid.
     */
    static void Cancel(TDelayedExecutorCookie cookie);

    //! Cancels an earlier scheduled execution and clears the cookie.
    /*!
     *  \returns True iff the cookie is valid.
     */
    static void CancelAndClear(TDelayedExecutorCookie& cookie);

private:
    TDelayedExecutor();
    ~TDelayedExecutor();

    class TImpl;

};

extern const TDelayedExecutorCookie NullDelayedExecutorCookie;

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
