#pragma once

#include "public.h"

#include <yt/core/actions/callback.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Manages delayed callback execution.
class TDelayedExecutor
{
public:
    ~TDelayedExecutor();

    typedef TCallback<void(bool)> TDelayedCallback;

    //! Constructs a future that gets set when a given #delay elapses.
    /*!
     * Note that during shutdown this future will be resolved prematurely with an error.
     */
    static TFuture<void> MakeDelayed(TDuration delay);

    //! Submits #callback for execution after a given #delay.
    /*!
     * Passed callback is guaranteed to be invoked unless the cookie was cancelled.
     * Flag is provided to the callback to indicate whenther this execution was aborted or not.
     * \param callback A callback to execute.
     * \param delay Execution delay.
     * \return Opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(TDelayedCallback callback, TDuration delay);

    //! Submits #closure for execution after a given #delay.
    /*!
     * Passed closure is not guaranteed to be executed (for example, in case of program termination).
     * \param closure A closure to execute.
     * \param delay Execution delay.
     * \return Opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(TClosure closure, TDuration delay);

    //! Submits #callback for execution at a given #deadline.
    /*!
     * Flag passed to the callback indicates whether this execution was aborted or not.
     * \param callback A callback to execute.
     * \param deadline Execution deadline.
     * \return Opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(TDelayedCallback callback, TInstant deadline);

    //! Submits #closure for execution at a given #deadline.
    /*!
     * Passed closure is not guaranteed to be executed (for example, in case of program termination).
     * \param closure A closure to execute.
     * \param delay Execution deadline.
     * \return Opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(TClosure closure, TInstant deadline);

    //! Cancels an earlier scheduled execution and clears the cookie.
    static void CancelAndClear(TDelayedExecutorCookie& cookie);

    //! Terminates the scheduler thread.
    /*!
     *  All subsequent #Submit calls are silently ignored.
     */
    static void StaticShutdown();

private:
    class TImpl;

    TDelayedExecutor();

    static TImpl* const GetImpl();

    Y_DECLARE_SINGLETON_FRIEND();
};

extern const TDelayedExecutorCookie NullDelayedExecutorCookie;

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
