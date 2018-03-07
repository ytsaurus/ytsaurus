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
    typedef TCallback<void(bool)> TDelayedCallback;

    //! Constructs a future that gets set when a given #delay elapses.
    /*!
     *  Note that during shutdown this future will be resolved prematurely with an error.
     */
    static TFuture<void> MakeDelayed(TDuration delay);

    //! Constructs a future that gets set when a given #duration elapses and
    //! immediately waits for it.
    /*!
     *  This is barely equivalent to MakeDelayed and WaitFor combination.
     *  The result of waiting is ignored.
     */
    static void WaitForDuration(TDuration duration);

    //! Submits #callback for execution after a given #delay.
    /*!
     *  #callback is guaranteed to be invoked exactly once unless the cookie was cancelled (cf. #Cancel).
     *  The exact thread where the invocation takes place is unspecified.
     *
     *  |aborted| flag is provided to the callback to indicate whether this is a premature execution
     *  due to shutdown or not.
     *
     *  Note that after shutdown has been initiated, #Submit may start invoking the newly-passed callbacks
     *  immediately with |aborted = true|. It is guaranteed, though, that each #Submit call may only
     *  cause an immediate execution of *its* callback but not others.
     *
     *  \param callback A callback to execute.
     *  \param delay Execution delay.
     *  \return An opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(TDelayedCallback callback, TDuration delay);

    //! Submits #closure for execution after a given #delay.
    /*!
     *  \param closure A closure to execute.
     *  \param delay Execution delay.
     *  \return An opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(TClosure closure, TDuration delay);

    //! Submits #callback for execution at a given #deadline.
    /*!
     * \param callback A callback to execute.
     * \param deadline Execution deadline.
     * \return An opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(TDelayedCallback callback, TInstant deadline);

    //! Submits #closure for execution at a given #deadline.
    /*!
     * \param closure A closure to execute.
     * \param delay Execution deadline.
     * \return An opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(TClosure closure, TInstant deadline);

    //! Cancels an earlier scheduled execution and clears the cookie.
    /*!
     *  This call is "safe", i.e. cannot lead to immediate execution of any callbacks even
     *  during shutdown.
     *
     *  Cancelation should always be regarded as a hint. It is inherently racy and
     *  is not guaranteed to be handled during and after shutdown.
     */
    static void CancelAndClear(TDelayedExecutorCookie& cookie);

    //! Shuts the subsystem down.
    /*!
     *  Safe to call multiple times.
     *
     *  This call may block to wait for all registered callbacks to complete.
     *  Some of the above callbacks (i.e. those for which the deadlines were not reached yet)
     *  will get |aborted = true| in their parameters. This is somewhat racy, though, and should not
     *  be relied upon.
     *
     *  For all calls to #Submit after the shutdown is finished, the callbacks are executed immediately
     *  with |aborted = true|.
     */
    static void StaticShutdown();

private:
    TDelayedExecutor();
    ~TDelayedExecutor();

    class TImpl;
    static TImpl* GetImpl();

    Y_DECLARE_SINGLETON_FRIEND();
};

extern const TDelayedExecutorCookie NullDelayedExecutorCookie;

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
