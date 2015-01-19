#pragma once

#include "public.h"
#include "execution_stack.h"
#include "execution_context.h"

#include <core/actions/future.h>

#include <core/misc/small_vector.h>

#include <atomic>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFiberState,
    (Sleeping)    // Unscheduled and waiting for an external event to happen.
    (Suspended)   // Scheduled but not yet running.
    (Running)     // Currently executing.
    (Terminated)  // Terminated.
);

//! A fiber :)
/*!
 *  This class is not intended to be used directly.
 *  Please use TCoroutine or TCallback::AsyncVia to instantiate fibers.
 *
 *  Some methods could only be called from the owner thread (which currently runs the fiber).
 *  Others could be called from an arbitrary thread.
 */
class TFiber
    : public TRefCounted
{
public:
    explicit TFiber(
        TClosure callee,
        EExecutionStack stack = EExecutionStack::Small);

    TFiber(const TFiber&) = delete;
    TFiber(TFiber&&) = delete;

    ~TFiber();

    //! Returns a unique fiber id.
    /*!
     *  Thread affinity: any
     *  Ids are unique for the duration of the process.
     */
    TFiberId GetId() const;

    //! Return the current fiber state.
    /*!
     *  Thread affinity: OwnerThread
     */
    EFiberState GetState() const;

    //! Sets the current fiber state to EFiberState::Running.
    /*!
     *  Thread affinity: OwnerThread
     */
    void SetRunning();

    //! Sets the current fiber state to EFiberState::Sleeping (optionally providing a future
    //! the fiber is waiting for).
    /*!
     *  Thread affinity: OwnerThread
     */
    void SetSleeping(TFuture<void> awaitedFuture = TFuture<void>());

    //! Sets the current fiber state to EFiberState::Suspended.
    /*!
     *  Thread affinity: OwnerThread
     */
    void SetSuspended();

    //! Returns the underlying execution context.
    /*!
     *  Thread affinity: OwnerThread
     */
    TExecutionContext* GetContext();

    //! Returns a cached callback that schedules fiber cancelation.
    /*!
     *  Thread affinity: any
     */
    const TClosure& GetCanceler();

    //! Returns |true| if the canceler was requested by anyone.
    /*!
     *  Thread affinity: any
     */
    bool IsCancelable() const;

    //! Returns |true| if the fiber was canceled.
    /*!
     *  Thread affinity: any
     */
    bool IsCanceled() const;

    //! Returns |true| if the fiber has finished executing.
    /*!
     * This could either happen normally (i.e. the callee returns) or
     * abnormally (TFiberCanceledException is thrown and is subsequently
     * caught in the trampoline).
     */
    bool IsTerminated() const;

    //! Provides access to the fiber-specific data.
    /*!
     *  Thread affinity: OwnerThread
     */
    uintptr_t& FsdAt(int index);

private:
    TFiberId Id_;
#ifdef DEBUG
    std::list<TFiber*>::iterator Iterator_;
#endif

    TSpinLock SpinLock_;

    EFiberState State_;
    TFuture<void> AwaitedFuture_;

    TClosure Callee_;
    std::shared_ptr<TExecutionStack> Stack_;
    TExecutionContext Context_;

    std::atomic<bool> Canceled_;
    TClosure Canceler_;
    void Cancel();

    SmallVector<uintptr_t, 8> Fsd_;
    void FsdResize();

    static void Trampoline(void*);
};

DEFINE_REFCOUNTED_TYPE(TFiber)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

