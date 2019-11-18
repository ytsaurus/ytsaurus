#pragma once

#include "execution_stack.h"
#include "fls.h"

#include <yt/core/actions/future.h>

#include <yt/core/profiling/public.h>

#include <yt/core/tracing/public.h>

#include <yt/core/misc/small_set.h>

#include <yt/core/ytalloc/memory_tag.h>
#include <yt/core/ytalloc/memory_zone.h>

#include <util/system/context.h>

#include <atomic>
#include <forward_list>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFiberState,
    (Sleeping)    // Unscheduled and waiting for an external event to happen.
    (Suspended)   // Scheduled but not yet running.
    (Running)     // Currently executing.
    (Terminated)  // Terminated.
);

struct TContextSwitchHandlers
{
    std::function<void()> Out;
    std::function<void()> In;
};

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
    , public ITrampoLine
{
public:
    explicit TFiber(
        TClosure callee,
        EExecutionStackKind stackKind = EExecutionStackKind::Small);

    TFiber(const TFiber&) = delete;
    TFiber(TFiber&&) = delete;

    ~TFiber();

    //! Returns a unique fiber id.
    /*!
     *  Thread affinity: any
     *  Ids are unique for the duration of the process.
     */
    TFiberId GetId() const;

    //! Generates a new id for this fiber. Used when the fiber instance is reused.
    //! Returns the new id.
    TFiberId RegenerateId();

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

    //! Sets the current fiber state to EFiberState::Sleeping (optionally providing an awaitable
    //! the fiber is waiting for).
    /*!
     *  Thread affinity: OwnerThread
     */
    void SetSleeping(TAwaitable awaitable = {});

    //! Sets the current fiber state to EFiberState::Suspended.
    /*!
     *  Thread affinity: OwnerThread
     */
    void SetSuspended();

    //! Returns the underlying execution context.
    /*!
     *  Thread affinity: OwnerThread
     */
    TExceptionSafeContext* GetContext();

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

    //! Throws TFiberCanceledException if canceled.
    /*!
     *  Thread affinity: any
     */
    void UnwindIfCanceled() const;

    //! Returns |true| if the fiber has finished executing.
    /*!
     * This could either happen normally (i.e. the callee returns) or
     * abnormally (TFiberCanceledException is thrown and is subsequently
     * caught in the trampoline).
     */
    bool IsTerminated() const;

    //! Pushes the context handlers.
    /*!
     *  Thread affinity: OwnerThread
     */
    void PushContextHandler(std::function<void()> out, std::function<void()> in);

    //! Pops the context handlers.
    /*!
     *  Thread affinity: OwnerThread
     */
    void PopContextHandler();

    //! Invokes all out handlers.
    /*!
     *  Thread affinity: OwnerThread
     */
    void InvokeContextOutHandlers();

    //! Invokes all in handlers.
    /*!
     *  Thread affinity: OwnerThread
     */
    void InvokeContextInHandlers();

    //! Returns |true| if there is enough remaining stack space.
    /*!
     *  Thread affinity: OwnerThread
     */
    bool CheckFreeStackSpace(size_t space) const;

    //! Returns the duration the fiber is running.
    //! This counts CPU wall time but excludes periods the fiber was sleeping.
    //! The call only makes sense if the fiber is currently runnning.
    /*!
     *  Thread affinity: OwnerThread
     */
    NProfiling::TCpuDuration GetRunCpuTime() const;

private:
    TFiberId Id_;
    std::list<TFiber*>::iterator Iterator_;

    TSpinLock SpinLock_;

    NTracing::TTraceContextPtr SavedTraceContext_;

    NProfiling::TCpuInstant RunStartInstant_ = 0;
    NProfiling::TCpuDuration RunCpuTime_ = 0;

    EFiberState State_ = EFiberState::Suspended;
    TAwaitable Awaitable_;

    TClosure Callee_;
    std::shared_ptr<TExecutionStack> Stack_;
    TExceptionSafeContext Context_;

    std::atomic<bool> Canceled_ = {false};
    TClosure Canceler_;
    void Cancel();

    NDetail::TFsdHolder FsdHolder_;

    NYTAlloc::TMemoryTag MemoryTag_ = NYTAlloc::NullMemoryTag;
    NYTAlloc::EMemoryZone MemoryZone_ = NYTAlloc::EMemoryZone::Normal;

    SmallVector<TContextSwitchHandlers, 16> SwitchHandlers_;

    void FinishRunning();

    // ITrampoLine implementation
    virtual void DoRunNaked() override;
};

DEFINE_REFCOUNTED_TYPE(TFiber)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

