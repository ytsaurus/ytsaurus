#pragma once

#include "public.h"
#include "execution_stack.h"
#include "execution_context.h"

#include <core/actions/future.h>
#include <core/actions/invoker.h>

#include <core/misc/small_vector.h>

#include <atomic>
#include <exception>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EFiberState,
    (Sleeping)    // Unscheduled and waiting for an external event to happen.
    (Suspended)   // Scheduled but not yet running.
    (Running)     // Currently executing.
    (Terminated)  // Terminated.
    (Canceled)    // Canceled. :)
);

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

    TFiberId GetId() const;

    EFiberState GetState() const;
    void SetState(EFiberState state);

    TExecutionContext* GetContext();

    void Cancel();

    bool IsCanceled() const;
    bool CanReturn() const;

    // Fiber-specific data.
    uintptr_t& FsdAt(int index);

private:
    TFiberId Id_;
    EFiberState State_;

    std::shared_ptr<TExecutionStack> Stack_;
    TExecutionContext Context_;

    std::atomic<bool> Canceled_;

    TClosure Callee_;

    SmallVector<uintptr_t, 8> Fsd_;
    void FsdResize();

    static void Trampoline(void*);
};

DEFINE_REFCOUNTED_TYPE(TFiber)

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TClosure GetCurrentFiberCanceler();

void ResumeFiber(TFiberPtr fiber);
void UnwindFiber(TFiberPtr fiber);

void ShutdownUnwindThread();

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

