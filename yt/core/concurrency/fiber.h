#pragma once

#include "public.h"
#include "execution_stack.h"
#include "execution_context.h"

#include <core/actions/future.h>
#include <core/actions/invoker.h>

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
    (Crashed)     // Crashed. :)
);

class TFiber
    : public TRefCounted
{
public:
    explicit TFiber(TClosure callee, EExecutionStack stack = EExecutionStack::Small);

    TFiber(const TFiber&) = delete;
    TFiber(TFiber&&) = delete;

    ~TFiber();

    EFiberState GetState() const;
    void SetState(EFiberState state);

    TExecutionContext* GetContext();
    std::exception_ptr GetException();

    void Cancel();

    bool IsCanceled() const;
    bool CanReturn() const;

    // Fiber-local information.
    typedef void* TFlsSlotValue;
    typedef TFlsSlotValue (*TFlsSlotCtor)();
    typedef void (*TFlsSlotDtor)(TFlsSlotValue);

    static int FlsAllocateSlot(TFlsSlotCtor ctor, TFlsSlotDtor dtor);

    TFlsSlotValue FlsGet(int index);
    void FlsSet(int index, TFlsSlotValue value);

private:
    EFiberState State_;

    std::shared_ptr<TExecutionStack> Stack_;
    TExecutionContext Context_;
    std::exception_ptr Exception_;

    std::atomic_bool Canceled_;

    TClosure Callee_;

    std::vector<TFlsSlotValue> Fls_;
    void FlsEnsure(int index);

    static void Trampoline(void*);
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TClosure GetCurrentFiberCanceler();

void ResumeFiber(TFiberPtr fiber);
void UnwindFiber(TFiberPtr fiber);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

