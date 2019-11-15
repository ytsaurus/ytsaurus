#pragma once

#include "private.h"
#include "execution_stack.h"
#include "fls.h"

#include <yt/core/ytalloc/memory_tag.h>

#include <yt/core/misc/small_vector.h>

#include <util/system/context.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static class TFiberIdGenerator
{
public:
    TFiberIdGenerator()
    {
        Seed_.store(static_cast<TFiberId>(::time(nullptr)));
    }

    TFiberId Generate()
    {
        const TFiberId Factor = std::numeric_limits<TFiberId>::max() - 173864;
        YT_ASSERT(Factor % 2 == 1); // Factor must be coprime with 2^n.

        while (true) {
            auto seed = Seed_++;
            auto id = seed * Factor;
            if (id != InvalidFiberId) {
                return id;
            }
        }
    }

private:
    std::atomic<TFiberId> Seed_;

} FiberIdGenerator;

struct TContextSwitchHandlers
{
    std::function<void()> Out;
    std::function<void()> In;
};

DECLARE_REFCOUNTED_CLASS(TFiber)

class TFiberRegistry;

class TFiber
    : public TRefCounted
    , public ITrampoLine
{
public:
    TFiber(EExecutionStackKind stackKind = EExecutionStackKind::Small);

    ~TFiber();

    TFiberId GetId()
    {
        return Id_;
    }

    bool CheckFreeStackSpace(size_t space) const
    {
        return reinterpret_cast<char*>(Stack_->GetStack()) + space < __builtin_frame_address(0);
    }

    TExceptionSafeContext* GetContext();

private:
    // Base fiber fields.
    TFiberId Id_;

    std::shared_ptr<TExecutionStack> Stack_;
    TExceptionSafeContext Context_;

    // No way to select static/thread_local variable in GDB from particular shared library.
    TFiberRegistry* const Registry_;
    const std::list<TFiber*>::iterator Iterator_;

    void RegenerateId()
    {
        Id_ = FiberIdGenerator.Generate();
    }

public:
    // User-defined context switch handlers (executed only during WaitFor).
    friend void PushContextHandler(std::function<void()> out, std::function<void()> in);

    friend void PopContextHandler();

    void InvokeContextOutHandlers();

    void InvokeContextInHandlers();

private:
    SmallVector<TContextSwitchHandlers, 16> SwitchHandlers_;


public:
    // FLS, memory and tracing.
    void OnSwitchInto();

    void OnSwitchOut();

    NProfiling::TCpuDuration GetRunCpuTime() const;

private:
    NDetail::TFsdHolder FsdHolder_;

    NYTAlloc::TMemoryTag MemoryTag_ = NYTAlloc::NullMemoryTag;
    NYTAlloc::EMemoryZone MemoryZone_ = NYTAlloc::EMemoryZone::Normal;

    NTracing::TTraceContextPtr SavedTraceContext_;
    NProfiling::TCpuInstant RunStartInstant_ = 0;
    NProfiling::TCpuDuration RunCpuTime_ = 0;

protected:
    void OnStartRunning();

    void OnFinishRunning();

public:
    // WaitFor and cancelation logic.
    void ResetForReuse();

    bool IsCanceled() const;

    const TClosure& GetCanceler();

    void SetAwaitable(TAwaitable awaitable);

    void ResetAwaitable();

private:
    std::atomic<bool> Canceled_ = {false};
    std::atomic<size_t> Epoch_ = {0};

    TSpinLock SpinLock_;
    TClosure Canceler_;
    TAwaitable Awaitable_;

    void CancelEpoch(size_t epoch);

};

DEFINE_REFCOUNTED_TYPE(TFiber)

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
