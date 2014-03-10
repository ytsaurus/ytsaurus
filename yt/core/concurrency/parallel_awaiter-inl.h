#ifndef PARALLEL_AWAITER_INL_H_
#error "Direct inclusion of this file is not allowed, include parallel_awaiter.h"
#endif
#undef PARALLEL_AWAITER_INL_H_

#include <core/concurrency/thread_affinity.h>

#include <core/profiling/timing.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

inline TParallelAwaiter::TParallelAwaiter(
    IInvokerPtr invoker)
{
    Init(invoker, nullptr, Null);
}

inline TParallelAwaiter::TParallelAwaiter(
    IInvokerPtr invoker,
    NProfiling::TProfiler* profiler,
    const NYPath::TYPath& timingPath)
{
    Init(invoker, profiler, timingPath);
}

inline void TParallelAwaiter::Init(
    IInvokerPtr invoker,
    NProfiling::TProfiler* profiler,
    const TNullable<NYPath::TYPath>& timingPath)
{
    YCHECK(invoker);

    Canceled_ = false;

    Completed_ = false;
    CompletedPromise_ = NewPromise();

    Terminated_ = false;

    RequestCount_ = 0;
    ResponseCount_ = 0;

    CancelableContext_ = New<TCancelableContext>();
    CancelableInvoker_ = CancelableContext_->CreateInvoker(invoker);

    Profiler_ = profiler;

    if (Profiler_ && timingPath) {
        Timer_ = Profiler_->TimingStart(
            *timingPath,
            NProfiling::EmptyTagIds,
            NProfiling::ETimerMode::Parallel);
    }
}

inline bool TParallelAwaiter::TryAwait()
{
    TGuard<TSpinLock> guard(SpinLock_);
    YASSERT(!Completed_);

    if (Canceled_ || Terminated_)
        return false;

    ++RequestCount_;
    return true;
}

template <class T>
void TParallelAwaiter::Await(
    TFuture<T> result,
    const NProfiling::TTagIdList& tagIds,
    TCallback<void(T)> onResult,
    TClosure onCancel)
{
    YASSERT(result);

    if (TryAwait()) {
        result.Subscribe(BIND(
            &TParallelAwaiter::HandleResult<T>,
            MakeStrong(this),
            tagIds,
            Passed(std::move(onResult))));
        result.OnCanceled(BIND(
            &TParallelAwaiter::HandleCancel,
            MakeStrong(this),
            tagIds,
            Passed(std::move(onCancel))));
    }
}

inline void TParallelAwaiter::Await(
    TFuture<void> result,
    const NProfiling::TTagIdList& tagIds,
    TCallback<void(void)> onResult,
    TClosure onCancel)
{
    YASSERT(result);

    if (TryAwait()) {
        result.Subscribe(BIND(
            (void(TParallelAwaiter::*)(const NProfiling::TTagIdList&, TCallback<void()>)) &TParallelAwaiter::HandleResult,
            MakeStrong(this),
            tagIds,
            Passed(std::move(onResult))));
        result.OnCanceled(BIND(
            &TParallelAwaiter::HandleCancel,
            MakeStrong(this),
            tagIds,
            Passed(std::move(onCancel))));
    }
}

template <class T>
void TParallelAwaiter::Await(
    TFuture<T> result,
    TCallback<void(T)> onResult,
    TClosure onCancel)
{
    Await(
        std::move(result),
        NProfiling::EmptyTagIds,
        std::move(onResult),
        std::move(onCancel));
}

inline void TParallelAwaiter::Await(
    TFuture<void> result,
    TCallback<void()> onResult,
    TClosure onCancel)
{
    Await(
        std::move(result),
        NProfiling::EmptyTagIds,
        std::move(onResult),
        std::move(onCancel));
}

template <class T>
void TParallelAwaiter::HandleResult(
    const NProfiling::TTagIdList& tagIds,
    TCallback<void(T)> onResult,
    T result)
{
    if (onResult) {
        CancelableInvoker_->Invoke(BIND(onResult, result));
    }

    HandleResponse(tagIds);
}

inline void TParallelAwaiter::HandleResult(
    const NProfiling::TTagIdList& tagIds,
    TCallback<void()> onResult)
{
    if (onResult) {
        CancelableInvoker_->Invoke(onResult);
    }

    HandleResponse(tagIds);
}

inline void TParallelAwaiter::HandleCancel(
    const NProfiling::TTagIdList& tagIds,
    TCallback<void()> onCancel)
{
    if (onCancel) {
        CancelableInvoker_->Invoke(onCancel);
    }

    HandleResponse(tagIds);
}

inline void TParallelAwaiter::HandleResponse(const NProfiling::TTagIdList& tagIds)
{
    bool fireCompleted = false;
    TClosure onComplete;
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (Canceled_ || Terminated_)
            return;

        if (Profiler_) {
            Profiler_->TimingCheckpoint(Timer_, tagIds);
        }

        ++ResponseCount_;

        fireCompleted = (ResponseCount_ == RequestCount_) && Completed_;

        if (fireCompleted) {
            onComplete = OnComplete_;
            Terminate();
        }
    }

    if (fireCompleted) {
        DoFireCompleted(std::move(onComplete));
    }
}

inline TFuture<void> TParallelAwaiter::Complete(
    TClosure onComplete,
    const NProfiling::TTagIdList& tagIds)
{
    bool fireCompleted;
    {
        TGuard<TSpinLock> guard(SpinLock_);

        YASSERT(!Completed_);
        if (Canceled_ || Terminated_) {
            return CompletedPromise_;
        }

        OnComplete_ = onComplete;
        CompletedTagIds_ = tagIds;
        Completed_ = true;

        fireCompleted = (RequestCount_ == ResponseCount_);

        if (fireCompleted) {
            Terminate();
        }
    }

    if (fireCompleted) {
        DoFireCompleted(onComplete);
    }

    return CompletedPromise_;
}

inline void TParallelAwaiter::DoFireCompleted(TClosure onComplete)
{
    auto this_ = MakeStrong(this);
    CancelableInvoker_->Invoke(BIND([this, this_, onComplete] () {
        if (onComplete) {
            onComplete.Run();
        }
        CompletedPromise_.Set();
    }));
}

inline void TParallelAwaiter::Cancel()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (Canceled_)
        return;

    CancelableContext_->Cancel();
    Canceled_ = true;
    Terminate();
}

inline int TParallelAwaiter::GetRequestCount() const
{
    return RequestCount_;
}

inline int TParallelAwaiter::GetResponseCount() const
{
    return ResponseCount_;
}

inline bool TParallelAwaiter::IsCompleted() const
{
    return Completed_;
}

inline TFuture<void> TParallelAwaiter::GetAsyncCompleted() const
{
    return CompletedPromise_;
}

inline bool TParallelAwaiter::IsCanceled() const
{
    return Canceled_;
}

inline void TParallelAwaiter::Terminate()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (Terminated_)
        return;

    if (Completed_ && Profiler_) {
        Profiler_->TimingStop(Timer_, CompletedTagIds_);
    }

    OnComplete_.Reset();
    Terminated_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
