#pragma once

#include "common.h"
#include "future.h"
#include "invoker_util.h"

#include <ytlib/profiling/profiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TParallelAwaiter
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TParallelAwaiter> TPtr;

    explicit TParallelAwaiter(
		IInvoker* invoker,
		NProfiling::TProfiler* profiler = NULL,
		const NYTree::TYPath& timerPath = "");
	explicit TParallelAwaiter(
		NProfiling::TProfiler* profiler = NULL,
		const NYTree::TYPath& timerPath = "");

    template <class T>
    void Await(
        TIntrusivePtr< TFuture<T> > result,
        TIntrusivePtr< IParamAction<T> > onResult = NULL);
	template <class T>
	void Await(
		TIntrusivePtr< TFuture<T> > result,
		const NYTree::TYPath& timerPathSuffix,
		TIntrusivePtr< IParamAction<T> > onResult = NULL);

    void Complete(IAction::TPtr onComplete = NULL);
    void Cancel();
    bool IsCanceled() const;

private:
    TSpinLock SpinLock;
    bool Canceled;
    bool Completed;
    bool Terminated;
    i32 RequestCount;
    i32 ResponseCount;
    IAction::TPtr OnComplete;
    TCancelableInvoker::TPtr CancelableInvoker;
	NProfiling::TProfiler* Profiler;
	NProfiling::TTimer Timer;

	void Init(
		IInvoker* invoker,
		NProfiling::TProfiler* profiler,
		const NYTree::TYPath& timerPath);
    void Terminate();

    template <class T>
    void OnResult(
        T result,
		const NYTree::TYPath& timerPathSuffix,
        typename IParamAction<T>::TPtr onResult);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PARALLEL_AWAITER_INL_H_
#include "parallel_awaiter-inl.h"
#undef PARALLEL_AWAITER_INL_H_
