#pragma once

#include "action.h"
#include "invoker.h"

#include "../misc/ptr.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template<class T>
class TAsyncResult
    : public TRefCountedBase
{
    volatile bool IsSet;
    T Value;
    mutable TSpinLock SpinLock;
    mutable THolder<Event> ReadyEvent;

    yvector<typename IParamAction<T>::TPtr> Subscribers;

public:
    typedef TIntrusivePtr<TAsyncResult> TPtr;

    TAsyncResult();
    explicit TAsyncResult(T value);

    void Set(T value);

    T Get() const;

    bool TryGet(T* value) const;

    void Subscribe(typename IParamAction<T>::TPtr action);

    template<class TOther>
    TIntrusivePtr< TAsyncResult<TOther> > Apply(
        TIntrusivePtr< IParamFunc<T, TOther> > func);

    template<class TOther>
    TIntrusivePtr< TAsyncResult<TOther> > Apply(
        TIntrusivePtr< IParamFunc<T, TIntrusivePtr< TAsyncResult<TOther>  > > > func);
};

////////////////////////////////////////////////////////////////////////////////
// TODO: move to separate file

class TParallelAwaiter
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TParallelAwaiter> TPtr;

    TParallelAwaiter(IInvoker::TPtr invoker = TSyncInvoker::Get());

    template<class T>
    void Await(
        TIntrusivePtr< TAsyncResult<T> > result,
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
    IInvoker::TPtr UserInvoker;
    TCancelableInvoker::TPtr CancelableInvoker;

    void Terminate();

    template<class T>
    void OnResult(
        T result,
        typename IParamAction<T>::TPtr onResult);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ASYNC_RESULT_INL_H_
#include "async_result-inl.h"
#undef ASYNC_RESULT_INL_H_
