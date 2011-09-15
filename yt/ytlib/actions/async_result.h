#pragma once

#include "action.h"
#include "invoker.h"

#include "../misc/ptr.h"
#include "../misc/foreach.h"


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template<class T>
class TAsyncResult
    : public TRefCountedBase
{
    volatile bool IsSet_;
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

    bool IsSet() const;

    void Subscribe(typename IParamAction<T>::TPtr action);

    template<class TOther>
    TIntrusivePtr< TAsyncResult<TOther> > Apply(
        TIntrusivePtr< IParamFunc<T, TOther> > func);

    template<class TOther>
    TIntrusivePtr< TAsyncResult<TOther> > Apply(
        TIntrusivePtr< IParamFunc<T, TIntrusivePtr< TAsyncResult<TOther>  > > > func);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ASYNC_RESULT_INL_H_
#include "async_result-inl.h"
#undef ASYNC_RESULT_INL_H_
