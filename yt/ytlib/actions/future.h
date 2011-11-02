#pragma once

#include "common.h"
#include "action.h"

#include <util/system/event.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template<class T>
class TFuture
    : public TRefCountedBase
{
    volatile bool IsSet_;
    T Value;
    mutable TSpinLock SpinLock;
    mutable THolder<Event> ReadyEvent;

    yvector<typename IParamAction<T>::TPtr> Subscribers;

public:
    typedef TIntrusivePtr<TFuture> TPtr;

    TFuture();
    explicit TFuture(T value);

    void Set(T value);

    T Get() const;

    bool TryGet(T* value) const;

    bool IsSet() const;

    void Subscribe(typename IParamAction<T>::TPtr action);

    template<class TOther>
    TIntrusivePtr< TFuture<TOther> > Apply(
        TIntrusivePtr< IParamFunc<T, TOther> > func);

    template<class TOther>
    TIntrusivePtr< TFuture<TOther> > Apply(
        TIntrusivePtr< IParamFunc<T, TIntrusivePtr< TFuture<TOther>  > > > func);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define FUTURE_INL_H_
#include "future-inl.h"
#undef FUTURE_INL_H_
