#pragma once

#include "public.h"

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NDI {

////////////////////////////////////////////////////////////////////////////////

//! TCyclePtr is wrapper around TIntrusivePtr<T> that is used to break dependency cycles.
template <class T>
class TCyclePtr
{
public:
    using TUnderlying = T;

    struct TState final
    {
        TIntrusivePtr<T> Ptr;
    };

    TCyclePtr(TIntrusivePtr<TState> state)
        : State_(std::move(state))
    { }

    TCyclePtr() = default;

    T* operator -> () const
    {
        return State_->Ptr.Get();
    }

    T* Get() const
    {
        return State_->Ptr.Get();
    }

    operator TIntrusivePtr<T> () const
    {
        return State_->Ptr;
    }

private:
    TIntrusivePtr<TState> State_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDI
