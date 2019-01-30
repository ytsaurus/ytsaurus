#pragma once

#include <yt/core/actions/future.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TBox
{
public:
    explicit TBox(TCallback<T()> callback)
        : Value_(callback.Run())
    { }

    T Unwrap()
    {
        return std::move(Value_);
    }

    void SetPromise(TPromise<T>& promise)
    {
        promise.Set(Value_);
    }

private:
    T Value_;

};

template <>
class TBox<void>
{
public:
    explicit TBox(TClosure callback)
    {
        callback.Run();
    }

    void Unwrap()
    { }

    void SetPromise(TPromise<void>& promise)
    {
        promise.Set();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
