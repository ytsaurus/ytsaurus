#pragma once

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

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
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
