#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

template <class TSignature>
class TCGFunction;

template <class R, class... TArgs>
class TCGFunction<R(TArgs...)>
{
private:
    typedef R (TType)(TArgs...);

public:
    TCGFunction() = default;

    TCGFunction(uint64_t functionAddress, TRefCountedPtr module)
        : FunctionPtr_(reinterpret_cast<TType*>(functionAddress))
        , Module_(std::move(module))
    { }

    R operator() (TArgs... args) const
    {
        return FunctionPtr_(args...);
    }

    explicit operator bool() const
    {
        return FunctionPtr_ != nullptr;
    }

    TType* Get() const
    {
        return FunctionPtr_;
    }

private:
    TType* FunctionPtr_ = nullptr;
    TRefCountedPtr Module_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
