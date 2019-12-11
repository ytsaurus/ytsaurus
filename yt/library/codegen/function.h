#pragma once

#include "public.h"

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

template <class R, class... TArgs>
class TCGFunction<R(TArgs...)>
{
private:
    typedef R (TType)(TArgs...);

public:
    TCGFunction() = default;

    TCGFunction(uint64_t functionAddress, TCGModulePtr&& module)
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

private:
    TType* FunctionPtr_ = nullptr;
    TCGModulePtr Module_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
