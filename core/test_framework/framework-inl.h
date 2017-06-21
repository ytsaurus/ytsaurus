#pragma once
#ifndef FRAMEWORK_INL_H_
#error "Direct inclusion of this file is not allowed, include framework.h"
#endif

#include <yt/core/misc/common.h>

#include <util/system/demangle.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void PrintTo(const TIntrusivePtr<T>& arg, std::ostream* os)
{
    *os << "TIntrusivePtr<"
        << CppDemangle(typeid(T).name())
        << ">@0x"
        << std::hex
        << reinterpret_cast<uintptr_t>(arg.Get())
        << std::dec
        << " [";
    ::testing::internal::UniversalPrinter<T>::Print(*arg, os);
    *os << "]";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
