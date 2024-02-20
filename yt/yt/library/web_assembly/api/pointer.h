#pragma once

#include "compartment.h"

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAddressSpace,
    (Host)
    (WebAssembly)
);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
Y_FORCE_INLINE T* ConvertPointerFromWasmToHost(T* data, size_t length = 1);

template <typename T>
Y_FORCE_INLINE T* ConvertPointerFromWasmToHost(const T* data, size_t length = 1);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
Y_FORCE_INLINE T* ConvertPointerFromHostToWasm(T* data, size_t length = 1);

template <typename T>
Y_FORCE_INLINE T* ConvertPointerFromHostToWasm(const T* data, size_t length = 1);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
Y_FORCE_INLINE T* ConvertPointer(
    T* offset,
    EAddressSpace sourceAddressSpace,
    EAddressSpace destinationAddressSpace,
    size_t length = 1);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly

#define POINTER_INL_H_
#include "pointer-inl.h"
#undef POINTER_INL_H_
