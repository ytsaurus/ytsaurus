#ifndef POINTER_INL_H_
#error "Direct inclusion of this file is not allowed, include pointer.h"
// For the sake of sane code completion.
#include "pointer.h"
#endif

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

template <>
Y_FORCE_INLINE char* ConvertPointerFromWasmToHost(char* data, size_t length)
{
    if (auto* compartment = GetCurrentCompartment()) {
        return static_cast<char*>(
            compartment->GetHostPointer(
                std::bit_cast<uintptr_t>(data),
                length));
    }

    return data;
}

template <typename T>
T* ConvertPointerFromWasmToHost(T* data, size_t length)
{
    return std::bit_cast<T*>(
        ConvertPointerFromWasmToHost(
            std::bit_cast<char*>(data),
            sizeof(T) * length));
}

template <typename T>
T* ConvertPointerFromWasmToHost(const T* data, size_t length)
{
    return std::bit_cast<T*>(
        ConvertPointerFromWasmToHost(
            std::bit_cast<char*>(data),
            sizeof(T) * length));
}

////////////////////////////////////////////////////////////////////////////////

template <>
Y_FORCE_INLINE char* ConvertPointerFromHostToWasm(char* data, size_t length)
{
    if (auto* compartment = GetCurrentCompartment()) {
        Y_UNUSED(length); // TODO(dtorilov): Check bounds.
        return std::bit_cast<char*>(compartment->GetCompartmentOffset(static_cast<void*>(data)));
    }

    return data;
}

template <typename T>
T* ConvertPointerFromHostToWasm(T* data, size_t length)
{
    return std::bit_cast<T*>(
        ConvertPointerFromHostToWasm(
            std::bit_cast<char*>(data),
            sizeof(T) * length));
}

template <typename T>
T* ConvertPointerFromHostToWasm(const T* data, size_t length)
{
    return std::bit_cast<T*>(
        ConvertPointerFromHostToWasm(
            std::bit_cast<char*>(data),
            sizeof(T) * length));
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
Y_FORCE_INLINE T* ConvertPointer(
    T* offset,
    EAddressSpace sourceAddressSpace,
    EAddressSpace destinationAddressSpace,
    size_t length)
{
    if (!HasCurrentCompartment()) {
        return offset;
    }

    if (sourceAddressSpace == EAddressSpace::WebAssembly && destinationAddressSpace == EAddressSpace::Host) {
        return ConvertPointerFromWasmToHost(offset, length);
    }

    if (sourceAddressSpace == EAddressSpace::Host && destinationAddressSpace == EAddressSpace::WebAssembly) {
        return ConvertPointerFromHostToWasm(offset, length);
    }

    if (sourceAddressSpace == EAddressSpace::Host && destinationAddressSpace == EAddressSpace::Host) {
        return offset;
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
