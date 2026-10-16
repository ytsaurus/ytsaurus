#ifndef POINTER_INL_H_
#error "Direct inclusion of this file is not allowed, include pointer.h"
// For the sake of sane code completion.
#include "pointer.h"
#endif

#include <yt/yt/library/numeric/util.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T* PtrFromVM(IWebAssemblyCompartment* compartment, T* data, size_t length)
{
    if (compartment) {
        return BitCast<T*>(BitCast<uintptr_t>(
            compartment->GetHostPointer(BitCast<uintptr_t>(data), sizeof(T) * length)));
    }

    return data;
}

template <typename T>
T* PtrToVM(IWebAssemblyCompartment* compartment, T* data, size_t length)
{
    if (compartment) {
        Y_UNUSED(length); // TODO(dtorilov): Check bounds.
        return BitCast<T*>(compartment->GetCompartmentOffset(
            BitCast<void*>(BitCast<uintptr_t>(data))));
    }

    return data;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
Y_FORCE_INLINE T* ConvertPointer(
    T* offset,
    EAddressSpace sourceAddressSpace,
    EAddressSpace destinationAddressSpace,
    size_t length)
{
    auto* compartment = GetCurrentCompartment();
    if (!compartment) {
        return offset;
    }

    if (sourceAddressSpace == EAddressSpace::WebAssembly && destinationAddressSpace == EAddressSpace::Host) {
        return PtrFromVM(compartment, offset, length);
    }

    if (sourceAddressSpace == EAddressSpace::Host && destinationAddressSpace == EAddressSpace::WebAssembly) {
        return PtrToVM(compartment, offset, length);
    }

    if (sourceAddressSpace == EAddressSpace::Host && destinationAddressSpace == EAddressSpace::Host) {
        return offset;
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
