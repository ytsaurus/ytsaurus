#ifndef POSITION_INDEPENDENT_VALUE_TRANSFER_INL_H_
#error "Direct inclusion of this file is not allowed, include position_independent_value_transfer.h"
// For the sake of sane code completion.
#include "position_independent_value_transfer.h"
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

inline TMutablePIValueRange AllocatePIValueRange(TExpressionContext* context, int valueCount, NWebAssembly::EAddressSpace where)
{
    auto* data = context->AllocateAligned(sizeof(TPIValue) * valueCount, where);
    return TMutablePIValueRange(
        reinterpret_cast<TPIValue*>(data),
        static_cast<size_t>(valueCount));
}

inline void CapturePIValue(
    NWebAssembly::IWebAssemblyCompartment* compartment,
    TExpressionContext* context,
    TPIValue* value)
{
    auto* valueAtHost = PtrFromVM(compartment, value);
    if (IsStringLikeType(valueAtHost->Type)) {
        auto* dataCopy = context->AllocateUnaligned(valueAtHost->Length, NWebAssembly::EAddressSpace::WebAssembly);
        valueAtHost = PtrFromVM(compartment, value); // NB: Possible reallocation.
        auto* dataCopyAtHost = PtrFromVM(compartment, dataCopy, valueAtHost->Length);
        ::memcpy(dataCopyAtHost, valueAtHost->AsStringBuf().data(), valueAtHost->Length);
        valueAtHost->SetStringPosition(dataCopyAtHost);
    }
}

////////////////////////////////////////////////////////////////////////////////

// NB(dtorilov): in WebAssembly case this function should use compartment's memory base.
template <class TNonPI>
TBorrowingPIValueGuard<TNonPI> BorrowFromNonPI(TNonPI value)
{
    return TBorrowingPIValueGuard<TNonPI>(value);
}

// NB(dtorilov): in WebAssembly case this function should use compartment's memory base.
template <class TPI>
TBorrowingNonPIValueGuard<TPI> BorrowFromPI(TPI value)
{
    return TBorrowingNonPIValueGuard<TPI>(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
