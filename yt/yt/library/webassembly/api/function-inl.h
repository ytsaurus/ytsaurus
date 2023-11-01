#ifndef WASM_FUNCTION_INL_H_
#error "Direct inclusion of this file is not allowed, include function.h"
// For the sake of sane code completion.
#include "function.h"
#endif

#include <array>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

union TWavmPodValue
{
    ui64 Data;
    char Padding[16];
};

Y_FORCE_INLINE void ConvertToWavmArguments(TMutableRange<TWavmPodValue> range)
{
    YT_ASSERT(range.Empty());
}

template <typename THead, typename... TTail>
Y_FORCE_INLINE void ConvertToWavmArguments(
    TMutableRange<TWavmPodValue> range,
    const THead& head,
    TTail&... tail)
{
    range[0].Data = std::bit_cast<ui64>(head);
    ConvertToWavmArguments(range.Slice(1, range.Size()), tail...);
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void WavmInvoke(
    IWebAssemblyCompartment* compartment,
    TWebAssemblyRuntimeType runtimeType,
    TCompartmentFunctionId runtimeFunction,
    TWavmPodValue* result,
    TRange<TWavmPodValue> arguments);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename TResult, typename... TArgs>
Y_FORCE_INLINE TCompartmentFunction<TResult(TArgs...)>::TCompartmentFunction(
    IWebAssemblyCompartment* compartment,
    TTypedInvokeFunction function)
    : Compartment_(compartment)
    , Function_(function)
    , RuntimeType_(TFunctionTypeBuilder<TResult(TArgs...)>::Get(/*intrinsic*/ false))
    , RuntimeFunction_(
        Compartment_
        ? Compartment_->GetFunction(std::bit_cast<size_t>(Function_))
        : nullptr)
{ }

template <typename TResult, typename... TArgs>
Y_FORCE_INLINE TResult TCompartmentFunction<TResult(TArgs...)>::operator()(TArgs... args) const
{
    static_assert(
        std::is_integral_v<TResult> || std::is_same_v<TResult, void>,
        "Non-integral result types are not supported");

    if (Compartment_) {
        std::array<TWavmPodValue, sizeof...(TArgs)> arguments;

        ConvertToWavmArguments(
            MakeMutableRange(arguments.data(), arguments.size()),
            args...);

        if constexpr (std::is_same_v<TResult, void>) {
            NDetail::WavmInvoke(
                Compartment_,
                RuntimeType_,
                RuntimeFunction_,
                nullptr,
                MakeRange(arguments.data(),
                arguments.size()));

            return;
        }

        TWavmPodValue result;
        result.Data = 0;

        NDetail::WavmInvoke(
            Compartment_,
            RuntimeType_,
            RuntimeFunction_,
            &result,
            MakeRange(arguments.data(),
            arguments.size()));

        return TResult(result.Data);
    }

    return Function_(args...);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TResult, typename... TArgs>
Y_FORCE_INLINE TCompartmentFunction<TResult(TArgs...)> PrepareFunction(
    IWebAssemblyCompartment* compartment,
    TResult(*function)(TArgs...))
{
    return TCompartmentFunction<TResult(TArgs...)>(compartment, function);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
