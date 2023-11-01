#ifndef WASM_TYPE_BUILDER_INL_H_
#error "Direct inclusion of this file is not allowed, include type_builder.h"
// For the sake of sane code completion.
#include "type_builder.h"
#endif

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

enum class EWebAssemblyValueType
{
    UintPtr = 0,
    Int64 = 1,
    Int32 = 2,
    Float32 = 3,
    Float64 = 4,
    Void = 5,
};

template <typename T>
EWebAssemblyValueType InferType();

////////////////////////////////////////////////////////////////////////////////

TWebAssemblyRuntimeType GetTypeId(bool intrinsic, EWebAssemblyValueType returnType, TRange<EWebAssemblyValueType> arguments);

template <typename THead, typename... TTail>
Y_FORCE_INLINE void InferTypesImpl(TMutableRange<EWebAssemblyValueType> range)
{
    range.Front() = InferType<THead>();
    if constexpr (sizeof...(TTail) > 0) {
        InferTypesImpl<TTail...>(range.Slice(1, range.Size()));
    }
}

template <typename... TArguments>
Y_FORCE_INLINE void InferTypes(TMutableRange<EWebAssemblyValueType> range)
{
    if constexpr (sizeof...(TArguments) > 0) {
        InferTypesImpl<TArguments...>(range);
    }
}

template <typename TResult, typename... TArguments>
Y_FORCE_INLINE TWebAssemblyRuntimeType TFunctionTypeBuilder<TResult(TArguments...)>::Get(bool intrinsic)
{
    std::array<EWebAssemblyValueType, sizeof...(TArguments)> argumentTypes;
    InferTypes<TArguments...>(MakeMutableRange(argumentTypes));
    return GetTypeId(intrinsic, InferType<TResult>(), MakeRange(argumentTypes));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
