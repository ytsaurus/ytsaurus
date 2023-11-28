#pragma once

#include <yt/yt/client/table_client/unversioned_value.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWebAssemblyValueType,
    ((UintPtr) (0))
    ((Int64)   (1))
    ((Int32)   (2))
    ((Float64) (3))
    ((Float32) (4))
    ((Void)    (5))
);

////////////////////////////////////////////////////////////////////////////////

struct TWebAssemblyRuntimeType
{
    void* Id;
};

template <class TSignature>
struct TFunctionTypeBuilder
{ };

template <typename TResult, typename... TArguments>
struct TFunctionTypeBuilder<TResult(TArguments...)>
{
    static TWebAssemblyRuntimeType Get(bool intrinsic);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NWebAssembly

#define WASM_TYPE_BUILDER_INL_H_
#include "type_builder-inl.h"
#undef WASM_TYPE_BUILDER_INL_H_
