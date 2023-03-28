#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t EstimatedValueSize = 16;

size_t EstimateSize(const TString& value);
size_t EstimateSize(const NYson::TYsonString& value);
size_t EstimateSize(i64 value);
size_t EstimateSize(TGuid value);
size_t EstimateSize(TInstant value);

template <typename T>
size_t EstimateSize(const std::optional<T>& value);

template <typename E>
    requires TEnumTraits<E>::IsEnum
size_t EstimateSize(E value);

////////////////////////////////////////////////////////////////////////////////

template <typename... Ts>
size_t EstimateSizes(Ts&&...values);

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent

#define ESTIMATE_SIZE_HELPERS_INL_H_
#include "estimate_size_helpers-inl.h"
#undef ESTIMATE_SIZE_HELPERS_INL_H_
