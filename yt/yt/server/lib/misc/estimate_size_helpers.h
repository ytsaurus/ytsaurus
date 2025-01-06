#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <google/protobuf/message.h>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t EstimatedValueSize = 16;

size_t EstimateSize(const std::string& value);
size_t EstimateSize(const TString& value);
size_t EstimateSize(const NYson::TYsonString& value);
size_t EstimateSize(TGuid value);
size_t EstimateSize(TInstant value);
size_t EstimateSize(TDuration value);
size_t EstimateSize(const ::google::protobuf::Message& value);

template <typename I>
    requires std::integral<I>
size_t EstimateSize(const I& value);

template <typename F>
    requires std::floating_point<F>
size_t EstimateSize(const F& value);

template <typename T>
size_t EstimateSize(const std::optional<T>& value);

template <typename C>
    requires std::ranges::range<C>
size_t EstimateSize(const C& value);

template <typename E>
    requires TEnumTraits<E>::IsEnum
size_t EstimateSize(E value);

////////////////////////////////////////////////////////////////////////////////

template <typename... Ts>
size_t EstimateSizes(Ts&&...values);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer

#define ESTIMATE_SIZE_HELPERS_INL_H_
#include "estimate_size_helpers-inl.h"
#undef ESTIMATE_SIZE_HELPERS_INL_H_
