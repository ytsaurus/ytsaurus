#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

size_t EstimateSize(const TString& s);
size_t EstimateSize(const NYson::TYsonString& s);
size_t EstimateSize(i64);
size_t EstimateSize(TGuid id);
size_t EstimateSize(TInstant);

template <typename T>
size_t EstimateSize(const std::optional<T>& v);

////////////////////////////////////////////////////////////////////////////////

size_t EstimateSizes();

template <typename T, typename... U>
size_t EstimateSizes(T&& t, U&& ... u);

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent

#define ESTIMATE_SIZE_HELPERS_INL_H_
#include "estimate_size_helpers-inl.h"
#undef ESTIMATE_SIZE_HELPERS_INL_H_
