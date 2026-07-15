#pragma once

#include "requests.h"

#include <yt/yt/core/misc/error.h>

#include <concepts>
#include <vector>

namespace NYT::NKafka {

////////////////////////////////////////////////////////////////////////////////

//! Collects all non-None error codes from Kafka response.
template <typename TResponse>
std::vector<EErrorCode> CollectErrorCodes(const TResponse& rsp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafka

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
