#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/framing/format.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <typename TProto>
TErrorOr<std::vector<TProto>> UnpackBatch(
    TStringBuf rawData Y_LIFETIME_BOUND,
    const EFramingFormat framingFormat);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define UNPACKER_INL_H_
#include "unpacker-inl.h"
#undef UNPACKER_INL_H_
