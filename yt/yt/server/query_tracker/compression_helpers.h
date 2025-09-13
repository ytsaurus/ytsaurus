#pragma once

#include <yt/yt/core/misc/common.h>

#include <util/generic/size_literals.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 MaxDyntableStringSize = 16_MB;

////////////////////////////////////////////////////////////////////////////////

TString Compress(const TString& data, std::optional<ui64> maxCompressedStringSize = std::nullopt, int quality = 9);
TString Decompress(const TString& data);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
