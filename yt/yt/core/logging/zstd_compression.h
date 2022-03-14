#pragma once

#include "public.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 MaxZstdFrameUncompressedLength = 5_MB;
constexpr const int DefaultZstdCompressionLevel = 3;

ILogCompressionCodecPtr CreateZstdCompressionCodec(int compressionLevel = DefaultZstdCompressionLevel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
