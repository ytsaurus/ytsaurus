#pragma once

#include <util/system/types.h>

#include <util/generic/size_literals.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

constexpr auto DefaultZstdExtension = ".zst";
constexpr auto DefaultIndexExtension = ".trindex";

constexpr i64 DefaultUncompressedFrameSize = 1_MBs;
constexpr i64 DefaultChunkSize = 1_GBs;
constexpr i64 DefaultBlockSize = 1_MBs;
constexpr i64 DefaultIndexSegmentSize = 64_KBs;
constexpr int DefaultBitsPerLineFingerprint = 8;
constexpr double DefaultIndexSizeFactor = 0.1;

struct ISequentialReader;
struct IRandomReader;

struct IIndexBuilderCallbacks;
struct IMatcherCallbacks;

DEFINE_ENUM(ECompressionCodec,
    (Uncompressed)
    (Zstd)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
