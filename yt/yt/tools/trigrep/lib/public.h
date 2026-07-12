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
constexpr double DefaultIndexSizeFactor = 0.1;

struct ISequentialReader;
struct IRandomReader;

struct IIndexBuilderCallbacks;
struct IMatcherCallbacks;

struct IPostingCodec;

DEFINE_ENUM(ECompressionCodec,
    (Uncompressed)
    (Zstd)
);

//! Posting-list on-disk format. V2 uses per-block bit-packed groups, V3 a joint
//! interpolative code. The reader auto-detects the format from the file
//! signature; the writer picks it explicitly.
DEFINE_ENUM(EIndexFormat,
    (V2)
    (V3)
);

constexpr EIndexFormat DefaultIndexFormat = EIndexFormat::V2;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
