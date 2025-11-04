#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <array>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, TrigrepLogger, "Trigrep");

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TTrigram, ui32);

// NB: Turning these into strong typedefs is a real pain.
using TPosting = ui32;
using TLineFingerprint = ui8;

constexpr auto TotalTrigramCount = 1ULL << 24;
constexpr auto MaxBlocksPerChunk = 1ULL << 10;

constexpr int BitsPerLineFingerprint = 8;
constexpr ui32 LineFingerprintMask = 0xff;

constexpr ui8 MaxShortBitpackedTag = 47;
constexpr ui8 MaxLongBitpackedTag = 55;
constexpr ui8 BitmapGroupTag = 60;
constexpr ui8 GroupSize1Tag = 61;
constexpr ui8 GroupSize2Tag = 62;
constexpr ui8 TerminatorGroupTag = 63;

// (2 ** BitsPerLineFingerprint) / 8 == 32
static constexpr int LineFingerprintBitmapByteSize = 32;
using TLineFingerprintBitmap = std::array<ui8, LineFingerprintBitmapByteSize>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep

