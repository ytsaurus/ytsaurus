#pragma once

#include "private.h"

#include <util/system/unaligned_mem.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

TLineFingerprint GetLineFingerprint(TStringBuf line);

TPosting MakePosting(ui32 blockIndex, TLineFingerprint lineFingerprint);
ui32 BlockIndexFromPosting(TPosting posting);
TLineFingerprint LineFingerprintFromPosting(TPosting posting);

//! Assumes 4 (!) bytes are readable.
TTrigram ReadTrigram(const char* src);
TTrigram PackTrigram(char ch1, char ch2, char ch3);
std::array<char, 3> UnpackTrigram(TTrigram trigram);

std::unique_ptr<IInputStream> CreateFrameInput(
    IInputStream* underlying,
    size_t frameLength);

std::vector<std::pair<int, int>> ComputeMatchingRanges(
    TStringBuf line,
    const std::vector<std::string>& patterns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
