#pragma once

#include "public.h"

#include <library/cpp/yt/memory/range.h>

#include <util/system/file.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

struct IMatcherCallbacks
{
    virtual ~IMatcherCallbacks() = default;

    virtual void OnMatch(
        i64 lineIndex,
        TStringBuf line,
        TRange<std::pair<int, int>> matchingRanges) = 0;
};

////////////////////////////////////////////////////////////////////////////////

void RunMatcher(
    IRandomReader* reader,
    TFile* indexFile,
    const std::vector<std::string>& patterns,
    IMatcherCallbacks* callbacks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
