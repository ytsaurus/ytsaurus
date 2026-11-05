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

struct TMatcherOptions
{
    // When set, the matcher only computes the frames the index selects (see
    // TMatchStatistics) and does not scan the input at all -- useful for
    // measuring filtering power on large inputs.
    bool DryRun = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TMatchStatistics
{
    // The frames the index selects and their share of the input; computable
    // from the index alone, so they are filled in even in dry-run.
    i64 FramesTotal = 0;
    i64 BytesTotal = 0;
    i64 FramesSelected = 0;
    i64 BytesSelected = 0;

    // The scanning work; zero in dry-run.
    i64 FramesChecked = 0;
    i64 LinesRead = 0;
    i64 LinesChecked = 0;
    i64 LinesMatched = 0;
};

////////////////////////////////////////////////////////////////////////////////

TMatchStatistics RunMatcher(
    IRandomReader* reader,
    TFile* indexFile,
    const std::vector<std::string>& patterns,
    IMatcherCallbacks* callbacks,
    const TMatcherOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
