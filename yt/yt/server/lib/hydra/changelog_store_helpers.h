#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TChangelogScanInfo
{
    i64 RecordCount;
    bool AtPrimaryPath = true;
};

struct TChangelogStoreScanResult
{
    int LatestChangelogId = InvalidSegmentId;
    i64 LatestChangelogRecordCount = -1;
    int LatestNonemptyChangelogId = InvalidSegmentId;
    i64 LatestNonemptyChangelogRecordCount = -1;
    int LastMutationTerm = InvalidTerm;
    i64 LastMutationSequenceNumber = -1;
    // COMPAT(danilalexeev)
    bool IsLatestNonemptyChangelogAtPrimaryPath;
};

TChangelogStoreScanResult ScanChangelogStore(
    const std::vector<int>& changelogIds,
    const std::function<TChangelogScanInfo(int changelogId)> scanInfoGetter,
    const std::function<TSharedRef(int changelogId, i64 recordId, bool atPrimaryPath)>& recordReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
