#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TChangelogStoreScanResult
{
    int LatestChangelogId = InvalidSegmentId;
    i64 LatestChangelogRecordCount = -1;
    int LatestNonemptyChangelogId = InvalidSegmentId;
    i64 LatestNonemptyChangelogRecordCount = -1;
    int LastMutationTerm = InvalidTerm;
    i64 LastMutationSequenceNumber = -1;
};

struct TChangelogStoreScanDescriptor
{
    int Id;
    i64 RecordCount;
};

TChangelogStoreScanResult ScanChangelogStore(
    const std::vector<TChangelogStoreScanDescriptor>& descriptors,
    const std::function<TSharedRef(int changelogId, i64 recordId)>& recordReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
