#pragma once

#include "public.h"

#include <yt/yt/core/misc/common.h>

namespace NYT::NTCMalloc {

////////////////////////////////////////////////////////////////////////////////

struct TMemoryProfileCleanupResult
{
    int RemovedDumpCount = 0;
    int RemovedOrphanFileCount = 0;
    int FailedRemovalCount = 0;
    i64 RemovedByteCount = 0;
};

//! Applies retention to complete memory profile dump sets and removes expired orphan files.
//! Files outside #path, nested files, symlinks, and unrelated files are never removed.
TMemoryProfileCleanupResult CleanupMemoryProfiles(
    const std::string& path,
    const std::optional<std::string>& filenameSuffix,
    const TMemoryProfileRetentionConfigPtr& retentionConfig,
    TInstant now = TInstant::Now());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTCMalloc
