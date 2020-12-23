#pragma once

#include <yt/yt/library/profiling/tag.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTagSet GetThreadTags(const TString& threadName);

NProfiling::TTagSet GetBucketTags(
    const TString& threadName,
    const TString& bucketName);

std::vector<NProfiling::TTagSet> GetBucketsTags(
    const TString& threadName,
    const std::vector<TString>& bucketNames);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

