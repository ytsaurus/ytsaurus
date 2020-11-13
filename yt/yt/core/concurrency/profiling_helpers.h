#pragma once

#include <yt/yt/library/profiling/tag.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTagSet GetThreadTags(
    bool enableProfiling,
    const TString& threadName);

NProfiling::TTagSet GetBucketTags(
    bool enableProfiling,
    const TString& threadName,
    const TString& bucketName);

std::vector<NProfiling::TTagSet> GetBucketsTags(
    bool enableProfiling,
    const TString& threadName,
    const std::vector<TString>& bucketNames);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

