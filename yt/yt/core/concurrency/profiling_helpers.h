#pragma once

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTagIdList GetThreadTagIds(
    bool enableProfiling,
    const TString& threadName);

NProfiling::TTagIdList GetBucketTagIds(
    bool enableProfiling,
    const TString& threadName,
    const TString& bucketName);

std::vector<NProfiling::TTagIdList> GetBucketsTagIds(
    bool enableProfiling,
    const TString& threadName,
    const std::vector<TString>& bucketNames);

NProfiling::TTagIdList GetInvokerTagIds(const TString& invokerName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

