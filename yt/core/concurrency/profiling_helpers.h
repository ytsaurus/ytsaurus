#pragma once

#include <yt/core/profiling/profile_manager.h>

namespace NYT {
namespace NConcurrency {

///////////////////////////////////////////////////////////////////////////////

NProfiling::TTagIdList GetThreadTagIds(
    bool enableProfiling,
    const Stroka& threadName);

NProfiling::TTagIdList GetBucketTagIds(
    bool enableProfiling,
    const Stroka& threadName,
    const Stroka& bucketName);

std::vector<NProfiling::TTagIdList> GetBucketsTagIds(
    bool enableProfiling,
    const Stroka& threadName,
    const std::vector<Stroka>& bucketNames);

NProfiling::TTagIdList GetInvokerTagIds(const Stroka& invokerName);

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

