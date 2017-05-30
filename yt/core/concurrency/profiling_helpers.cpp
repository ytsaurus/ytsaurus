#include "profiling_helpers.h"
#include <yt/core/profiling/profile_manager.h>

namespace NYT {
namespace NConcurrency {

using namespace NProfiling;

///////////////////////////////////////////////////////////////////////////////

TTagIdList GetThreadTagIds(
    bool enableProfiling,
    const Stroka& threadName)
{
    TTagIdList result;
    if (enableProfiling) {
        auto* profilingManager = TProfileManager::Get();
        result.push_back(profilingManager->RegisterTag("thread", threadName));
    }
    return result;
}

TTagIdList GetBucketTagIds(
    bool enableProfiling,
    const Stroka& threadName,
    const Stroka& bucketName)
{
    TTagIdList result;
    if (enableProfiling) {
        auto* profilingManager = TProfileManager::Get();
        result.emplace_back(profilingManager->RegisterTag("thread", threadName));
        result.emplace_back(profilingManager->RegisterTag("bucket", bucketName));
    }
    return result;
}

std::vector<TTagIdList> GetBucketsTagIds(
    bool enableProfiling,
    const Stroka& threadName,
    const std::vector<Stroka>& bucketNames)
{
    std::vector<TTagIdList> result;
    for (const auto& bucketName : bucketNames) {
        result.emplace_back(GetBucketTagIds(enableProfiling, threadName, bucketName));
    }
    return result;
}

TTagIdList GetInvokerTagIds(const Stroka& invokerName)
{
    TTagIdList result;
    auto* profilingManager = TProfileManager::Get();
    result.push_back(profilingManager->RegisterTag("invoker", invokerName));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

