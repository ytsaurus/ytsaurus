#include "profiling_helpers.h"

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTagSet GetThreadTags(
    bool enableProfiling,
    const TString& threadName)
{
    TTagSet tags;
    if (enableProfiling) {
        tags.AddTag(std::pair<TString, TString>("thread", threadName));
    }
    return tags;
}

TTagSet GetBucketTags(
    bool enableProfiling,
    const TString& threadName,
    const TString& bucketName)
{
    TTagSet tags;

    if (enableProfiling) {
        tags.AddTag(std::pair<TString, TString>("thread", threadName));
        tags.AddTag(std::pair<TString, TString>("bucket", bucketName), -1);
    }

    return tags;
}

std::vector<TTagSet> GetBucketsTags(
    bool enableProfiling,
    const TString& threadName,
    const std::vector<TString>& bucketNames)
{
    std::vector<TTagSet> result;
    for (const auto& bucketName : bucketNames) {
        result.emplace_back(GetBucketTags(enableProfiling, threadName, bucketName));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

