#include "profiling_helpers.h"

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTagSet GetThreadTags(
    const TString& threadName)
{
    TTagSet tags;
    tags.AddTag(std::pair<TString, TString>("thread", threadName));
    return tags;
}

TTagSet GetBucketTags(
    const TString& threadName,
    const TString& bucketName)
{
    TTagSet tags;

    tags.AddTag(std::pair<TString, TString>("thread", threadName));
    tags.AddTag(std::pair<TString, TString>("bucket", bucketName), -1);

    return tags;
}

std::vector<TTagSet> GetBucketsTags(
    const TString& threadName,
    const std::vector<TString>& bucketNames)
{
    std::vector<TTagSet> result;
    for (const auto& bucketName : bucketNames) {
        result.emplace_back(GetBucketTags(threadName, bucketName));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

