#include "tag.h"
#include "yt/core/misc/hash.h"

#include <yt/core/misc/farm_hash.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void TProjectionSet::Resize(int size)
{
    Parents_.resize(size, NoParentSentinel);
}

void TTagSet::Append(const TTagSet& other)
{
    auto offset = Tags_.size();

    for (const auto& tag : other.Tags_) {
        Tags_.push_back(tag);
    }

    for (auto i : other.Required_) {
        Required_.push_back(offset + i);
    }

    for (auto i : other.Excluded_) {
        Excluded_.push_back(offset + i);
    }

    for (auto i : other.Parents_) {
        if (i == NoParentSentinel) {
            Parents_.push_back(NoParentSentinel);
        } else {
            Parents_.push_back(i + offset);
        }
    }
}

TTagSet TTagSet::WithTag(TTag tag, int parent)
{
    auto copy = *this;
    copy.AddTag(std::move(tag), parent);
    return copy;
}

void TTagSet::AddTag(TTag tag, int parent)
{
    if (parent < 0) {
        YT_VERIFY(static_cast<size_t>(-parent) <= Tags_.size());
        Parents_.push_back(Tags_.size() - parent);
    } else {
        Parents_.push_back(NoParentSentinel);
    }

    Tags_.emplace_back(std::move(tag));
}

void TTagSet::AddRequiredTag(TTag tag, int parent)
{
    Required_.push_back(Tags_.size());
    AddTag(std::move(tag), parent);
}

void TTagSet::AddExcludedTag(TTag tag, int parent)
{
    Excluded_.push_back(Tags_.size());
    AddTag(std::move(tag), parent);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

size_t THash<NYT::NProfiling::TTagIndexList>::operator()(const NYT::NProfiling::TTagIndexList& list) const
{
    size_t result = 0;
    for (auto index : list) {
        NYT::HashCombine(result, index);
    }
    return result;
}
