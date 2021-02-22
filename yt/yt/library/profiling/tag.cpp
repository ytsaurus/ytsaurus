#include "tag.h"
#include "yt/core/misc/hash.h"

#include <yt/core/misc/farm_hash.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void TProjectionSet::Resize(int size)
{
    Parents_.resize(size, NoTagSentinel);
    Children_.resize(size, NoTagSentinel);
    Alternative_.resize(size, NoTagSentinel);
}

void TProjectionSet::SetEnabled(bool enabled)
{
    Enabled_ = enabled;
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
        if (i == NoTagSentinel) {
            Parents_.push_back(NoTagSentinel);
        } else {
            Parents_.push_back(i + offset);
        }
    }

    for (auto i : other.Children_) {
        if (i == NoTagSentinel) {
            Children_.push_back(NoTagSentinel);
        } else {
            Children_.push_back(i + offset);
        }
    }

    for (auto i : other.Alternative_) {
        if (i == NoTagSentinel) {
            Alternative_.push_back(NoTagSentinel);
        } else {
            Alternative_.push_back(i + offset);
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
    int parentIndex = Tags_.size() + parent;
    if (parentIndex >= 0 && static_cast<size_t>(parentIndex) < Tags_.size()) {
        Parents_.push_back(parentIndex);
    } else {
        Parents_.push_back(NoTagSentinel);
    }

    Children_.push_back(NoTagSentinel);
    Alternative_.push_back(NoTagSentinel);
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

void TTagSet::AddAlternativeTag(TTag tag, int alternativeTo, int parent)
{
    int alternativeIndex = Tags_.size() + alternativeTo;

    AddTag(std::move(tag), parent);
    
    if (alternativeIndex >= 0 && static_cast<size_t>(alternativeIndex) < Tags_.size()) {
        Alternative_.back() = alternativeIndex;
    }
}

void TTagSet::AddTagWithChild(TTag tag, int child)
{
    int childIndex = Tags_.size() + child;
    AddTag(tag);
    Children_.back() = childIndex;
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
