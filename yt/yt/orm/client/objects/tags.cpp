#include "tags.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <util/generic/hash.h>

#include <google/protobuf/repeated_field.h>

////////////////////////////////////////////////////////////////////////////////

template<>
struct THash<NYT::NOrm::NClient::NObjects::TTag>
{
    size_t operator()(const NYT::NOrm::NClient::NObjects::TTag& tag) const
    {
        return THash<int>{}(tag.Underlying());
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NOrm::NClient::NObjects {

using namespace google::protobuf;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf UnknownTagName = "unknown";

////////////////////////////////////////////////////////////////////////////////

class TTagsRegistry
    : public ITagsRegistry
{
public:
    explicit TTagsRegistry(std::vector<TRawTag> tags)
    {
        for (auto& tagAndName : tags) {
            InsertOrCrash(Tags_, tagAndName.Value);
            EmplaceOrCrash(TagToName_, tagAndName.Value, tagAndName.Name);
            EmplaceOrCrash(NameToTag_, tagAndName.Name, tagAndName.Value);
        }
    }

    template <typename It>
    std::vector<TTag> FilterUnknownTags(It begin, It end) const
    {
        std::vector<TTag> result;
        for (auto it = begin; it != end; ++it) {
            if (!Tags_.contains(*it)) {
                result.push_back(*it);
            }
        }
        return result;
    }

    std::vector<TTag> FilterUnknownTags(const std::vector<TTag>& tags) const override
    {
        return FilterUnknownTags(tags.begin(), tags.end());
    }

    std::vector<TTag> FilterUnknownTags(const TTagSet& tags) const override
    {
        return FilterUnknownTags(tags.begin(), tags.end());
    }

    TStringBuf GetTagName(TTag tag) const override
    {
        if (auto it = TagToName_.find(tag); it != TagToName_.end()) {
            return it->second;
        }

        return UnknownTagName;
    }

    TTag GetTagValue(TStringBuf name) const override
    {
        if (auto it = NameToTag_.find(name); it != NameToTag_.end()) {
            return it->second;
        }

        THROW_ERROR_EXCEPTION("Unknown tag %Qv", name);
    }

    size_t GetTagCount() const override
    {
        return Tags_.size();
    }

    std::vector<TStringBuf> MakeHumanReadableTagsList(std::vector<TTag> tags) const override
    {
        return DoMakeHumanReadableTagsList(tags.begin(), tags.end());
    }

    std::vector<TStringBuf> MakeHumanReadableTagsList(TTagSet tags) const override
    {
        return DoMakeHumanReadableTagsList(tags.begin(), tags.end());
    }

    template <typename It>
    std::vector<TStringBuf> DoMakeHumanReadableTagsList(It begin, It end) const
    {
        std::vector<TStringBuf> result;
        std::transform(begin, end, std::back_inserter(result), [&] (TTag tag) {
            return GetTagName(tag);
        });
        return result;
    }

private:
    TTagSet Tags_;
    THashMap<TTag, TString> TagToName_;
    THashMap<TString, TTag> NameToTag_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ToProto(int* serialized, TTag original)
{
    *serialized = original.Underlying();
}

void FromProto(TTag* original, int serialized)
{
    *original = TTag{serialized};
}

void ToProto(google::protobuf::RepeatedField<i32>* serialized, const TTagSet& tagSet)
{
    serialized->Clear();
    serialized->Reserve(tagSet.size());
    for (const auto& tag : tagSet) {
        serialized->Add(tag.Underlying());
    }
}

void FromProto(TTagSet* tagSet, const google::protobuf::RepeatedField<i32>& serialized)
{
    tagSet->clear();
    tagSet->insert(serialized.begin(), serialized.end());
}

TTagSet TagsVectorToSet(const std::vector<i32>& tags)
{
    return {tags.begin(), tags.end()};
}

std::vector<i32> TagsSetToVector(const TTagSet& tagsSet)
{
    return {tagsSet.begin(), tagsSet.end()};
}

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TTag& tag,
    TStringBuf format)
{
    FormatValue(builder, tag.Underlying(), format);
}

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TTagSet& tagSet,
    TStringBuf format)
{
    FormatValue(builder, MakeFormattableView(tagSet, TDefaultFormatter{}), format);
}

////////////////////////////////////////////////////////////////////////////////

ITagsRegistryPtr CreateTagsRegistry(std::vector<TRawTag> tags)
{
    return New<TTagsRegistry>(std::move(tags));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
