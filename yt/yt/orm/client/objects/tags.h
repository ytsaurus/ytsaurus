#pragma once

#include "public.h"

#include <util/generic/strbuf.h>

#include <vector>

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

void ToProto(int* serialized, TTag original);
void FromProto(TTag* original, int serialized);

void ToProto(google::protobuf::RepeatedField<i32>* serialized, const TTagSet& tagSet);
void FromProto(TTagSet* tagSet, const google::protobuf::RepeatedField<i32>& serialized);

TTagSet TagsVectorToSet(const std::vector<i32>& tags);
std::vector<i32> TagsSetToVector(const TTagSet& tagsSet);

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TTag& tag,
    TStringBuf format);

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TTagSet& tagSet,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct ITagsRegistry
    : public TRefCounted
{
    virtual std::vector<TTag> FilterUnknownTags(const std::vector<TTag>& tags) const = 0;
    virtual std::vector<TTag> FilterUnknownTags(const TTagSet& tags) const = 0;
    virtual TStringBuf GetTagName(TTag tag) const = 0;
    virtual TTag GetTagValue(TStringBuf name) const = 0;
    virtual size_t GetTagCount() const = 0;

    virtual std::vector<TStringBuf> MakeHumanReadableTagsList(std::vector<TTag> tags) const = 0;
    virtual std::vector<TStringBuf> MakeHumanReadableTagsList(TTagSet tags) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITagsRegistry)

////////////////////////////////////////////////////////////////////////////////

struct TRawTag
{
    int Value;
    TString Name;
};

ITagsRegistryPtr CreateTagsRegistry(std::vector<TRawTag> tags);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
