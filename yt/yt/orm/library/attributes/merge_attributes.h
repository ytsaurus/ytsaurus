#pragma once

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/orm/library/mpl/projection.h>

#include <library/cpp/yt/yson_string/string.h>

#include <ranges>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

struct TAttributeValue
{
    NYPath::TYPath Path;
    NYson::TYsonString Value;
    bool IsEtc = false;
};

////////////////////////////////////////////////////////////////////////////////

template <std::ranges::range TRange, class TPathProj, class TIsEtcProj>
void ValidateSortedPaths(const TRange& paths, TPathProj pathProj, TIsEtcProj etcProj);

template <
    typename TType,
    std::invocable<TType> TPathProj = std::identity,
    std::predicate<TType> TKeepFixedProj = NMpl::TConstantProjection<bool, false>>
void SortAndRemoveNestedPaths(std::vector<TType>& collection, TPathProj pathProj = {}, TKeepFixedProj opaqueProj = {});

////////////////////////////////////////////////////////////////////////////////

class TMergeAttributesHelper
{
public:
    TMergeAttributesHelper(NYson::IYsonConsumer* consumer);

    // Path should be provided in lexicographical order and
    // validated through `ValidateSortedPath()` call.
    void ToNextPath(NYPath::TYPathBuf path, bool isEtc);

    void Finalize();

private:
    NYson::IYsonConsumer* Consumer_;
    std::vector<TString> PathToCurrentMap_;
};

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString MergeAttributes(
    std::vector<TAttributeValue> attributeValues,
    NYson::EYsonFormat format = NYson::EYsonFormat::Binary);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes

#define MERGE_ATTRIBUTES_INL_H_
#include "merge_attributes-inl.h"
#undef MERGE_ATTRIBUTES_INL_H_
