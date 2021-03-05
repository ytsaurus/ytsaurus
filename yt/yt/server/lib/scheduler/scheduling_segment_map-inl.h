#pragma once

#ifndef SCHEDULING_SEGMENT_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include scheduling_segment_map.h"
// For the sake of sane code completion.
#include "scheduling_segment_map.h"
#endif

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsDataCenterAwareSchedulingSegment(ESchedulingSegment segment)
{
    return segment == ESchedulingSegment::LargeGpu;
}

////////////////////////////////////////////////////////////////////////////////

static inline const TDataCenter NullDataCenter = {};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
const TValue& TDataCenterAwareValue<TValue>::GetOrDefault(const TValue& defaultValue) const
{
    YT_VERIFY(!IsMultiDataCenter_);
    return GetOrDefaultImpl(NullDataCenter, defaultValue);
}

template <class TValue>
TValue& TDataCenterAwareValue<TValue>::Mutable()
{
    YT_VERIFY(!IsMultiDataCenter_);
    return MutableImpl(NullDataCenter);
}

template <class TValue>
void TDataCenterAwareValue<TValue>::Set(const TValue& value)
{
    YT_VERIFY(!IsMultiDataCenter_);
    SetImpl(NullDataCenter, value);
}

template <class TValue>
const TValue& TDataCenterAwareValue<TValue>::GetOrDefaultAt(const TDataCenter& dataCenter, const TValue& defaultValue) const
{
    YT_VERIFY(IsMultiDataCenter_);
    return GetOrDefaultImpl(dataCenter, defaultValue);
}

template <class TValue>
TValue& TDataCenterAwareValue<TValue>::MutableAt(const TDataCenter& dataCenter)
{
    YT_VERIFY(IsMultiDataCenter_);
    return MutableImpl(dataCenter);
}

template <class TValue>
void TDataCenterAwareValue<TValue>::SetAt(const TDataCenter& dataCenter, const TValue& value)
{
    YT_VERIFY(IsMultiDataCenter_);
    SetImpl(dataCenter, value);
}

template <class TValue>
TDataCenterList TDataCenterAwareValue<TValue>::GetDataCenters() const
{
    YT_VERIFY(IsMultiDataCenter_);

    TDataCenterList result;
    for (const auto& [dataCenter, _] : Map_) {
        result.push_back(dataCenter);
    }

    return result;
}

template <class TValue>
TValue TDataCenterAwareValue<TValue>::GetTotal() const
{
    YT_VERIFY(IsMultiDataCenter_);

    TValue result = {};
    for (const auto& [_, value] : Map_) {
        result += value;
    }

    return result;
}

template <class TValue>
const TValue& TDataCenterAwareValue<TValue>::GetOrDefaultImpl(const TDataCenter& dataCenter, const TValue& defaultValue) const
{
    auto it = Map_.find(dataCenter);
    return it != Map_.end() ? it->second : defaultValue;
}

template <class TValue>
TValue& TDataCenterAwareValue<TValue>::MutableImpl(const TDataCenter& dataCenter)
{
    return Map_[dataCenter];
}

template <class TValue>
void TDataCenterAwareValue<TValue>::SetImpl(const TDataCenter& dataCenter, const TValue& value)
{
    Map_[dataCenter] = value;
}

template <class TValue>
void Serialize(const TDataCenterAwareValue<TValue>& value, NYson::IYsonConsumer* consumer)
{
    using NYTree::Serialize;
    if (value.IsMultiDataCenter_) {
        Serialize(value.Map_, consumer);
    } else {
        Serialize(value.GetOrDefault(), consumer);
    }
}

template <class TValue>
void DeserializeScalarValue(TDataCenterAwareValue<TValue>& value, const NYTree::INodePtr& node)
{
    using NYTree::Deserialize;

    value.IsMultiDataCenter_ = false;
    Deserialize(value.Mutable(), node);
}

template <class TValue>
void DeserializeMultiDataCenterValue(TDataCenterAwareValue<TValue>& value, const NYTree::INodePtr& node)
{
    using NYTree::Deserialize;

    value.IsMultiDataCenter_ = true;

    // NB(eshcherbin): This workaround is used because we cannot deserialize std::optional as a map node key.
    // TODO(eshcherbin): Is it possible to add a FromStringImpl<TDataCenter> template specialization instead?
    typename TDataCenterAwareValue<TValue>::TMapForDeserialize map;
    Deserialize(map, node);

    value.Map_.clear();
    for (auto&& [dataCenter, valueAtDataCenter] : map) {
        YT_VERIFY(value.Map_.emplace(std::move(dataCenter), std::move(valueAtDataCenter)).second);
    }
}

template <class TValue>
void FormatValue(TStringBuilderBase* builder, const TDataCenterAwareValue<TValue>& value, TStringBuf format)
{
    if (value.IsMultiDataCenter_) {
        FormatValue(builder, value.Map_, format);
    } else {
        FormatValue(builder, value.GetOrDefault(), format);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TSchedulingSegmentMap<TValue>::TSchedulingSegmentMap()
{
    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        Map_[segment].IsMultiDataCenter_ = IsDataCenterAwareSchedulingSegment(segment);
    }
}

template <class TValue>
const TDataCenterAwareValue<TValue>& TSchedulingSegmentMap<TValue>::At(ESchedulingSegment segment) const
{
    return Map_[segment];
}

template <class TValue>
TDataCenterAwareValue<TValue>& TSchedulingSegmentMap<TValue>::At(ESchedulingSegment segment)
{
    return Map_[segment];
}

template <class TValue>
void Serialize(const TSchedulingSegmentMap<TValue>& map, NYson::IYsonConsumer* consumer)
{
    using NYTree::Serialize;
    Serialize(map.Map_, consumer);
}

template <class TValue>
void Deserialize(TSchedulingSegmentMap<TValue>& map, const NYTree::INodePtr& node)
{
    // NB(eshcherbin): This is almost a copy-paste from Deserialize implementation for TEnumIndexedVector.
    // However, we need it here because we have to call the correct Deserialize function for different segments.
    map = {};

    auto mapNode = node->AsMap();
    for (const auto& [stringSegment, child] : mapNode->GetChildren()) {
        auto segment = TEnumTraits<ESchedulingSegment>::FromString(DecodeEnumValue(stringSegment));
        if (IsDataCenterAwareSchedulingSegment(segment)) {
            DeserializeMultiDataCenterValue<TValue>(map.Map_[segment], child);
        } else {
            DeserializeScalarValue<TValue>(map.Map_[segment], child);
        }
    }
}

template <class TValue>
void FormatValue(TStringBuilderBase* builder, const TSchedulingSegmentMap<TValue>& map, TStringBuf format)
{
    FormatValue(builder, map.Map_, format);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
