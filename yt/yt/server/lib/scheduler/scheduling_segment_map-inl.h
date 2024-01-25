#ifndef SCHEDULING_SEGMENT_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include scheduling_segment_map.h"
// For the sake of sane code completion.
#include "scheduling_segment_map.h"
#endif

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsModuleAwareSchedulingSegment(ESchedulingSegment segment)
{
    return segment == ESchedulingSegment::LargeGpu;
}

////////////////////////////////////////////////////////////////////////////////

static inline const TSchedulingSegmentModule NullModule = {};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
const TValue& TModuleAwareValue<TValue>::GetOrDefault(const TValue& defaultValue) const
{
    YT_VERIFY(!IsMultiModule_);
    return GetOrDefaultImpl(NullModule, defaultValue);
}

template <class TValue>
TValue& TModuleAwareValue<TValue>::Mutable()
{
    YT_VERIFY(!IsMultiModule_);
    return MutableImpl(NullModule);
}

template <class TValue>
void TModuleAwareValue<TValue>::Set(const TValue& value)
{
    YT_VERIFY(!IsMultiModule_);
    SetImpl(NullModule, value);
}

template <class TValue>
const TValue& TModuleAwareValue<TValue>::GetOrDefaultAt(
    const TSchedulingSegmentModule& schedulingSegmentModule,
    const TValue& defaultValue) const
{
    YT_VERIFY(IsMultiModule_);
    return GetOrDefaultImpl(schedulingSegmentModule, defaultValue);
}

template <class TValue>
TValue& TModuleAwareValue<TValue>::MutableAt(const TSchedulingSegmentModule& schedulingSegmentModule)
{
    YT_VERIFY(IsMultiModule_);
    return MutableImpl(schedulingSegmentModule);
}

template <class TValue>
void TModuleAwareValue<TValue>::SetAt(const TSchedulingSegmentModule& schedulingSegmentModule, const TValue& value)
{
    YT_VERIFY(IsMultiModule_);
    SetImpl(schedulingSegmentModule, value);
}

template <class TValue>
TSchedulingSegmentModuleList TModuleAwareValue<TValue>::GetModules() const
{
    YT_VERIFY(IsMultiModule_);

    TSchedulingSegmentModuleList result;
    for (const auto& [schedulingSegmentModule, _] : Map_) {
        result.push_back(schedulingSegmentModule);
    }

    return result;
}

template <class TValue>
TValue TModuleAwareValue<TValue>::GetTotal() const
{
    YT_VERIFY(IsMultiModule_);

    TValue result = {};
    for (const auto& [_, value] : Map_) {
        result += value;
    }

    return result;
}

template <class TValue>
const TValue& TModuleAwareValue<TValue>::GetOrDefaultImpl(const TSchedulingSegmentModule& schedulingSegmentModule, const TValue& defaultValue) const
{
    auto it = Map_.find(schedulingSegmentModule);
    return it != Map_.end() ? it->second : defaultValue;
}

template <class TValue>
TValue& TModuleAwareValue<TValue>::MutableImpl(const TSchedulingSegmentModule& schedulingSegmentModule)
{
    return Map_[schedulingSegmentModule];
}

template <class TValue>
void TModuleAwareValue<TValue>::SetImpl(const TSchedulingSegmentModule& schedulingSegmentModule, const TValue& value)
{
    Map_[schedulingSegmentModule] = value;
}

template <class TValue>
void Serialize(const TModuleAwareValue<TValue>& value, NYson::IYsonConsumer* consumer)
{
    using NYTree::Serialize;
    if (value.IsMultiModule_) {
        Serialize(value.Map_, consumer);
    } else {
        Serialize(value.GetOrDefault(), consumer);
    }
}

template <class TValue>
void DeserializeScalarValue(TModuleAwareValue<TValue>& value, const NYTree::INodePtr& node)
{
    using NYTree::Deserialize;

    value.IsMultiModule_ = false;
    Deserialize(value.Mutable(), node);
}

template <class TValue>
void DeserializeMultiModuleValue(TModuleAwareValue<TValue>& value, const NYTree::INodePtr& node)
{
    using NYTree::Deserialize;

    value.IsMultiModule_ = true;

    // NB(eshcherbin): This workaround is used because we cannot deserialize std::optional as a map node key.
    // TODO(eshcherbin): Is it possible to add a FromStringImpl<TModule> template specialization instead?
    typename TModuleAwareValue<TValue>::TMapForDeserialize map;
    Deserialize(map, node);

    value.Map_.clear();
    for (auto&& [schedulingSegmentModule, valueAtModule] : map) {
        YT_VERIFY(value.Map_.emplace(std::move(schedulingSegmentModule), std::move(valueAtModule)).second);
    }
}

template <class TValue>
void FormatValue(TStringBuilderBase* builder, const TModuleAwareValue<TValue>& value, TStringBuf format)
{
    if (value.IsMultiModule_) {
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
        Map_[segment].IsMultiModule_ = IsModuleAwareSchedulingSegment(segment);
    }
}

template <class TValue>
const TModuleAwareValue<TValue>& TSchedulingSegmentMap<TValue>::At(ESchedulingSegment segment) const
{
    return Map_[segment];
}

template <class TValue>
TModuleAwareValue<TValue>& TSchedulingSegmentMap<TValue>::At(ESchedulingSegment segment)
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
    // NB(eshcherbin): This is almost a copy-paste from Deserialize implementation for TEnumIndexedArray.
    // However, we need it here because we have to call the correct Deserialize function for different segments.
    map = {};

    auto mapNode = node->AsMap();
    for (const auto& [stringSegment, child] : mapNode->GetChildren()) {
        auto segment = ParseEnum<ESchedulingSegment>(stringSegment);
        if (IsModuleAwareSchedulingSegment(segment)) {
            DeserializeMultiModuleValue<TValue>(map.Map_[segment], child);
        } else {
            DeserializeScalarValue<TValue>(map.Map_[segment], child);
        }
    }
}

template <class TValue>
void Deserialize(TSchedulingSegmentMap<TValue>& map, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(map, ExtractTo<NYTree::INodePtr>(cursor));
}

template <class TValue>
void FormatValue(TStringBuilderBase* builder, const TSchedulingSegmentMap<TValue>& map, TStringBuf format)
{
    FormatValue(builder, map.Map_, format);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
