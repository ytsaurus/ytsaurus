#pragma once

#include "public.h"

#include <yt/yt/ytlib/scheduler/job_resources.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsModuleAwareSchedulingSegment(ESchedulingSegment segment);

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TModuleAwareValue;

template <class TValue>
void Serialize(const TModuleAwareValue<TValue>& value, NYson::IYsonConsumer* consumer);

template <class TValue>
void DeserializeScalarValue(TModuleAwareValue<TValue>& value, const NYTree::INodePtr& node);

template <class TValue>
void DeserializeMultiModuleValue(TModuleAwareValue<TValue>& value, const NYTree::INodePtr& node);

template <class TValue>
void FormatValue(TStringBuilderBase* builder, const TModuleAwareValue<TValue>& value, TStringBuf /*format*/);

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TSchedulingSegmentMap;

template <class TValue>
void Serialize(const TSchedulingSegmentMap<TValue>& map, NYson::IYsonConsumer* consumer);

template <class TValue>
void Deserialize(TSchedulingSegmentMap<TValue>& map, const NYTree::INodePtr& node);

template <class TValue>
void Deserialize(TSchedulingSegmentMap<TValue>& map, NYson::TYsonPullParserCursor* cursor);

template <class TValue>
void FormatValue(TStringBuilderBase* builder, const TSchedulingSegmentMap<TValue>& map, TStringBuf /*format*/);

////////////////////////////////////////////////////////////////////////////////

using TSchedulingSegmentModule = std::optional<TString>;
using TSchedulingSegmentModuleList = std::vector<TSchedulingSegmentModule>;

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TModuleAwareValue
{
public:
    [[nodiscard]] const TValue& GetOrDefault(const TValue& defaultValue = {}) const;
    TValue& Mutable();
    void Set(const TValue& value);

    [[nodiscard]] const TValue& GetOrDefaultAt(
        const TSchedulingSegmentModule& schedulingSegmentModule,
        const TValue& defaultValue = {}) const;
    TValue& MutableAt(const TSchedulingSegmentModule& schedulingSegmentModule);
    void SetAt(const TSchedulingSegmentModule& schedulingSegmentModule, const TValue& value);
    [[nodiscard]] TSchedulingSegmentModuleList GetModules() const;
    [[nodiscard]] TValue GetTotal() const;

private:
    bool IsMultiModule_ = false;

    using TMap = THashMap<TSchedulingSegmentModule, TValue>;
    using TMapForDeserialize = THashMap<TString, TValue>;
    TMap Map_;

    [[nodiscard]] const TValue& GetOrDefaultImpl(
        const TSchedulingSegmentModule& schedulingSegmentModule,
        const TValue& defaultValue) const;
    TValue& MutableImpl(const TSchedulingSegmentModule& schedulingSegmentModule);
    void SetImpl(const TSchedulingSegmentModule& schedulingSegmentModule, const TValue& value);

    friend void Serialize<>(const TModuleAwareValue& value, NYson::IYsonConsumer* consumer);
    friend void DeserializeScalarValue<>(TModuleAwareValue& value, const NYTree::INodePtr& node);
    friend void DeserializeMultiModuleValue<>(TModuleAwareValue& value, const NYTree::INodePtr& node);
    friend void FormatValue<>(TStringBuilderBase* builder, const TModuleAwareValue& value, TStringBuf format);

    friend class TSchedulingSegmentMap<TValue>;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TSchedulingSegmentMap
{
public:
    TSchedulingSegmentMap();

    [[nodiscard]] const TModuleAwareValue<TValue>& At(ESchedulingSegment segment) const;
    TModuleAwareValue<TValue>& At(ESchedulingSegment segment);

private:
    TEnumIndexedArray<ESchedulingSegment, TModuleAwareValue<TValue>> Map_;

    friend void Serialize<>(const TSchedulingSegmentMap& map, NYson::IYsonConsumer* consumer);
    friend void Deserialize<>(TSchedulingSegmentMap& map, const NYTree::INodePtr& node);
    friend void Deserialize<>(TSchedulingSegmentMap& map, NYson::TYsonPullParserCursor* cursor);
    friend void FormatValue<>(TStringBuilderBase* builder, const TSchedulingSegmentMap& map, TStringBuf format);
};

////////////////////////////////////////////////////////////////////////////////

using TSegmentToResourceAmount = TSchedulingSegmentMap<double>;
using TSegmentToFairShare = TSchedulingSegmentMap<double>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

#define SCHEDULING_SEGMENT_MAP_INL_H_
#include "scheduling_segment_map-inl.h"
#undef SCHEDULING_SEGMENT_MAP_INL_H_
