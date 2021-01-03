#pragma once

#include "public.h"

#include <yt/core/misc/small_vector.h>

#include <yt/ytlib/scheduler/job_resources.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsDataCenterAwareSchedulingSegment(ESchedulingSegment segment);

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TDataCenterAwareValue;

template <class TValue>
void Serialize(const TDataCenterAwareValue<TValue>& value, NYson::IYsonConsumer* consumer);

template <class TValue>
void DeserializeScalarValue(TDataCenterAwareValue<TValue>& value, const NYTree::INodePtr& node);

template <class TValue>
void DeserializeMultiDataCenterValue(TDataCenterAwareValue<TValue>& value, const NYTree::INodePtr& node);

template <class TValue>
void FormatValue(TStringBuilderBase* builder, const TDataCenterAwareValue<TValue>& value, TStringBuf /* format */);

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TSchedulingSegmentMap;

template <class TValue>
void Serialize(const TSchedulingSegmentMap<TValue>& map, NYson::IYsonConsumer* consumer);

template <class TValue>
void Deserialize(TSchedulingSegmentMap<TValue>& map, const NYTree::INodePtr& node);

template <class TValue>
void FormatValue(TStringBuilderBase* builder, const TSchedulingSegmentMap<TValue>& map, TStringBuf /* format */);

////////////////////////////////////////////////////////////////////////////////

using TDataCenter = std::optional<TString>;
using TDataCenterList = std::vector<TDataCenter>;

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TDataCenterAwareValue
{
public:
    [[nodiscard]] const TValue& GetOrDefault(const TValue& defaultValue = {}) const;
    TValue& Mutable();
    void Set(const TValue& value);

    [[nodiscard]] const TValue& GetOrDefaultAt(const TDataCenter& dataCenter, const TValue& defaultValue = {}) const;
    TValue& MutableAt(const TDataCenter& dataCenter);
    void SetAt(const TDataCenter& dataCenter, const TValue& value);
    [[nodiscard]] TDataCenterList GetDataCenters() const;
    [[nodiscard]] TValue GetTotal() const;

private:
    bool IsMultiDataCenter_ = false;

    using TMap = THashMap<TDataCenter, TValue>;
    using TMapForDeserialize = THashMap<TString, TValue>;
    TMap Map_;

    [[nodiscard]] const TValue& GetOrDefaultImpl(const TDataCenter& dataCenter, const TValue& defaultValue) const;
    TValue& MutableImpl(const TDataCenter& dataCenter);
    void SetImpl(const TDataCenter& dataCenter, const TValue& value);

    friend void Serialize<>(const TDataCenterAwareValue& value, NYson::IYsonConsumer* consumer);
    friend void DeserializeScalarValue<>(TDataCenterAwareValue& value, const NYTree::INodePtr& node);
    friend void DeserializeMultiDataCenterValue<>(TDataCenterAwareValue& value, const NYTree::INodePtr& node);
    friend void FormatValue<>(TStringBuilderBase* builder, const TDataCenterAwareValue& value, TStringBuf format);

    friend class TSchedulingSegmentMap<TValue>;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TSchedulingSegmentMap
{
public:
    TSchedulingSegmentMap();

    [[nodiscard]] const TDataCenterAwareValue<TValue>& At(ESchedulingSegment segment) const;
    TDataCenterAwareValue<TValue>& At(ESchedulingSegment segment);

private:
    TEnumIndexedVector<ESchedulingSegment, TDataCenterAwareValue<TValue>> Map_;

    friend void Serialize<>(const TSchedulingSegmentMap& map, NYson::IYsonConsumer* consumer);
    friend void Deserialize<>(TSchedulingSegmentMap& map, const NYTree::INodePtr& node);
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
