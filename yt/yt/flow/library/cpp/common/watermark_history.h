#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkHistoryElement
    : public NYTree::TYsonStructLite
{
    //! Instant and watermark of the newest sample folded into the element.
    TSystemTimestamp Time;
    TSystemTimestamp Watermark;
    //! Instant of the oldest sample folded into the element.
    TSystemTimestamp FirstSampleTime;

    REGISTER_YSON_STRUCT_LITE(TWatermarkHistoryElement);

    static void Register(TRegistrar registrar);
};

//! Monotonic history of a published watermark, exponentially thinned by time span: a merged element
//! spans at most 1/K of its age, K being the thinning factor. Reach instants are never later than the true ones,
//! and earlier by at most 1/K of their age.
struct TWatermarkHistory
    : public NYTree::TYsonStructLite
{
    //! Ordered from oldest to newest; the first element is the anchor sample.
    std::vector<TWatermarkHistoryElement> Elements;

    //! Records that |watermark| was published at |instant|; a regressing watermark or a non-increasing instant is ignored.
    void Append(TSystemTimestamp instant, TSystemTimestamp watermark, int thinningFactor);

    //! Returns a conservative estimate of the first instant at which the watermark was at least |watermark|:
    //! never later than that instant, and earlier by at most 1/K of its age, K being the thinning factor.
    //! #InfinitySystemTimestamp if not reached yet, null if it precedes the history.
    std::optional<TSystemTimestamp> FindReachInstant(TSystemTimestamp watermark) const;

    //! Instant of the anchor sample; the history must not be empty.
    TSystemTimestamp GetStartTime() const;

    REGISTER_YSON_STRUCT_LITE(TWatermarkHistory);

    static void Register(TRegistrar registrar);
};

//! Watermark histories of the streams referenced by event-time and system-time timer conditions.
struct TPipelineWatermarkHistory
    : public NYTree::TYsonStruct
{
    THashMap<TStreamId, TWatermarkHistory> Event;
    THashMap<TStreamId, TWatermarkHistory> System;

    THashMap<TStreamId, TWatermarkHistory>& Get(ETimeType timeType);
    const THashMap<TStreamId, TWatermarkHistory>& Get(ETimeType timeType) const;

    REGISTER_YSON_STRUCT(TPipelineWatermarkHistory);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPipelineWatermarkHistory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
