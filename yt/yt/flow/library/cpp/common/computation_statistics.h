#pragma once

#include "public.h"
#include "stream_statistics.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TBatchStatistics
{
    i64 Count = 0;
    i64 ByteSize = 0;

    TBatchStatistics& operator+=(const TBatchStatistics& other);
};

struct TPartitionProcessingRates
    : public NYTree::TYsonStructLite
{
    // Counts messages, timers and key visits, including rows consumed by skip_if_expression.
    TStreamSpeedStatistics Processed;
    std::optional<TStreamSpeedStatistics> Capacity;

    REGISTER_YSON_STRUCT_LITE(TPartitionProcessingRates);
    static void Register(TRegistrar registrar);
};

//! Locally processed rates and capacity assuming an unchanged input-stream mix.
struct TComputationProcessingRates
    : public NYTree::TYsonStruct
{
    // A missing window is unknown, not an observed zero.
    std::optional<TPartitionProcessingRates> Rate1m;
    std::optional<TPartitionProcessingRates> Rate10m;

    REGISTER_YSON_STRUCT(TComputationProcessingRates);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TComputationProcessingRates);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
