#pragma once
#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// TODO(mikari): rename Stream to something else
// TODO(mikari): write documentation about StreamTraverse.

struct TInflightMetrics
    : public NYTree::TYsonStruct
{
    i64 Count{};
    // Undefined - is not supported.
    std::optional<i64> ByteSize;

    // Last timestamp when idleness was observed. nullopt if partition is not idle at the last observed moment.
    std::optional<TSystemTimestamp> LastIdleTimestamp;
    std::optional<TDuration> IdleDuration;

    // Last moment when partition get reading error. The value is null if there was a success later.
    std::optional<TSystemTimestamp> UnavailableTimestamp;

    std::optional<double> NewCountPerSec;
    std::optional<double> NewBytesPerSec;
    std::optional<double> ProcessedCountPerSec;
    std::optional<double> ProcessedBytesPerSec;

    REGISTER_YSON_STRUCT(TInflightMetrics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInflightMetrics);

void InPlaceMergeInflightMetrics(
    const TInflightMetricsPtr& current,
    const TInflightMetricsPtr& other,
    EInflightMerge inflightMerge,
    bool allowPartial = false);

////////////////////////////////////////////////////////////////////////////////

struct TStreamTraverseData
    : public NYTree::TYsonStruct
{
    i64 Epoch{};

    EStreamState State{};
    TSystemTimestamp SystemWatermark;
    TSystemTimestamp EventWatermark;

    // Approximate. Information about current inflight. So even with State == Active, InflightMetrics->Count could be equal 0.
    TInflightMetricsPtr InflightMetrics;

    REGISTER_YSON_STRUCT(TStreamTraverseData);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStreamTraverseData);

////////////////////////////////////////////////////////////////////////////////

TStreamTraverseDataPtr MakeCompletedStreamTraverseData(
    i64 epoch,
    TSystemTimestamp timestamp);

TStreamTraverseDataPtr MergeStreamTraverseData(
    const std::vector<TStreamTraverseDataPtr>& streams,
    EInflightMerge inflightMerge,
    bool allowPartial = false);

TStreamTraverseDataPtr AdvanceStreamTraverseData(
    const TStreamTraverseDataPtr& currentStream,
    const TStreamTraverseDataPtr& newStream);

////////////////////////////////////////////////////////////////////////////////

struct TInflightStreamTraverseData
    : public NYTree::TYsonStruct
{
    bool Suspended{};
    bool Empty{};

    std::optional<TSystemTimestamp> MinSystemTimestamp;
    std::optional<TSystemTimestamp> MinEventTimestamp;

    // Approximate inflight metrics
    TInflightMetricsPtr InflightMetrics;

    REGISTER_YSON_STRUCT(TInflightStreamTraverseData);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInflightStreamTraverseData);

////////////////////////////////////////////////////////////////////////////////

TStreamTraverseDataPtr ApplyInflightTraverseData(
    const TStreamTraverseDataPtr& stream,
    const TInflightStreamTraverseDataPtr& inflight,
    TSystemTimestamp systemWatermark);

TInflightStreamTraverseDataPtr MergeInflightTraverseData(
    const std::vector<TInflightStreamTraverseDataPtr>& inflights);

////////////////////////////////////////////////////////////////////////////////

struct TNodeTraverseData
    : public NYTree::TYsonStruct
{
    TSystemTimestamp ReportTime;

    THashMap<TStreamId, TStreamTraverseDataPtr> Streams;

    REGISTER_YSON_STRUCT(TNodeTraverseData);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeTraverseData);

////////////////////////////////////////////////////////////////////////////////

TNodeTraverseDataPtr MergeNodeTraverseData(const std::vector<TNodeTraverseDataPtr>& nodes, const TComputationSpecPtr& spec);

TNodeTraverseDataPtr AdvanceNodeTraverseData(
    const TNodeTraverseDataPtr& currentNode,
    const TNodeTraverseDataPtr& newNode);

////////////////////////////////////////////////////////////////////////////////

struct TFromPartitionTraverseData
    : public NYTree::TYsonStruct
{
    TNodeTraverseDataPtr Node;

    REGISTER_YSON_STRUCT(TFromPartitionTraverseData);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFromPartitionTraverseData);

TFromPartitionTraverseDataPtr MakeCompletedPartitionTraverseData(
    i64 epoch,
    TSystemTimestamp timestamp,
    const TExtendedComputationSpecPtr& spec);

////////////////////////////////////////////////////////////////////////////////

struct TToPartitionTraverseData
    : public NYTree::TYsonStruct
{
    THashMap<TStreamId, TStreamTraverseDataPtr> InputStreams;

    REGISTER_YSON_STRUCT(TToPartitionTraverseData);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TToPartitionTraverseData);

////////////////////////////////////////////////////////////////////////////////

// TODO(mikari): revise validation

void ValidateComputationInflights(
    const THashMap<TStreamId, TInflightStreamTraverseDataPtr>& inflights,
    const TComputationSpecPtr& spec);

void ValidateFromPartitionTraverseData(
    const TFromPartitionTraverseDataPtr& traverseData,
    const TComputationSpecPtr& /*spec*/,
    const TExtendedComputationSpecPtr& extendedSpec);

void ValidateToPartitionTraverseData(
    const TToPartitionTraverseDataPtr& traverseData,
    const TComputationSpecPtr& spec,
    const TExtendedComputationSpecPtr& extendedSpec);

////////////////////////////////////////////////////////////////////////////////

struct TPipelineTraverseData
    : public NYTree::TYsonStruct
{
    THashMap<TComputationId, TNodeTraverseDataPtr> Computations;
    TStreamsTraverse Streams;
    TStreamTraverseDataPtr UnitedSourceStream;
    TStreamTraverseDataPtr UnitedTimerStream;
    TStreamTraverseDataPtr UnitedKeyVisitorStream;
    TStreamTraverseDataPtr UnitedOutputStream;
    TStreamTraverseDataPtr UnitedStream;

    TSystemTimestamp InputSystemWatermark;

    REGISTER_YSON_STRUCT(TPipelineTraverseData);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPipelineTraverseData);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
