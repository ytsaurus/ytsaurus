#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

#include <ytlib/job_proxy/public.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

Stroka FormatResourceUtilization(
    const NProto::TNodeResources& utilization,
    const NProto::TNodeResources& limits);

Stroka FormatResources(
    const NProto::TNodeResources& resources);

void IncreaseResourceUtilization(
    NProto::TNodeResources* utilization,
    const NProto::TNodeResources& delta);

bool HasEnoughResources(
    const NProto::TNodeResources& currentUtilization,
    const NProto::TNodeResources& requestedUtilization,
    const NProto::TNodeResources& limits);

bool HasSpareResources(
    const NProto::TNodeResources& utilization,
    const NProto::TNodeResources& limits);

void BuildNodeResourcesYson(
    const NProto::TNodeResources& resources,
    NYTree::IYsonConsumer* consumer);

NProto::TNodeResources ZeroResources();
NProto::TNodeResources InfiniteResources();

i64 GetIOMemorySize(
    NJobProxy::TJobIOConfigPtr ioConfig,
    int inputStreamCount,
    int outputStreamCount);

NProto::TNodeResources GetMapResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TMapOperationSpecPtr spec);

NProto::TNodeResources GetMapDuringMapReduceResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TMapReduceOperationSpecPtr spec,
    int partitionCount);

NProto::TNodeResources GetMergeJobResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TMergeOperationSpecPtr spec);

NProto::TNodeResources GetPartitionReduceDuringMapReduceResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TMapReduceOperationSpecPtr spec,
    i64 dataSize,
    i64 rowCount);

NProto::TNodeResources GetSortedReduceDuringMapReduceResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TMapReduceOperationSpecPtr spec,
    int stripeCount);

NProto::TNodeResources GetEraseResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TEraseOperationSpecPtr spec);

NProto::TNodeResources GetSortedReduceResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TReduceOperationSpecPtr spec);

NProto::TNodeResources GetPartitionResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    i64 dataSize,
    int partitionCount);

NProto::TNodeResources GetSimpleSortResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec,
    i64 dataSize,
    i64 rowCount,
    i64 valueCount);

NProto::TNodeResources GetPartitionSortResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec,
    i64 dataSize,
    i64 rowCount);

NProto::TNodeResources GetSortedMergeDuringSortResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec,
    int stripeCount);

NProto::TNodeResources GetUnorderedMergeDuringSortResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
