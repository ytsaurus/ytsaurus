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

NProto::TNodeResources GetMapJobResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TMapOperationSpecPtr spec);

NProto::TNodeResources GetMergeJobResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TMergeOperationSpecPtr spec);

NProto::TNodeResources GetReduceJobResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TReduceOperationSpecPtr spec);

NProto::TNodeResources GetEraseJobResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TEraseOperationSpecPtr spec);

NProto::TNodeResources GetPartitionJobResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    int partitionCount);

NProto::TNodeResources GetSimpleSortJobResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec,
    i64 dataWeight,
    i64 rowCountPerJob,
    i64 valueCountPerJob);

NProto::TNodeResources GetSortedMergeDuringSortJobResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec,
    int stripeCount);

NProto::TNodeResources GetUnorderedMergeDuringSortJobResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
