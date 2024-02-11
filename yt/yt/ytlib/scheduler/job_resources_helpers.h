#pragma once

#include "job_resources_with_quota.h"

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/numeric/serialize/fixed_point_number.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/library/vector_hdrf/resource_helpers.h>

namespace NYT {

namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TJobResources ToJobResources(const NNodeTrackerClient::NProto::TNodeResources& nodeResources);
NNodeTrackerClient::NProto::TNodeResources ToNodeResources(const TJobResources& jobResources);

////////////////////////////////////////////////////////////////////////////////

TJobResources ToJobResources(const TJobResourcesConfigPtr& config, TJobResources defaultValue);

////////////////////////////////////////////////////////////////////////////////

void SerializeDiskQuota(
    const TDiskQuota& quota,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
    NYson::IYsonConsumer* consumer);

void SerializeJobResourcesWithQuota(
    const TJobResourcesWithQuota& resources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TDiskQuota& diskQuota, TStringBuf /*format*/);

TString FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits);
TString FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits,
    const NNodeTrackerClient::NProto::TDiskResources& diskResources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

TString FormatResources(const TJobResources& resources);
TString FormatResources(const TJobResourcesWithQuota& resources);

TString FormatResourcesConfig(const TJobResourcesConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

class TJobResourcesProfiler
{
public:
    void Init(const NProfiling::TProfiler& profiler);
    void Reset();
    void Update(const TJobResources& resources);

private:
#define XX(name, Name) NProfiling::TGauge Name;
    ITERATE_JOB_RESOURCES(XX)
#undef XX
};

void ProfileResources(
    NProfiling::ISensorWriter* writer,
    const TJobResources& resources,
    const TString& prefix,
    NProfiling::EMetricType metricType = NProfiling::EMetricType::Gauge);

////////////////////////////////////////////////////////////////////////////////

struct TJobResourcesSerializer
{
    template <class C>
    static void Save(C& context, const TJobResources& value)
    {
        NYT::Save(context, value.GetUserSlots());
        NYT::Save(context, value.GetCpu());
        NYT::Save(context, value.GetGpu());
        NYT::Save(context, value.GetMemory());
        NYT::Save(context, value.GetNetwork());
    }

    template <class C>
    static void Load(C& context, TJobResources& value)
    {
        i64 userSlots;
        TCpuResource cpu;
        int gpu;
        i64 memory;
        i64 network;

        NYT::Load(context, userSlots);
        NYT::Load(context, cpu);
        NYT::Load(context, gpu);
        NYT::Load(context, memory);
        NYT::Load(context, network);

        value.SetUserSlots(userSlots);
        value.SetCpu(cpu);
        value.SetGpu(gpu);
        value.SetMemory(memory);
        value.SetNetwork(network);
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NScheduler::NProto::TDiskQuota* protoDiskQuota, const NScheduler::TDiskQuota& diskQuota);
void FromProto(NScheduler::TDiskQuota* diskQuota, const NScheduler::NProto::TDiskQuota& protoDiskQuota);

void ToProto(NScheduler::NProto::TJobResources* protoResources, const NScheduler::TJobResources& resources);
void FromProto(NScheduler::TJobResources* resources, const NScheduler::NProto::TJobResources& protoResources);

void ToProto(NScheduler::NProto::TJobResourcesWithQuota* protoResources, const NScheduler::TJobResourcesWithQuota& resources);
void FromProto(NScheduler::TJobResourcesWithQuota* resources, const NScheduler::NProto::TJobResourcesWithQuota& protoResources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

} // namespace NScheduler

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct TSerializerTraits<NScheduler::TJobResources, C, void>
{
    using TSerializer = NScheduler::TJobResourcesSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
