#pragma once

#include "job_resources_with_quota.h"

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/phoenix/context.h>
#include <yt/yt/core/phoenix/type_decl.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/numeric/serialize/fixed_point_number.h>

#include <yt/yt/library/profiling/producer.h>

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

std::string FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits);
std::string FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits,
    const NNodeTrackerClient::NProto::TDiskResources& diskResources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

std::string FormatResources(const TJobResourcesWithQuota& resources);

std::string FormatResourcesConfig(const TJobResourcesConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

class TJobResourcesProfiler
{
public:
    TJobResourcesProfiler();

    void Init(
        const NProfiling::TProfiler& profiler,
        NProfiling::EMetricType metricType = NProfiling::EMetricType::Gauge);
    void Start();
    void Stop();
    void Update(
        const TJobResources& resources,
        TCompactVector<NProfiling::TTag, 2> tags = {});

private:
    NProfiling::TBufferedProducerPtr Producer_;
    NProfiling::EMetricType MetricType_ = NProfiling::EMetricType::Gauge;
};

void ProfileResources(
    NProfiling::ISensorWriter* writer,
    const TJobResources& resources,
    const std::string& prefix,
    NProfiling::EMetricType metricType = NProfiling::EMetricType::Gauge);

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

} // namespace NYT

PHOENIX_DECLARE_EXTERNAL_TYPE(
    NYT::NScheduler::TJobResources,
    0xf84d3e2a,
    NYT::NPhoenix::TSaveContext,
    NYT::NPhoenix::TLoadContext);
