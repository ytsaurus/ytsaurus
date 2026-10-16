#pragma once

#include <yt/yt/server/scheduler/strategy/policy/public.h>

#include <yt/yt/server/scheduler/common/public.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

TJobResources GetAdjustedResourceLimits(
    const TJobResources& demand,
    const TJobResources& limits,
    const TMemoryDistribution& execNodeMemoryDistribution);

////////////////////////////////////////////////////////////////////////////////

void ProfileResourceVector(
    NProfiling::ISensorWriter* writer,
    const THashSet<EJobResourceType>& resourceTypes,
    const TResourceVector& resourceVector,
    const std::string& prefix);

void ProfileResourceVolume(
    NProfiling::ISensorWriter* writer,
    const TResourceVolume& volume,
    const std::string& prefix,
    NProfiling::EMetricType metricType = NProfiling::EMetricType::Gauge);

void ProfileResourcesConfig(
    NProfiling::ISensorWriter* writer,
    const NVectorHdrf::TJobResourcesConfig* resourcesConfig,
    const std::string& prefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy

