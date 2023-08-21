#pragma once

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// For each memory capacity gives the number of nodes with this much memory.
using TMemoryDistribution = THashMap<i64, int>;

TJobResources GetAdjustedResourceLimits(
    const TJobResources& demand,
    const TJobResources& limits,
    const TMemoryDistribution& execNodeMemoryDistribution);

////////////////////////////////////////////////////////////////////////////////

void ProfileResourceVector(
    NProfiling::ISensorWriter* writer,
    const THashSet<EJobResourceType>& resourceTypes,
    const TResourceVector& resourceVector,
    const TString& prefix);

void ProfileResourceVolume(
    NProfiling::ISensorWriter* writer,
    const TResourceVolume& volume,
    const TString& prefix,
    NProfiling::EMetricType metricType = NProfiling::EMetricType::Gauge);

void ProfileResourcesConfig(
    NProfiling::ISensorWriter* writer,
    const TJobResourcesConfigPtr& resourcesConfig,
    const TString& prefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

