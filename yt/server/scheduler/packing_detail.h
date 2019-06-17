#pragma once

#include "private.h"
#include "packing.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJobResourcesRatio
{
public:
    DEFINE_BYVAL_RO_PROPERTY(double, CpuRatio);
    DEFINE_BYVAL_RO_PROPERTY(double, MemoryRatio);

    TJobResourcesRatio(double cpuRatio, double memoryRatio);

    TJobResourcesRatio(
        const NScheduler::TJobResources& resources,
        const NScheduler::TJobResources& totalResources);

    static TJobResourcesRatio Ones();

    static TJobResourcesRatio Zeros();
};

TJobResourcesRatio operator*(const TJobResourcesRatio& ratio, double scale);

TJobResourcesRatio operator-(const TJobResourcesRatio& lhs, const TJobResourcesRatio& rhs);

bool Dominates(const TJobResourcesRatio& lhs, const TJobResourcesRatio& rhs);

double LengthSquare(const TJobResourcesRatio& ratio);

double Length(const TJobResourcesRatio& ratio);

double DotProduct(const TJobResourcesRatio& lhs, const TJobResourcesRatio& rhs);

double CosBetween(const TJobResourcesRatio& lhs, const TJobResourcesRatio& rhs);

double MinComponent(const TJobResourcesRatio& ratio);

double ComponentsSum(const TJobResourcesRatio& ratio);

TJobResourcesRatio ToRatio(const TJobResources& jobResources, const TJobResources& totalResourceLimits);

////////////////////////////////////////////////////////////////////////////////

double AngleLengthPackingMetric(
    const TPackingNodeResourcesSnapshot& nodeResourcesSnapshot,
    const TJobResourcesWithQuota& jobResourcesWithQuota,
    const TJobResources& totalResourceLimits);

double AnglePackingMetric(
    const TPackingNodeResourcesSnapshot& nodeResourcesSnapshot,
    const TJobResourcesWithQuota& jobResourcesWithQuota,
    const TJobResources& totalResourceLimits);

double PackingMetric(
    const TPackingNodeResourcesSnapshot& nodeResourcesSnapshot,
    const TJobResourcesWithQuota& jobResourcesWithQuota,
    const TJobResources& totalResourceLimits,
    const TFairShareStrategyPackingConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
