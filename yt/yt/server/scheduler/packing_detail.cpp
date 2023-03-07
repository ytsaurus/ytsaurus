#include "packing_detail.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TJobResourcesRatio::TJobResourcesRatio(double cpuRatio, double memoryRatio)
    : CpuRatio_(cpuRatio)
    , MemoryRatio_(memoryRatio)
{ }

TJobResourcesRatio::TJobResourcesRatio(
    const NScheduler::TJobResources& resources,
    const NScheduler::TJobResources& totalResources)
    : TJobResourcesRatio(
        static_cast<double>(resources.GetCpu()) / static_cast<double>(totalResources.GetCpu()),
        static_cast<double>(resources.GetMemory()) / static_cast<double>(totalResources.GetMemory()))
{ }

TJobResourcesRatio TJobResourcesRatio::Ones()
{
    return TJobResourcesRatio(1.0, 1.0);
}

TJobResourcesRatio TJobResourcesRatio::Zeros()
{
    return TJobResourcesRatio(0, 0);
}

TJobResourcesRatio operator*(const TJobResourcesRatio& ratio, double scale)
{
    return TJobResourcesRatio(ratio.GetCpuRatio() * scale, ratio.GetMemoryRatio() * scale);
}

TJobResourcesRatio operator-(const TJobResourcesRatio& lhs, const TJobResourcesRatio& rhs)
{
    return TJobResourcesRatio(lhs.GetCpuRatio() - rhs.GetCpuRatio(), lhs.GetMemoryRatio() - rhs.GetMemoryRatio());
}

bool Dominates(const TJobResourcesRatio& lhs, const TJobResourcesRatio& rhs)
{
    return lhs.GetCpuRatio() >= rhs.GetCpuRatio() && lhs.GetMemoryRatio() >= rhs.GetMemoryRatio();
}

double LengthSquare(const TJobResourcesRatio& ratio)
{
    return ratio.GetCpuRatio() * ratio.GetCpuRatio() + ratio.GetMemoryRatio() * ratio.GetMemoryRatio();
}

double Length(const TJobResourcesRatio& ratio)
{
    return std::sqrt(LengthSquare(ratio));
}

double DotProduct(const TJobResourcesRatio& lhs, const TJobResourcesRatio& rhs)
{
    return lhs.GetCpuRatio() * rhs.GetCpuRatio() + lhs.GetMemoryRatio() * rhs.GetMemoryRatio();
}

double CosBetween(const TJobResourcesRatio& lhs, const TJobResourcesRatio& rhs)
{
    return DotProduct(lhs, rhs) / Length(lhs) / Length(rhs);
}

double MinComponent(const TJobResourcesRatio& ratio)
{
    return std::min(ratio.GetCpuRatio(), ratio.GetMemoryRatio());
}

double ComponentsSum(const TJobResourcesRatio& ratio)
{
    return ratio.GetCpuRatio() + ratio.GetMemoryRatio();
}

TJobResourcesRatio ToRatio(const TJobResources& jobResources, const TJobResources& totalResourceLimits)
{
    return TJobResourcesRatio(
        static_cast<double>(jobResources.GetCpu()) / static_cast<double>(totalResourceLimits.GetCpu()),
        static_cast<double>(jobResources.GetMemory()) / static_cast<double>(totalResourceLimits.GetMemory()));
}

////////////////////////////////////////////////////////////////////////////////

double AngleLengthPackingMetric(
    const TPackingNodeResourcesSnapshot& nodeResourcesSnapshot,
    const TJobResourcesWithQuota& jobResourcesWithQuota,
    const TJobResources& totalResourceLimits)
{
    auto nodeFree = ToRatio(nodeResourcesSnapshot.Free(), totalResourceLimits);
    auto jobDemand = ToRatio(jobResourcesWithQuota.ToJobResources(), totalResourceLimits);

    double angleMetric = 1 - CosBetween(jobDemand, nodeFree);  // Between 0 and 1.
    double lengthMetric = Length(nodeFree) / Length(jobDemand);  // At least 1.

    return angleMetric * lengthMetric;
}

double AnglePackingMetric(
    const TPackingNodeResourcesSnapshot& nodeResourcesSnapshot,
    const TJobResourcesWithQuota& jobResourcesWithQuota,
    const TJobResources& totalResourceLimits)
{
    auto nodeFree = ToRatio(nodeResourcesSnapshot.Free(), totalResourceLimits);
    auto jobDemand = ToRatio(jobResourcesWithQuota.ToJobResources(), totalResourceLimits);
    return 1 - CosBetween(jobDemand, nodeFree);
}

double PackingMetric(
    const TPackingNodeResourcesSnapshot& nodeResourcesSnapshot,
    const TJobResourcesWithQuota& jobResourcesWithQuota,
    const TJobResources& totalResourceLimits,
    const TFairShareStrategyPackingConfigPtr& config)
{
    // NB: this function must be thread-safe.

    auto getMetricFunction = [](EPackingMetricType metricType) {
        switch (metricType) {
            case EPackingMetricType::Angle:
                return AnglePackingMetric;
            case EPackingMetricType::AngleLength:
                return AngleLengthPackingMetric;
            default:
                THROW_ERROR_EXCEPTION("Unexpected packing metric type: '%v'", FormatEnum(metricType));
        }
    };

    return getMetricFunction(config->Metric)(nodeResourcesSnapshot, jobResourcesWithQuota, totalResourceLimits);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
