#include "yql_ytflow_metrics.h"

#include "yql_ytflow_computation_pattern.h"
#include "yql_ytflow_computation_pattern_resource.h"
#include "yql_ytflow_utils.h"

#include <yt/yt/flow/library/cpp/common/resource.h>

#include <util/generic/hash_set.h>
#include <util/generic/serialized_enum.h>
#include <util/system/yassert.h>

#include <exception>

namespace NYql::NYtflow {
namespace {

TVector<NYT::NProfiling::TCounter> CreateReasonCounters(
    const NYT::NProfiling::TProfiler& profiler,
    TStringBuf sensorPath)
{
    const auto reasons = GetEnumAllValues<EComputationPatternUnsuitabilityReason>();
    TVector<NYT::NProfiling::TCounter> counters;
    counters.reserve(reasons.size());
    for (const auto reason : reasons) {
        counters.push_back(
            profiler.WithTag(
                "reason",
                NEnumSerializationRuntime::ToStringBuf(reason))
                .Counter(sensorPath));
    }
    return counters;
}

const NYT::NProfiling::TCounter& GetReasonCounter(
    EComputationPatternUnsuitabilityReason reason,
    const TVector<NYT::NProfiling::TCounter>& counters)
{
    size_t index = 0;
    for (const auto knownReason : GetEnumAllValues<EComputationPatternUnsuitabilityReason>()) {
        if (knownReason == reason) {
            return counters.at(index);
        }
        ++index;
    }
    Y_UNREACHABLE();
}

TString MakeSensorPath(TStringBuf operationPath, TStringBuf suffix)
{
    TString result(operationPath);
    result += suffix;
    return result;
}

} // namespace

TStringBuf GetComputationPatternUnsuitabilityReasonName(
    EComputationPatternUnsuitabilityReason reason)
{
    return NEnumSerializationRuntime::ToStringBuf(reason);
}

TProfiledOperationGuard::TProfiledOperationGuard(
    const NYT::NProfiling::TCounter& count,
    const NYT::NProfiling::TCounter& errorCount,
    TCpuVCpuTimeCounter& timeCounter)
    : InitialUncaughtExceptionCount_(std::uncaught_exceptions())
    , ErrorCounter_(errorCount)
    , TimingGuard_(timeCounter)
{
    count.Increment();
}

TProfiledOperationGuard::~TProfiledOperationGuard()
{
    if (ErrorCounter_ && std::uncaught_exceptions() > InitialUncaughtExceptionCount_) {
        ErrorCounter_.Increment();
    }
}

TProfiledOperationMetrics::TProfiledOperationMetrics(
    const NYT::NProfiling::TProfiler& profiler,
    TStringBuf operationPath,
    bool profileErrors)
    : Count_(profiler.Counter(operationPath))
    , ErrorCount_(profileErrors
        ? profiler.Counter(MakeSensorPath(operationPath, "_errors"))
        : NYT::NProfiling::TCounter())
    , CpuTime_(profiler.TimeCounter(MakeSensorPath(operationPath, "/cpu_time")))
    , VCpuTime_(profiler.TimeCounter(MakeSensorPath(operationPath, "/vcpu_time")))
    , CpuVCpuTime_(CpuTime_, VCpuTime_, TryGetCpuToVCpuFactor())
{
}

TProfiledOperationGuard TProfiledOperationMetrics::Profile() const
{
    return TProfiledOperationGuard(Count_, ErrorCount_, CpuVCpuTime_);
}

TFunctionRegistryMetrics::TFunctionRegistryMetrics(
    const NYT::NProfiling::TProfiler& profiler)
    : LoadMetrics_(profiler, "/custom/function_registry/load", true)
    , UdfPathCount_(profiler.Counter("/custom/function_registry/udf_paths"))
{
}

TProfiledOperationGuard TFunctionRegistryMetrics::ProfileLoad() const
{
    return LoadMetrics_.Profile();
}

void TFunctionRegistryMetrics::RecordUdfPaths(const TVector<TString>& udfPaths) const
{
    const THashSet<TString> uniqueUdfPaths(udfPaths.begin(), udfPaths.end());
    UdfPathCount_.Increment(static_cast<i64>(uniqueUdfPaths.size()));
}

TComputationPatternMetrics::TComputationPatternMetrics(
    const NYT::NFlow::TResourceContext& context)
    : LoadMetrics_(context.Profiler, "/custom/computation_pattern/load", true)
    , SuitableCount_(context.Profiler.Counter("/custom/computation_pattern/suitable"))
    , UnsuitableCounters_(CreateReasonCounters(
        context.Profiler,
        "/custom/computation_pattern/unsuitable"))
    , LambdaFileBytes_(context.Profiler.Counter("/custom/computation_pattern/lambda_file_bytes"))
    , NodeCount_(context.Profiler.Counter("/custom/computation_pattern/nodes"))
{
}

TProfiledOperationGuard TComputationPatternMetrics::ProfileLoad() const
{
    return LoadMetrics_.Profile();
}

void TComputationPatternMetrics::RecordShape(
    size_t lambdaFileBytes,
    size_t nodeCount) const
{
    LambdaFileBytes_.Increment(static_cast<i64>(lambdaFileBytes));
    NodeCount_.Increment(static_cast<i64>(nodeCount));
}

void TComputationPatternMetrics::RecordResult(const TComputationPatternResult& result) const
{
    if (result.IsSuitable()) {
        SuitableCount_.Increment();
    } else {
        const auto reason = result.GetUnsuitabilityReason();
        GetReasonCounter(reason, UnsuitableCounters_).Increment();
    }
}

TComputationGraphMetrics::TComputationGraphMetrics(
    const NYT::NProfiling::TProfiler& profiler)
    : CloneMetrics_(profiler, "/custom/computation_graph/clone", false)
    , PrepareMetrics_(profiler, "/custom/computation_graph/prepare", false)
    , FallbackCounters_(CreateReasonCounters(
        profiler,
        "/custom/computation_graph/fallback"))
{
}

TProfiledOperationGuard TComputationGraphMetrics::ProfileClone() const
{
    return CloneMetrics_.Profile();
}

TProfiledOperationGuard TComputationGraphMetrics::ProfilePrepare() const
{
    return PrepareMetrics_.Profile();
}

void TComputationGraphMetrics::RecordFallback(
    EComputationPatternUnsuitabilityReason reason) const
{
    GetReasonCounter(reason, FallbackCounters_).Increment();
}

} // namespace NYql::NYtflow
