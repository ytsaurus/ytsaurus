#pragma once

#include "yql_ytflow_timing_guard.h"

#include <yt/yt/library/profiling/sensor.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYT::NFlow {

struct TResourceContext;

} // namespace NYT::NFlow

namespace NYql::NYtflow {

enum class EComputationPatternUnsuitabilityReason;
class TComputationPatternResult;

TStringBuf GetComputationPatternUnsuitabilityReasonName(
    EComputationPatternUnsuitabilityReason reason);

class TProfiledOperationGuard {
public:
    TProfiledOperationGuard(
        const NYT::NProfiling::TCounter& count,
        const NYT::NProfiling::TCounter& errorCount,
        TCpuVCpuTimeCounter& timeCounter);

    ~TProfiledOperationGuard();

    TProfiledOperationGuard(const TProfiledOperationGuard&) = delete;
    TProfiledOperationGuard& operator=(const TProfiledOperationGuard&) = delete;

private:
    int InitialUncaughtExceptionCount_;
    NYT::NProfiling::TCounter ErrorCounter_;
    TSimpleTimingGuard TimingGuard_;
};

class TProfiledOperationMetrics {
public:
    TProfiledOperationMetrics(
        const NYT::NProfiling::TProfiler& profiler,
        TStringBuf operationPath,
        bool profileErrors);

    TProfiledOperationMetrics(const TProfiledOperationMetrics&) = delete;
    TProfiledOperationMetrics& operator=(const TProfiledOperationMetrics&) = delete;
    TProfiledOperationMetrics(TProfiledOperationMetrics&&) = delete;
    TProfiledOperationMetrics& operator=(TProfiledOperationMetrics&&) = delete;

    TProfiledOperationGuard Profile() const;

private:
    NYT::NProfiling::TCounter Count_;
    NYT::NProfiling::TCounter ErrorCount_;
    NYT::NProfiling::TTimeCounter CpuTime_;
    NYT::NProfiling::TTimeCounter VCpuTime_;
    mutable TCpuVCpuTimeCounter CpuVCpuTime_;
};

class TFunctionRegistryMetrics {
public:
    explicit TFunctionRegistryMetrics(const NYT::NProfiling::TProfiler& profiler);

    TProfiledOperationGuard ProfileLoad() const;
    void RecordUdfPaths(const TVector<TString>& udfPaths) const;

private:
    TProfiledOperationMetrics LoadMetrics_;
    NYT::NProfiling::TCounter UdfPathCount_;
};

class TComputationPatternMetrics {
public:
    explicit TComputationPatternMetrics(const NYT::NFlow::TResourceContext& context);

    TProfiledOperationGuard ProfileLoad() const;
    void RecordShape(size_t lambdaFileBytes, size_t nodeCount) const;
    void RecordResult(const TComputationPatternResult& result) const;

private:
    TProfiledOperationMetrics LoadMetrics_;
    NYT::NProfiling::TCounter SuitableCount_;
    TVector<NYT::NProfiling::TCounter> UnsuitableCounters_;
    NYT::NProfiling::TCounter LambdaFileBytes_;
    NYT::NProfiling::TCounter NodeCount_;
};

class TComputationGraphMetrics {
public:
    explicit TComputationGraphMetrics(const NYT::NProfiling::TProfiler& profiler);

    TProfiledOperationGuard ProfileClone() const;
    TProfiledOperationGuard ProfilePrepare() const;
    void RecordFallback(EComputationPatternUnsuitabilityReason reason) const;

private:
    TProfiledOperationMetrics CloneMetrics_;
    TProfiledOperationMetrics PrepareMetrics_;
    TVector<NYT::NProfiling::TCounter> FallbackCounters_;
};

} // namespace NYql::NYtflow
