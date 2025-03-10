#include "job_size_constraints.h"

#include <yt/yt/server/lib/chunk_pools/job_size_tracker.h>

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include "config.h"

namespace NYT::NClickHouseServer {

using namespace NChunkPools;
using namespace NControllerAgent;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxTotalSliceCount = 1'000'000;

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): consider introducing new job size constraints to encapsulate all these heuristics.

TClickHouseJobSizeSpec CreateClickHouseJobSizeSpec(
    const TExecutionSettingsPtr& executionSettings,
    const TSubqueryConfigPtr& subqueryConfig,
    i64 totalDataWeight,
    i64 totalRowCount,
    int jobCount,
    std::optional<double> samplingRate,
    EReadInOrderMode readInOrderMode,
    const TLogger& logger)
{
    const auto& Logger = logger;

    YT_VERIFY(jobCount);
    auto dataWeightPerJob = totalDataWeight / jobCount;

    if (samplingRate) {
        double rate = samplingRate.value();
        auto adjustedRate = std::max(rate, static_cast<double>(jobCount) / subqueryConfig->MaxJobCountForPool);
        auto adjustedJobCount = std::floor(jobCount / adjustedRate);
        YT_LOG_INFO("Adjusting job count and sampling rate (OldSamplingRate: %v, AdjustedSamplingRate: %v, OldJobCount: %v, AdjustedJobCount: %v)",
            rate,
            adjustedRate,
            jobCount,
            adjustedJobCount);
        rate = adjustedRate;
        jobCount = adjustedJobCount;
    } else {
        // Try not to form too small ranges when total data weight is small.
        auto maxJobCount = totalDataWeight / std::max<i64>(1, subqueryConfig->MinDataWeightPerThread) + 1;
        if (maxJobCount < jobCount) {
            jobCount = maxJobCount;
            dataWeightPerJob = std::max<i64>(1, totalDataWeight / maxJobCount);
            YT_LOG_INFO("Query is small and without sampling; forcing new constraints (JobCount: %v, DataWeightPerJob: %v)",
                jobCount,
                dataWeightPerJob);
        }
    }

    auto inputSliceDataWeight = std::max<i64>(1, dataWeightPerJob * 0.1);

    TJobSizeTrackerOptions jobSizeTrackerOptions;

    if (readInOrderMode != EReadInOrderMode::None) {
        jobSizeTrackerOptions = TJobSizeTrackerOptions{
            .LimitProgressionRatio = 2,
            .GeometricResources = {EResourceKind::DataWeight, EResourceKind::PrimaryDataWeight},
            .LimitProgressionLength = 1,
        };

        // This allows configuring the size of the first subquery via query settings.
        // We can logically use this setting, since each thread subquery forms its own
        // secondary query when reading in order.
        auto startDataWeightPerJob = std::max(executionSettings->MinDataWeightPerSecondaryQuery, subqueryConfig->MinDataWeightPerThread);
        for (auto dataWeight = startDataWeightPerJob; dataWeight < dataWeightPerJob; dataWeight *= *jobSizeTrackerOptions.LimitProgressionRatio) {
            ++jobSizeTrackerOptions.LimitProgressionLength;
        }

        switch (readInOrderMode) {
            case EReadInOrderMode::Forward:
                // The first range is small!
                dataWeightPerJob = startDataWeightPerJob;
                break;
            case EReadInOrderMode::Backward:
                // We will be reading these ranges backwards!
                jobSizeTrackerOptions.LimitProgressionRatio = 0.5;
                // We need to perform an actual split for the optimization to work.
                if (jobCount == 1) {
                    dataWeightPerJob /= 2;
                    ++jobCount;
                }
                // This is necessary to avoid producing ranges that would be
                // larger than they were supposed to be originally.
                // We multiply by the ratio *after* we stage a job, so we need
                // to do it after the second to last range.
                jobSizeTrackerOptions.LimitProgressionOffset = jobCount - 2;
                break;
            default:
                YT_ABORT();
        }

        // Account for the additional jobs.
        // This is not exact, but the sorted pool won't use this value anyway.
        jobCount += jobSizeTrackerOptions.LimitProgressionLength - 1;

        // This helps us actually produce small jobs when we need them.
        inputSliceDataWeight = std::max<i64>(1, startDataWeightPerJob * 0.1);
    }

    // It is important to watch the ratio of total data weight to input slice data weight, to avoid dealing with too many slices.
    // The total data weight is bounded by the maximum subquery size, so the maximum number of slices with the current defaults
    // is 50G / 64M = 800, which is fine. Reconfigure with care.
    inputSliceDataWeight = std::max(inputSliceDataWeight, subqueryConfig->MinSliceDataWeight);
    YT_LOG_WARNING_IF(
        totalDataWeight / inputSliceDataWeight > MaxTotalSliceCount,
        "Job size constraints allow too many slices in pool (TotalDataWeight: %v, InputSliceDataWeight: %v, PotentialSliceCount: %v)",
        totalDataWeight,
        inputSliceDataWeight,
        totalDataWeight / inputSliceDataWeight);

    YT_LOG_INFO(
        "Computed job size constraints for pool (JobCount: %v, DataWeightPerJob: %v, InputSliceDataWeight: %v, "
        "LimitProgressionLength: %v, LimitProgressionOffset: %v, LimitProgressionRatio: %v, SamplingRate: %v)",
        jobCount,
        dataWeightPerJob,
        inputSliceDataWeight,
        jobSizeTrackerOptions.LimitProgressionLength,
        jobSizeTrackerOptions.LimitProgressionOffset,
        jobSizeTrackerOptions.LimitProgressionRatio,
        samplingRate);

    auto jobSizeConstraints = CreateExplicitJobSizeConstraints(
        /*canAdjustDataWeightPerJob*/ false,
        /*isExplicitJobCount*/ true,
        jobCount,
        dataWeightPerJob,
        /*primaryDataWeightPerJob*/ 1_EB,
        /*maxDataSlicesPerJob*/ 1'000'000'000'000ll,
        /*maxDataWeightPerJob*/ 1_EB,
        /*maxPrimaryDataWeightPerJob*/ 1_EB,
        /*maxCompressedDataSizePerJob*/ 1_EB,
        /*inputSliceDataWeight*/ inputSliceDataWeight,
        /*inputSliceRowCount*/ std::max<i64>(1, totalRowCount / jobCount),
        /*batchRowCount*/ {},
        /*foreignSliceDataWeight*/ 0,
        /*samplingRate*/ std::nullopt);

    return {
        .JobSizeConstraints = std::move(jobSizeConstraints),
        .JobSizeTrackerOptions = std::move(jobSizeTrackerOptions),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseServer
