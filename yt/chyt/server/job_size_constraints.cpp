#include "job_size_constraints.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include "config.h"

namespace NYT::NClickHouseServer {

using namespace NControllerAgent;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): consider introducing new job size constraints to encapsulate all these heuristics.

IJobSizeConstraintsPtr CreateClickHouseJobSizeConstraints(
    TSubqueryConfigPtr config,
    i64 totalDataWeight,
    i64 totalRowCount,
    int jobCount,
    std::optional<double> samplingRate,
    const TLogger& logger)
{
    const auto& Logger = logger;

    auto dataWeightPerJob = totalDataWeight / jobCount;

    if (samplingRate) {
        double rate = samplingRate.value();
        auto adjustedRate = std::max(rate, static_cast<double>(jobCount) / config->MaxJobCountForPool);
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
        auto maxJobCount = totalDataWeight / std::max<i64>(1, config->MinDataWeightPerThread) + 1;
        if (maxJobCount < jobCount) {
            jobCount = maxJobCount;
            dataWeightPerJob = std::max<i64>(1, totalDataWeight / maxJobCount);
            YT_LOG_INFO("Query is small and without sampling; forcing new constraints (JobCount: %v, DataWeightPerJob: %v)",
                jobCount,
                dataWeightPerJob);
        }
    }

    auto inputSliceDataWeight = std::max<i64>(1, dataWeightPerJob * 0.1);
    if (inputSliceDataWeight < config->MinSliceDataWeight) {
        inputSliceDataWeight = dataWeightPerJob;
    }

    return CreateExplicitJobSizeConstraints(
        /*canAdjustDataWeightPerJob*/ false,
        /*isExplicitJobCount*/ true,
        jobCount,
        dataWeightPerJob,
        /*primaryDataWeightPerJob*/ 1_EB,
        /*maxDataSlicesPerJob*/ 1'000'000'000'000ll,
        /*maxDataWeightPerJob*/ 1_EB,
        /*primaryMaxDataWeightPerJob*/ 1_EB,
        /*inputSliceDataWeight*/ inputSliceDataWeight,
        /*inputSliceRowCount*/ std::max<i64>(1, totalRowCount / jobCount),
        /*batchRowCount*/ {},
        /*foreignSliceDataWeight*/ 0,
        /*samplingRate*/ std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseServer
