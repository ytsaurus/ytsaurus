#pragma once

#include "private.h"

#include <yt/server/scheduler/public.h>

#include <yt/core/misc/phoenix.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct IJobSizeConstraints
    : public virtual TRefCounted
    , public virtual NPhoenix::IPersistent
{
    //! True if neither job count nor data weight per job were explicitly specified by user in spec.
    virtual bool CanAdjustDataWeightPerJob() const = 0;

    //! True if job count was explicitly specified by user in spec.
    virtual bool IsExplicitJobCount() const = 0;

    //! Job count, estimated from input statistics or provided via operation spec.
    virtual int GetJobCount() const = 0;

    //! Approximate data weight, estimated from input statistics or provided via operation spec.
    virtual i64 GetDataWeightPerJob() const = 0;

    //! Recommended upper limit on the number of chunk stripes per job.
    //! Can be overflown if exact job count is provided.
    virtual i64 GetMaxDataSlicesPerJob() const = 0;

    //! Recommended upper limit on the data size per job.
    //! Can be overflown if exact job count is provided.
    virtual i64 GetMaxDataWeightPerJob() const = 0;
    //! Recommended upper limit on the primary data size per job.
    //! Can be overflown if exact job count is provided.
    virtual i64 GetMaxPrimaryDataWeightPerJob() const = 0;

    virtual i64 GetInputSliceDataWeight() const = 0;
    virtual i64 GetInputSliceRowCount() const = 0;

    //! Approximate primary data size. Has meaning only in context of sorted operation.
    virtual i64 GetPrimaryDataWeightPerJob() const = 0;

    //! A sampling rate if it was specified in a job spec, otherwise null.
    virtual std::optional<double> GetSamplingRate() const = 0;
    //! When sampling is on, we initially create jobs of this data weight, sample them according to a given rate
    //! and join them together to fulfill data weight per job of `GetDataWeightPerJob()`.
    virtual i64 GetSamplingDataWeightPerJob() const = 0;
    //! Similar to previous, but for primary data weight per job.
    virtual i64 GetSamplingPrimaryDataWeightPerJob() const = 0;

    //! If during job building the slice count limit is exceeded, data weight per job is multiplied
    //! by this factor and the process is restarted.
    virtual double GetDataWeightPerJobRetryFactor() const = 0;
    //! If during job building we encounter an error that may disappear if we rebuild jobs with larger
    //! data weight per job, we try to rebuild jobs that many times.
    virtual i64 GetMaxBuildRetryCount() const = 0;

    //! Used to update the input data weight in case pool starts to know the better estimate than the
    //! controller by the moment of operation start. For example, when sampling is enabled, controller
    //! knows only the expected input data weight after the sampling, and actual data weight may be different.
    virtual void UpdateInputDataWeight(i64 inputDataWeight) = 0;

    virtual void Persist(const NPhoenix::TPersistenceContext& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

//! Fits for operations with user code.
IJobSizeConstraintsPtr CreateUserJobSizeConstraints(
    const NScheduler::TSimpleOperationSpecBasePtr& spec,
    const NControllerAgent::TSimpleOperationOptionsPtr& options,
    NLogging::TLogger logger,
    int outputTableCount,
    double dataWeightRatio,
    i64 inputChunkCount,
    i64 primaryInputDataWeight,
    i64 inputRowCount = std::numeric_limits<i64>::max(),
    i64 foreignInputDataWeight = 0,
    int inputTableCount = 1,
    int primaryInputTableCount = 1);

//! Fits for system operations like merge or erase.
IJobSizeConstraintsPtr CreateMergeJobSizeConstraints(
    const NScheduler::TSimpleOperationSpecBasePtr& spec,
    const NControllerAgent::TSimpleOperationOptionsPtr& options,
    NLogging::TLogger logger,
    i64 inputChunkCount,
    i64 inputDataWeight,
    double dataWeightRatio,
    double compressionRatio,
    int inputTableCount = 1,
    int primaryInputTableCount = 1);

IJobSizeConstraintsPtr CreateSimpleSortJobSizeConstraints(
    const NScheduler::TSortOperationSpecBasePtr& spec,
    const NControllerAgent::TSortOperationOptionsBasePtr& options,
    NLogging::TLogger logger,
    i64 inputDataWeight);

IJobSizeConstraintsPtr CreatePartitionJobSizeConstraints(
    const NScheduler::TSortOperationSpecBasePtr& spec,
    const NControllerAgent::TSortOperationOptionsBasePtr& options,
    NLogging::TLogger logger,
    i64 inputDataSize,
    i64 inputDataWeight,
    i64 inputRowCount,
    double compressionRatio);

IJobSizeConstraintsPtr CreatePartitionBoundSortedJobSizeConstraints(
    const NScheduler::TSortOperationSpecBasePtr& spec,
    const NControllerAgent::TSortOperationOptionsBasePtr& options,
    NLogging::TLogger logger,
    int outputTableCount);

IJobSizeConstraintsPtr CreateExplicitJobSizeConstraints(
    bool canAdjustDataSizePerJob,
    bool isExplicitJobCount,
    int jobCount,
    i64 dataWeightPerJob,
    i64 primaryDataWeightPerJob,
    i64 maxDataSlicesPerJob,
    i64 maxDataWeightPerJob,
    i64 maxPrimaryDataWeightPerJob,
    i64 inputSliceDataWeight,
    i64 inputSliceRowCount,
    std::optional<double> samplingRate,
    i64 samplingDataWeightPerJob = -1,
    i64 samplingPrimaryDataWeightPerJob = -1,
    i64 maxBuildRetryCount = 5,
    double dataWeightPerJobBuildRetryFactor = 2.0);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
