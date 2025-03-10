#pragma once

#include "private.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/scheduler/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! When neither data_size_per_job nor job_count are specified,
//! we need to get a hint about job size from another source.
DEFINE_ENUM(EDataSizePerMergeJobHint,
    //! Use the value from T${OperationType}OperationOptions
    //! from the dynamic config. Useful when DesiredChunkSize is meaningless
    //! (for example, when remote_copying files).
    (OperationOptions)
    //! Try to optimize writer's memory using
    //! #TMultiChunkWriterConfig::DesiredChunkSize.
    (DesiredChunkSize)
);

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
    i64 primaryInputCompressedDataSize,
    i64 inputRowCount = std::numeric_limits<i64>::max() / 4,
    i64 foreignInputDataWeight = 0,
    i64 foreignInputCompressedDataSize = 0,
    int inputTableCount = 1,
    int primaryInputTableCount = 1,
    bool sortedOperation = false);

//! Fits for system operations like merge or erase.
IJobSizeConstraintsPtr CreateMergeJobSizeConstraints(
    const NScheduler::TSimpleOperationSpecBasePtr& spec,
    const NControllerAgent::TSimpleOperationOptionsPtr& options,
    NLogging::TLogger logger,
    i64 inputChunkCount,
    i64 inputDataWeight,
    i64 inputCompressedDataSize,
    double dataWeightRatio,
    double compressionRatio,
    int inputTableCount = 1,
    int primaryInputTableCount = 1,
    EDataSizePerMergeJobHint dataSizeHint = EDataSizePerMergeJobHint::DesiredChunkSize);

IJobSizeConstraintsPtr CreateSimpleSortJobSizeConstraints(
    const NScheduler::TSortOperationSpecBasePtr& spec,
    const NControllerAgent::TSortOperationOptionsBasePtr& options,
    NLogging::TLogger logger,
    i64 inputDataWeight,
    i64 inputCompressedDataSize);

IJobSizeConstraintsPtr CreatePartitionJobSizeConstraints(
    const NScheduler::TSortOperationSpecBasePtr& spec,
    const NControllerAgent::TSortOperationOptionsBasePtr& options,
    NLogging::TLogger logger,
    i64 inputUncompressedDataSize,
    i64 inputDataWeight,
    i64 inputRowCount,
    double compressionRatio);

IJobSizeConstraintsPtr CreatePartitionBoundSortedJobSizeConstraints(
    const NScheduler::TSortOperationSpecBasePtr& spec,
    const NControllerAgent::TSortOperationOptionsBasePtr& options,
    int outputTableCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
