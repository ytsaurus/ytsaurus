#pragma once

#include "private.h"

#include <yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/core/misc/phoenix.h>

namespace NYT::NControllerAgent {

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
    int primaryInputTableCount = 1,
    bool sortedOperation = false);

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
