#pragma once

#include "public.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateLegacyOrderedMapController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateLegacySortedMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateLegacyOrderedMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateLegacyEraseController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateLegacyReduceController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateLegacyJoinReduceController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
