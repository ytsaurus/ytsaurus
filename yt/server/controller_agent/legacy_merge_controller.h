#pragma once

#include "public.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateLegacyOrderedMapController(
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateLegacySortedMergeController(
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateLegacyOrderedMergeController(
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateLegacyEraseController(
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateLegacyReduceController(
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateLegacyJoinReduceController(
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
