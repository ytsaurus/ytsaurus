#pragma once

#include <core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TWorkloadDescriptor;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

//! The type of the workload.
//! Higher is better.
/*!
 *  This is the most fine-grained categorization available.
 *  Most subsystems will map EWorkloadCategory to their own coarser categories.
 */
DEFINE_ENUM(EWorkloadCategory,
    (Idle)
    (SystemReplication)
    (SystemRepair)
    (UserBatch)
    (UserRealtime)
    (SystemRealtime)
);

struct TWorkloadDescriptor;

DECLARE_REFCOUNTED_CLASS(TWorkloadConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
