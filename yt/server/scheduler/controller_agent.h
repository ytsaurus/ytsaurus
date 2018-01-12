#pragma once

#include "public.h"

#include <yt/server/controller_agent/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/misc/property.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Scheduler-side representation of a controller agent.
/*!
 *  Thread affinity: Control thread (unless noted otherwise)
 */
class TControllerAgent
    : public TIntrinsicRefCounted
{
public:
    TControllerAgent();

    DEFINE_BYVAL_RW_PROPERTY(NYson::TYsonString, SuspiciousJobsYson);
    DEFINE_BYVAL_RW_PROPERTY(NControllerAgent::TIncarnationId, IncarnationId);
    DEFINE_BYVAL_RW_PROPERTY(NRpc::TMutationId, LastSeenHeartbeatMutationId);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgent)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
