#pragma once

#include "public.h"
#include "message_queue.h"

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
    explicit TControllerAgent(const NControllerAgent::TIncarnationId& incarnationId);

    DEFINE_BYVAL_RO_PROPERTY(NControllerAgent::TIncarnationId, IncarnationId);

    DEFINE_BYVAL_RW_PROPERTY(NYson::TYsonString, SuspiciousJobsYson);
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMessageQueueInbox, OperationEventsQueue);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgent)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
