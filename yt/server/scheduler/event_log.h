#pragma once

#include "public.h"

#include <yt/ytlib/event_log/event_log.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELogEventType,
    (SchedulerStarted)
    (MasterConnected)
    (MasterDisconnected)
    (JobStarted)
    (JobCompleted)
    (JobFailed)
    (JobAborted)
    (OperationStarted)
    (OperationCompleted)
    (OperationFailed)
    (OperationAborted)
    (FairShareInfo)
    (OperationPrepared)
    (ClusterInfo)
    (PoolsInfo)
    (RuntimeParametersInfo)
);

////////////////////////////////////////////////////////////////////////////////

struct IEventLogHost
{
    virtual ~IEventLogHost() = default;

    virtual NEventLog::TFluentLogEvent LogEventFluently(ELogEventType eventType) = 0;
    virtual NEventLog::TFluentLogEvent LogEventFluently(ELogEventType eventType, TInstant now) = 0;
};

class TEventLogHostBase
    : public virtual IEventLogHost
{
public:
    virtual NEventLog::TFluentLogEvent LogEventFluently(ELogEventType eventType) override;
    virtual NEventLog::TFluentLogEvent LogEventFluently(ELogEventType eventType, TInstant now) override;

protected:
    virtual NYson::IYsonConsumer* GetEventLogConsumer() = 0;

private:
    NEventLog::TFluentEventLogger EventLogger_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
