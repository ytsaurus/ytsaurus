#pragma once

#include "public.h"

#include <yt/yt/ytlib/event_log/event_log.h>

namespace NYT::NScheduler {

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
    (OperationPrepared)
    (OperationMaterialized)
    (OperationBannedInTree)
    (FairShareInfo)
    (ClusterInfo)
    (NodesInfo)
    (PoolsInfo)
    (RuntimeParametersInfo)
    (AccumulatedUsageInfo)
);

////////////////////////////////////////////////////////////////////////////////

struct IEventLogHost
{
    virtual ~IEventLogHost() = default;

    virtual NEventLog::TFluentLogEvent LogEventFluently(ELogEventType eventType) = 0;
    virtual NEventLog::TFluentLogEvent LogEventFluently(const NLogging::TLogger* eventLogger, ELogEventType eventType) = 0;
    virtual NEventLog::TFluentLogEvent LogEventFluently(const NLogging::TLogger* eventLogger, ELogEventType eventType, TInstant now) = 0;
};

class TEventLogHostBase
    : public virtual IEventLogHost
{
public:
    NEventLog::TFluentLogEvent LogEventFluently(ELogEventType eventType) override;
    NEventLog::TFluentLogEvent LogEventFluently(const NLogging::TLogger* eventLogger, ELogEventType eventType) override;
    NEventLog::TFluentLogEvent LogEventFluently(const NLogging::TLogger* eventLogger, ELogEventType eventType, TInstant now) override;

protected:
    NEventLog::TFluentLogEvent LogEventFluently(
        const NLogging::TLogger* eventLogger,
        NYson::IYsonConsumer* eventLogConsumer,
        ELogEventType eventType,
        TInstant now);

    virtual const NLogging::TLogger* GetEventLogger() = 0;
    virtual NYson::IYsonConsumer* GetEventLogConsumer() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
