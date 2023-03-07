#pragma once

#include "config.h"

#include <yt/ytlib/event_log/event_log.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

NEventLog::IEventLogWriterPtr CreateRemoteEventLogWriter(const TRemoteEventLogConfigPtr& config, const IInvokerPtr& invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
