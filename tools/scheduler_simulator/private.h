#pragma once

#include <yt/core/misc/intrusive_ptr.h>

#include <yt/core/logging/config.h>
#include <yt/core/logging/log_manager.h>

namespace NYT {
namespace NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TOperation)
DECLARE_REFCOUNTED_CLASS(TOperationController)

extern const NLogging::TLogger SchedulerSimulatorLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT
