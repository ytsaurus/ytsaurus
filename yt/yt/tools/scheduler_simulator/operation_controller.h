#pragma once

#include "private.h"
#include "operation_description.h"
#include "scheduling_context.h"

#include <yt/yt/server/scheduler/operation.h>
#include <yt/yt/server/scheduler/operation_controller.h>

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/core/logging/log_manager.h>

#include <deque>
#include <map>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

struct ISimulatorOperationController
    : public NScheduler::ISchedulingOperationController
{
    virtual void OnJobCompleted(std::unique_ptr<NControllerAgent::TCompletedJobSummary> jobSummary) = 0;

    virtual bool IsOperationCompleted() const = 0;

    virtual TString GetLoggingProgress() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISimulatorOperationController)

ISimulatorOperationControllerPtr CreateSimulatorOperationController(
    const TOperation* operation,
    const TOperationDescription* operationDescription,
    NScheduler::TDelayConfigPtr scheduleJobDelay);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
