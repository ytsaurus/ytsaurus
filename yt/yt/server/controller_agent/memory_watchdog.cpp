#include "memory_watchdog.h"

#include "private.h"

#include "config.h"
#include "controller_agent.h"
#include "operation.h"
#include "master_connector.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/memory/weak_ptr.h>

namespace NYT::NControllerAgent {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger MemoryWatchdogLogger("MemoryWatchdog");

////////////////////////////////////////////////////////////////////////////////

TMemoryWatchdog::TMemoryWatchdog(TMemoryWatchdogConfigPtr config, TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Config_(std::move(config))
    , MemoryCheckExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND(&TMemoryWatchdog::DoCheckMemoryUsage, MakeWeak(this)),
        Config_->MemoryUsageCheckPeriod))
    , Logger(MemoryWatchdogLogger)
{
    MemoryCheckExecutor_->Start();
}

void TMemoryWatchdog::DoCheckMemoryUsage()
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

    YT_LOG_DEBUG("Memory watchdog check started");

    Bootstrap_->GetControllerAgent()->GetMasterConnector()->SetControllerAgentAlert(
        EControllerAgentAlertType::ControllerMemoryOverconsumption,
        TError());

    auto operations = Bootstrap_->GetControllerAgent()->GetOperations();
    i64 totalMemory = 0;
    std::vector<std::tuple<TOperationId, IOperationControllerPtr, i64>> overconsumptingControllers;
    for (const auto& [operationId, operation] : operations) {
        auto controller = operation->GetController();
        if (!controller || controller->IsFinished() || controller->IsMemoryLimitExceeded()) {
            continue;
        }
        auto memory = controller->GetMemoryUsage();

        YT_LOG_DEBUG("Checking operation controller memory usage (OperationId: %v, MemoryUsage: %v, MemoryLimit: %v)",
            operationId,
            memory,
            Config_->OperationControllerMemoryLimit);

        if (memory > Config_->OperationControllerMemoryLimit) {
            YT_LOG_DEBUG("Aborting operation due to exceeded memory (OperationId: %v)", operationId);

            auto error = TError(EErrorCode::OperationControllerMemoryLimitExceeded,
                "Operation controller memory usage exceeds memory limit, probably input of the operation "
                "is too large, try splitting the operation into smaller ones")
                << TErrorAttribute("operation_controller_memory_usage", memory)
                << TErrorAttribute("operation_controller_memory_limit", Config_->OperationControllerMemoryLimit)
                << TErrorAttribute("operation_id", operationId);

            controller->OnMemoryLimitExceeded(error);
            continue;
        }

        totalMemory += memory;
        if (memory > Config_->OperationControllerMemoryOverconsumptionThreshold) {
            YT_LOG_DEBUG("Setting overconsumption alert for operation (OperationId: %v)", operationId);

            overconsumptingControllers.emplace_back(operationId, controller, memory);
            controller->SetOperationAlert(
                EOperationAlertType::MemoryOverconsumption,
                TError(
                    "Operation controller memory usage exceeds threshold; "
                    "it may be killed by memory watchdog in case of controller agent memory pressure, "
                    "consider reducing amount of input data")
                    << TErrorAttribute("threshold", Config_->OperationControllerMemoryOverconsumptionThreshold));
        }
    }

    YT_LOG_DEBUG("Checking total controller memory usages (TotalMemoryUsage: %v, MemoryLimit: %v)",
        totalMemory,
        Config_->TotalControllerMemoryLimit);

    if (!Config_->TotalControllerMemoryLimit || totalMemory <= *Config_->TotalControllerMemoryLimit) {
        return;
    }

    YT_LOG_DEBUG("Memory pressure detected, aborting topmost overflown operations");

    // Sorts operation controllers by memory.
    SortBy(overconsumptingControllers, [] (const auto& op) {
        return std::get<2>(op);
    });

    while (!overconsumptingControllers.empty() && totalMemory > Config_->TotalControllerMemoryLimit) {
        auto [operationId, controller, memory] = overconsumptingControllers.back();

        YT_LOG_DEBUG(
            "Aborting operation due to memory overconsumption under memory pressure (OperationId: %v)",
            operationId);

        controller->OnMemoryLimitExceeded(TError(
            EErrorCode::OperationControllerMemoryLimitExceeded,
            "Operation controller memory usage exceeds threshold: %v > %v",
            memory,
            Config_->OperationControllerMemoryOverconsumptionThreshold)
            << TErrorAttribute("operation_controller_memory_usage", memory)
            << TErrorAttribute("threshold", Config_->OperationControllerMemoryOverconsumptionThreshold));
        totalMemory -= memory;
        overconsumptingControllers.pop_back();
    }

    if (totalMemory > Config_->TotalControllerMemoryLimit) {
        Bootstrap_->GetControllerAgent()->GetMasterConnector()->SetControllerAgentAlert(
            EControllerAgentAlertType::ControllerMemoryOverconsumption,
            TError(
                "Total controller memory usage of running operations exceeds limit "
                "but no particular operation is above threshold")
                << TErrorAttribute("total_controller_memory_usage", totalMemory)
                << TErrorAttribute("total_controller_memory_limit", Config_->TotalControllerMemoryLimit));
    }

    YT_LOG_DEBUG("Memory wachdog check finished");
}

void TMemoryWatchdog::UpdateConfig(TMemoryWatchdogConfigPtr config)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

    Config_ = std::move(config);

    MemoryCheckExecutor_->SetPeriod(Config_->MemoryUsageCheckPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NControllerAgent
