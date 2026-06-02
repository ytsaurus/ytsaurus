#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerProgram
    : public TServerProgram<TTabletBalancerProgramConfig>
{
public:
    TTabletBalancerProgram()
        : DryRunConfig_(New<TDryRunConfig>())
    {
        SetMainThreadName("TabBalancerProg");

        Opts_
            .AddLongOption(
                "run-once",
                "Read cluster state and schedule mutations for one bundle without applying anything")
            .SetFlag(&DryRunConfig_->IsDryRun)
            .NoArgument();

        Opts_.AddLongOption(
            "bundle", "Bundle to balance in dry-run mode")
            .StoreResult(&DryRunConfig_->Bundle)
            .RequiredArgument("BUNDLE");

        Opts_.AddLongOption(
            "apply", "Apply changes from run-once iteration")
            .SetFlag(&DryRunConfig_->CreateTabletActions)
            .NoArgument();

        Opts_.AddLongOption(
            "groups", "Groups to balance in dry run mode. All by default")
            .RequiredArgument("GROUPS")
            .SplitHandler(&DryRunConfig_->Groups, ' ');

        Opts_.AddLongOption(
            "mode", "Choose balancing mode from [move | reshard]. "
            "If not set, `reshard` is used")
            .RequiredArgument("MODE")
            .StoreResult(&Mode_);
    }

private:
    TDryRunConfigPtr DryRunConfig_;
    std::string Mode_;

    void ValidateOpts() final
    {
        if (DryRunConfig_->IsDryRun && Mode_.empty()) {
            THROW_ERROR_EXCEPTION("Option 'mode' is required in dry-run mode");
        }

        if (DryRunConfig_->IsDryRun && DryRunConfig_->Bundle.empty()) {
            THROW_ERROR_EXCEPTION("Option 'bundle' is required in dry-run mode");
        }
    }

    void DoStart() final
    {
        auto config = GetConfig();
        config->DryRun = DryRunConfig_;

        if (DryRunConfig_->IsDryRun) {
            config->TabletBalancer->ParameterizedTimeoutOnStart = TDuration{};
            config->TabletBalancer->ParameterizedTimeout = TDuration{};

            auto loggingConfig = config->GetSingletonConfig<NLogging::TLogManagerConfig>();
            loggingConfig->ShutdownGraceTimeout = TDuration::Seconds(10);
        }

        auto bootstrap = CreateTabletBalancerBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);

        if (DryRunConfig_->IsDryRun) {
            bootstrap->ExecuteBalancerIteration(DryRunConfig_);

            NLogging::TLogManager::Get()->Shutdown();

            AbortProcessSilently(EProcessExitCode::OK);
            return;
        }

        bootstrap->Run()
            .BlockingGet()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunTabletBalancerProgram(int argc, const char** argv)
{
    TTabletBalancerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
