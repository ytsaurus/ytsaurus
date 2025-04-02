#include "program.h"

#include "config.h"
#include "bootstrap.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerProgram
    : public TServerProgram<TCellBalancerProgramConfig>
{
public:
    TCellBalancerProgram()
    {
        SetMainThreadName("CBProg");

        Opts_
            .AddLongOption(
                "dry-run",
                "Read cluster state and schedule mutations without applying anything")
            .SetFlag(&DryRun_)
            .NoArgument();
    }

private:
    bool DryRun_ = false;

    void DoStart() final
    {
        if (DryRun_) {
            auto config = GetConfig();

            auto loggingConfig = config->GetSingletonConfig<NLogging::TLogManagerConfig>();
            loggingConfig->ShutdownGraceTimeout = TDuration::Seconds(10);
        }

        auto bootstrap = CreateCellBalancerBootstrap(
            GetConfig(),
            GetConfigNode(),
            GetServiceLocator());
        DoNotOptimizeAway(bootstrap);

        if (DryRun_) {
            bootstrap->ExecuteDryRunIteration();

            NLogging::TLogManager::Get()->Shutdown();

            AbortProcessSilently(EProcessExitCode::OK);
            return;
        }

        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunCellBalancerProgram(int argc, const char** argv)
{
    TCellBalancerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
