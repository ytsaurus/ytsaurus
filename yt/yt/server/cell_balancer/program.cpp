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
                "apply",
                "Apply changes from run-once iteration")
            .SetFlag(&Apply_)
            .NoArgument();

        Opts_
            .AddLongOption(
                "run-once",
                "Read cluster state and schedule mutations without applying anything")
            .SetFlag(&RunOnce_)
            .NoArgument();
    }

private:
    bool Apply_ = false;
    bool RunOnce_ = false;

    void DoStart() final
    {
        if (RunOnce_) {
            auto config = GetConfig();

            auto loggingConfig = config->GetSingletonConfig<NLogging::TLogManagerConfig>();
            loggingConfig->ShutdownGraceTimeout = TDuration::Seconds(10);
        }

        auto bootstrap = CreateCellBalancerBootstrap(
            GetConfig(),
            GetConfigNode(),
            GetServiceLocator());
        DoNotOptimizeAway(bootstrap);

        if (RunOnce_) {
            bootstrap->ExecuteIteration(!Apply_);

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
