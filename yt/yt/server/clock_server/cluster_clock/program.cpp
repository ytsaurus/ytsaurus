#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

class TClusterClockProgram
    : public TServerProgram<TClusterClockConfig>
{
public:
    TClusterClockProgram()
    {
        Opts_
            .AddLongOption(
                "dump-snapshot",
                "Dumps clock snapshot\n"
                "Expects path to snapshot")
            .Handler0([&] { DumpSnapshotFlag_ = true; })
            .StoreMappedResult(&LoadSnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption(
                "validate-snapshot",
                "Loads clock snapshot in a dry run mode\n"
                "Expects path to snapshot")
            .Handler0([&] { ValidateSnapshotFlag_ = true; })
            .StoreMappedResult(&LoadSnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");

        SetMainThreadName("Clock");
    }

private:
    bool IsDumpSnapshotMode() const
    {
        return DumpSnapshotFlag_;
    }

    bool IsValidateSnapshotMode() const
    {
        return ValidateSnapshotFlag_;
    }

    bool IsDryRunMode() const
    {
        return
            IsDumpSnapshotMode() ||
            IsValidateSnapshotMode();
    }

    void ValidateOpts() final
    {
        if (static_cast<int>(IsDumpSnapshotMode()) +
            static_cast<int>(IsValidateSnapshotMode()) > 1)
        {
            THROW_ERROR_EXCEPTION("Options 'dump-snapshot' and 'validate-snapshot' are mutually exclusive");
        }
    }

    void TweakConfig() final
    {
        auto config = GetConfig();

        if (IsDumpSnapshotMode()) {
            config->Logging = NLogging::TLogManagerConfig::CreateSilent();
        }

        if (IsValidateSnapshotMode()) {
            config->Logging = NLogging::TLogManagerConfig::CreateQuiet();
        }
    }

    void DoStart() final
    {
        auto* bootstrap = new NClusterClock::TBootstrap(GetConfig(), GetConfigNode());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Initialize();

        if (IsDryRunMode()) {
            NBus::TTcpDispatcher::Get()->DisableNetworking();

            if (IsDumpSnapshotMode()) {
                bootstrap->TryLoadSnapshot(LoadSnapshotPath_, true);
                return;
            }

            if (IsValidateSnapshotMode()) {
                bootstrap->TryLoadSnapshot(LoadSnapshotPath_, false);
                return;
            }
        }

        bootstrap->Run();
        SleepForever();
    }

private:
    bool DumpSnapshotFlag_ = false;
    bool ValidateSnapshotFlag_ = false;
    TString LoadSnapshotPath_;
};

////////////////////////////////////////////////////////////////////////////////

void RunClusterClockProgram(int argc, const char** argv)
{
    TClusterClockProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
