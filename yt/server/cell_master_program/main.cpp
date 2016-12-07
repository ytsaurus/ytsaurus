#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>

#include <yt/server/program/program.h>
#include <yt/server/program/program_config_mixin.h>
#include <yt/server/program/program_pdeathsig_mixin.h>

#include <yt/server/misc/configure_singletons.h>

#include <yt/core/logging/log_manager.h>
#include <yt/core/logging/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCellMasterProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<NCellMaster::TCellMasterConfig>
{
public:
    TCellMasterProgram()
        : TProgram()
        , TProgramPdeathsigMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    {
        Opts_
            .AddLongOption("dump-snapshot", "dump master snapshot and exit")
            .StoreMappedResult(&DumpSnapshot_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("validate-snapshot", "validate master snapshot and exit")
            .StoreMappedResult(&ValidateSnapshot_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::CurrentThreadSetName("MasterMain");

        ConfigureUids();
        ConfigureSignals();
        ConfigureCrashHandler();

        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        auto config = GetConfig();
        auto configNode = GetConfigNode();

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NCellMaster::TBootstrap(std::move(config), std::move(configNode));

        if (parseResult.Has("dump-snapshot")) {
            NLogging::TLogManager::Get()->Configure(NLogging::TLogConfig::CreateSilent());
            bootstrap->Initialize();
            bootstrap->TryLoadSnapshot(DumpSnapshot_, true);
        } else if (parseResult.Has("validate-snapshot")) {
            NLogging::TLogManager::Get()->Configure(NLogging::TLogConfig::CreateQuiet());
            bootstrap->Initialize();
            bootstrap->TryLoadSnapshot(ValidateSnapshot_, false);
        } else {
            ConfigureServerSingletons(config);
            bootstrap->Initialize();
            bootstrap->Run();
        }
    }

private:
    Stroka DumpSnapshot_;
    Stroka ValidateSnapshot_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TCellMasterProgram().Run(argc, argv);
}

