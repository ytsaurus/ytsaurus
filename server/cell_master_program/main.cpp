#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/core/logging/log_manager.h>
#include <yt/core/logging/config.h>

#include <yt/core/misc/phdr_cache.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCellMasterProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<NCellMaster::TCellMasterConfig>
{
public:
    TCellMasterProgram()
        : TProgramPdeathsigMixin(Opts_)
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

        bool dumpSnapshot = parseResult.Has("dump-snapshot");
        bool validateSnapshot = parseResult.Has("validate-snapshot");

        ConfigureUids();
        ConfigureSignals();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();

        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        auto config = GetConfig();
        auto configNode = GetConfigNode();

        if (dumpSnapshot) {
            config->Logging = NLogging::TLogConfig::CreateSilent();
        } else if (validateSnapshot) {
            config->Logging = NLogging::TLogConfig::CreateQuiet();
        }

        ConfigureSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NCellMaster::TBootstrap(std::move(config), std::move(configNode));
        bootstrap->Initialize();

        if (dumpSnapshot) {
            bootstrap->TryLoadSnapshot(DumpSnapshot_, true);
        } else if (validateSnapshot) {
            bootstrap->TryLoadSnapshot(ValidateSnapshot_, false);
        } else {
            bootstrap->Run();
        }
    }

private:
    TString DumpSnapshot_;
    TString ValidateSnapshot_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TCellMasterProgram().Run(argc, argv);
}

