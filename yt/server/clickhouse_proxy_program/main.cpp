#include <yt/server/clickhouse_proxy/bootstrap.h>
#include <yt/server/clickhouse_proxy/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <library/getopt/small/last_getopt.h>

#include <util/generic/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseProxyProgram
    : public NYT::TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<NClickHouseProxy::TClickHouseProxyServerConfig>
{
public:
    TClickHouseProxyProgram()
        : TProgram()
        , TProgramPdeathsigMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    { }

private:
    void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::CurrentThreadSetName("ProxyMain");

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

        ConfigureSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NClickHouseProxy::TBootstrap(std::move(config), std::move(configNode));
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    return NYT::TClickHouseProxyProgram().Run(argc, argv);
}
