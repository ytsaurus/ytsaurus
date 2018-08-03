#include <yt/server/clickhouse/bootstrap.h>
#include <yt/server/clickhouse/server/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>

#include <library/getopt/small/last_getopt.h>

#include <util/generic/string.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<TConfig>
{
private:
    TString XmlConfig;
    TString CliqueId_;

public:
    TProgram();

private:
    void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override;
};

////////////////////////////////////////////////////////////////////////////////

TProgram::TProgram()
    : NYT::TProgram()
    , TProgramPdeathsigMixin(Opts_)
    , TProgramConfigMixin(Opts_)
{
    Opts_.AddLongOption("xml-config", "xml configuration file")
        .RequiredArgument()
        .DefaultValue("config.xml")
        .StoreResult(&XmlConfig);
    Opts_.AddLongOption("clique-id", "ClickHouse clique id (if not set, $YT_OPERATION_ID is used)")
        .DefaultValue("")
        .StoreResult(&CliqueId_);
}

void TProgram::DoRun(const NLastGetopt::TOptsParseResult& parseResult)
{
    Y_UNUSED(parseResult);

    ConfigureUids();
    ConfigureSignals();
    ConfigureCrashHandler();
    ConfigureExitZeroOnSigterm();

    if (HandlePdeathsigOptions()) {
        return;
    }

    if (HandleConfigOptions()) {
        return;
    }

    auto config = GetConfig();
    auto configNode = GetConfigNode();

    TBootstrap bootstrap {
        std::move(config),
        std::move(configNode),
        XmlConfig,
        CliqueId_,
    };

    bootstrap.Initialize();
    bootstrap.Run();

    // TODO
    Sleep(TDuration::Max());
}

}   // namespace NClickHouse
}   // namespace NYT

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    return NYT::NClickHouse::TProgram().Run(argc, argv);
}
