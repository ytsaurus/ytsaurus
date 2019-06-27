#include <yt/server/clickhouse_server/bootstrap.h>
#include <yt/server/clickhouse_server/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/core/phdr_cache/phdr_cache.h>

#include <yt/core/alloc/alloc.h>

#include <yt/core/misc/ref_counted_tracker_profiler.h>

#include <Common/config_version.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseServerProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<TClickHouseServerBootstrapConfig>
{
private:
    TString InstanceId_;
    TString CliqueId_;
    ui16 RpcPort_ = 0;
    ui16 MonitoringPort_ = 0;
    ui16 TcpPort_ = 0;
    ui16 HttpPort_ = 0;

public:
    TClickHouseServerProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    {
        Opts_.AddLongOption("instance-id", "ClickHouse instance id")
            .Required()
            .StoreResult(&InstanceId_);
        Opts_.AddLongOption("clique-id", "ClickHouse clique id")
            .Required()
            .StoreResult(&CliqueId_);
        Opts_.AddLongOption("rpc-port", "ytserver RPC port")
            .DefaultValue(9200)
            .StoreResult(&RpcPort_);
        Opts_.AddLongOption("monitoring-port", "ytserver monitoring port")
            .DefaultValue(9201)
            .StoreResult(&MonitoringPort_);
        Opts_.AddLongOption("tcp-port", "ClickHouse TCP port")
            .DefaultValue(9202)
            .StoreResult(&TcpPort_);
        Opts_.AddLongOption("http-port", "ClickHouse HTTP port")
            .DefaultValue(9203)
            .StoreResult(&HttpPort_);
        Opts_.AddLongOption("clickhouse-version", "ClickHouse version")
            .NoArgument()
            .Handler0(std::bind(&TClickHouseServerProgram::PrintClickHouseVersionAndExit, this));

        SetCrashOnError();
    }

private:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::CurrentThreadSetName("Main");

        ConfigureUids();
        ConfigureSignals();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();
        EnableRefCountedTrackerProfiling();
        NYTAlloc::EnableLogging();
        NYTAlloc::EnableProfiling();
        NYTAlloc::EnableStockpile();

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
        auto* bootstrap = new TBootstrap(
            std::move(config),
            std::move(configNode),
            InstanceId_,
            CliqueId_,
            RpcPort_,
            MonitoringPort_,
            TcpPort_,
            HttpPort_);
        bootstrap->Run();
    }

    void PrintClickHouseVersionAndExit() const
    {
        Cout << VERSION_DESCRIBE << Endl;
        Cout << VERSION_GITHASH << Endl;
        _exit(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

int main(int argc, const char** argv)
{
    return NYT::NClickHouseServer::TClickHouseServerProgram().Run(argc, argv);
}
