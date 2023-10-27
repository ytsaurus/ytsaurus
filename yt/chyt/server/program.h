#pragma once

#include "bootstrap.h"
#include "config.h"
#include "clickhouse_config.h"
#include "version.h"

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/core/ytalloc/bindings.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <util/system/env.h>
#include <util/system/hostname.h>

#include <contrib/clickhouse/includes/configs/config_version.h>

#include <util/system/thread.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseServerProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<TClickHouseServerBootstrapConfig>
{
private:
    TString InstanceId_;
    TString CliqueId_;
    ui16 RpcPort_ = 0;
    ui16 MonitoringPort_ = 0;
    ui16 TcpPort_ = 0;
    ui16 HttpPort_ = 0;
    bool PrintClickHouseVersion_ = false;

public:
    TClickHouseServerProgram()
        : TProgram()
        , TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    {
        Opts_.AddLongOption("instance-id", "ClickHouse instance id")
            // Some special options (e.g. --version) do not require --instance-id.
            // To prevent misleading "missing argument" errors, we first handle these special options,
            // and then check required arguments by ourselves.
            // See ValidateRequiredArguments() for more details.
            // .Required()
            .StoreResult(&InstanceId_);
        Opts_.AddLongOption("clique-id", "ClickHouse clique id")
            // See ValidateRequiredArguments() for more details.
            // .Required()
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
            .StoreValue(&PrintClickHouseVersion_, true);

        SetCrashOnError();
    }

private:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        HandleClickHouseVersion();
        ValidateRequiredArguments(parseResult);

        TThread::SetCurrentThreadName("Main");

        ConfigureUids();
        ConfigureIgnoreSigpipe();
        // NB: ConfigureCrashHandler() is not called intentionally; crash handlers is set up in bootstrap.
        ConfigureExitZeroOnSigterm();
        ConfigureAllocator();

        if (HandleSetsidOptions()) {
            return;
        }
        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        PatchConfigFromEnv();

        auto config = GetConfig();
        auto configNode = GetConfigNode();

        ConfigureNativeSingletons(config);
        StartDiagnosticDump(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new TBootstrap(std::move(config), std::move(configNode));
        bootstrap->Run();
    }

    void PatchConfigFromEnv()
    {
        auto config = GetConfig();

        std::optional<TString> address;
        for (const auto& networkName : {"DEFAULT", "BB", "BACKBONE", "FASTBONE"}) {
            auto addressOrEmpty = GetEnv(Format("YT_IP_ADDRESS_%v", networkName), /*default =*/ "");
            if (!addressOrEmpty.empty()) {
                address = addressOrEmpty;
                break;
            }
        }

        if (address) {
            config->Yt->Address = "[" + *address + "]";
            // In MTN there may be no reasonable FQDN;
            // hostname returns something human-readable, but barely resolvable.
            // COMPAT(max42): move to launcher in future.
            config->AddressResolver->ResolveHostNameIntoFqdn = false;
            HttpPort_ = 10042;
            TcpPort_ = 10043;
            MonitoringPort_ = 10142;
            RpcPort_ = 10143;
        } else {
            config->Yt->Address = GetFQDNHostName();
        }

        config->MonitoringPort = MonitoringPort_;
        config->BusServer->Port = config->RpcPort = RpcPort_;
        config->ClickHouse->TcpPort = TcpPort_;
        config->ClickHouse->HttpPort = HttpPort_;
        config->Yt->CliqueId = TGuid::FromString(CliqueId_);
        config->Yt->InstanceId = TGuid::FromString(InstanceId_);
    }

    //! Override to print CHYT version.
    virtual void PrintVersionAndExit() override
    {
        Cout << GetCHYTVersion() << Endl;
        _exit(0);
    }

    void PrintClickHouseVersionAndExit()
    {
        Cout << VERSION_STRING << Endl;
        _exit(0);
    }

    void HandleClickHouseVersion()
    {
        if (PrintClickHouseVersion_) {
            PrintClickHouseVersionAndExit();
            Y_UNREACHABLE();
        }
    }

    void ValidateRequiredArguments(const NLastGetopt::TOptsParseResult& parseResult)
    {
        std::vector<TString> requiredArguments = {
            "instance-id",
            "clique-id",
        };

        bool missingRequiredArgument = false;

        for (const auto& argument: requiredArguments) {
            if (!parseResult.Has(argument)) {
                missingRequiredArgument = true;
                Cout << "Missing required argument: --" << argument << Endl;
            }
        }

        if (missingRequiredArgument) {
            _exit(1);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
