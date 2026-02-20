#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/yson/node/node_io.h>
#include <util/stream/file.h>
#include <util/system/interrupt_signals.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/impl/yql_yt_coordinator_service_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/server/yql_yt_coordinator_server.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/gc_service/impl/yql_yt_gc_service_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/client/impl/yql_yt_table_data_service_client_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/file/yql_yt_file_service_discovery.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>
#include <yt/yql/providers/yt/fmr/tvm/impl/yql_yt_fmr_tvm_impl.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>
#include <yql/essentials/utils/mem_limit.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_tvm_helpers.h>

using namespace NYql::NFmr;
using namespace NYql;

volatile sig_atomic_t isInterrupted = 0;

class TCoordinatorServerRunOptions {
public:
    ui16 Port;
    TString Host;
    ui32 WorkersNum;
    int Verbosity;
    TString FmrOperationSpecFilePath;
    TString TableDataServiceDiscoveryFilePath;
    TString UnderlyingGatewayType;
    TString LoggerFormat;
    TString FmrTvmConfig;
    TMaybe<ui32> FmrTvmPort;
    TMaybe<TString> FmrTvmSecretPath;

    void InitLogger() {
        NLog::ELevel level = NLog::TLevelHelpers::FromInt(Verbosity);
        NLog::TComponentHelpers::ForEach([level](NLog::EComponent c) {
            NYql::NLog::YqlLogger().SetComponentLevel(c, level);
        });
    }
};

void SignalHandler(int) {
    isInterrupted = 1;
}

int main(int argc, const char *argv[]) {
    try {
        SetInterruptSignalsHandler(SignalHandler);
        TCoordinatorServerRunOptions options;
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddHelpOption();
        opts.AddLongOption('p', "port", "Fast map reduce coordinator server port").StoreResult(&options.Port).DefaultValue(7000);
        opts.AddLongOption('h', "host", "Fast map reduce coordinator server host").StoreResult(&options.Host).DefaultValue("localhost");
        opts.AddLongOption('w', "workers-num", "Number of fast map reduce workers").StoreResult(&options.WorkersNum).DefaultValue(1);
        opts.AddLongOption('v', "verbosity", "Logging verbosity level").StoreResult(&options.Verbosity).DefaultValue(static_cast<int>(TLOG_ERR));
        opts.AddLongOption("mem-limit", "Set memory limit in megabytes").Handler1T<ui32>(0, SetAddressSpaceLimit);
        opts.AddLongOption('s', "fmr-operation-spec-path", "Path to file with fmr operation spec settings").Optional().StoreResult(&options.FmrOperationSpecFilePath);
        opts.AddLongOption('d', "table-data-service-discovery-file-path", "Table data service discovery file path").StoreResult(&options.TableDataServiceDiscoveryFilePath);
        opts.AddLongOption('g', "gateway-type", "Type of underlying gateway (native, file)").StoreResult(&options.UnderlyingGatewayType).DefaultValue("native");
        opts.AddLongOption('f', "logger-format", "Logs formatting type").StoreResult(&options.LoggerFormat).DefaultValue("legacy");
        opts.AddLongOption('t', "tvm-cfg", "fmr tvm config").Optional().StoreResult(&options.FmrTvmConfig);
        opts.AddLongOption("tvm-port", "fmr tvm port").Optional().StoreResult(&options.FmrTvmPort);
        opts.AddLongOption("tvm-secret-path", "fmr tvm secret path").Optional().StoreResult(&options.FmrTvmSecretPath);
        opts.SetFreeArgsMax(0);

        auto res = NLastGetopt::TOptsParseResult(&opts, argc, argv);

        TString loggerFormat = options.LoggerFormat;
        YQL_ENSURE(loggerFormat == "json" || loggerFormat == "legacy");
        auto formatter = loggerFormat == "json" ? NYql::NLog::JsonFormat : NYql::NLog::LegacyFormat;
        NYql::NLog::YqlLoggerScope logger(&Cerr, formatter);

        options.InitLogger();

        TFmrCoordinatorSettings coordinatorSettings{};
        coordinatorSettings.WorkersNum = options.WorkersNum;

        TString underlyingGatewayType = options.UnderlyingGatewayType;
        if (underlyingGatewayType != "native" && underlyingGatewayType != "file") {
            throw yexception() << " Incorrect gateway type " << underlyingGatewayType << " passed in parameters";
        }
        bool isNative = underlyingGatewayType == "native";

        if (options.FmrOperationSpecFilePath) {
            TFileInput input(options.FmrOperationSpecFilePath);
            auto fmrOperationSpec = NYT::NodeFromYsonStream(&input);
            coordinatorSettings.DefaultFmrOperationSpec = fmrOperationSpec;
        }

        TFmrCoordinatorServerSettings coordinatorServerSettings{.Port = options.Port, .Host = options.Host};

        IFmrTvmClient::TPtr coordinatorTvmClient = nullptr;
        ui64 tableDataServiceTvmId = 0;

        auto tvmSpec = ParseFmrTvmSpec(options.FmrTvmConfig);
        if (tvmSpec.Defined()) {
            auto tvmSecret = ParseFmrTvmSecretFile(options.FmrTvmSecretPath);
            coordinatorTvmClient = MakeFmrTvmClient(TFmrTvmToolSettings{
                .SourceTvmAlias = tvmSpec->CoordinatorTvmAlias,
                .TvmPort = options.FmrTvmPort,
                .TvmSecret = tvmSecret
            });
            tableDataServiceTvmId = tvmSpec->TableDataServiceTvmId;
            coordinatorServerSettings.AllowedSourceTvmIds = {tvmSpec->WorkerTvmId};
            // TODO - add gateway tvm id to allowed source ids when it is supported in yql.
        }

        ITableDataService::TPtr tableDataService = nullptr;
        if (options.TableDataServiceDiscoveryFilePath) {
            auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path = options.TableDataServiceDiscoveryFilePath});
            tableDataService = MakeTableDataServiceClient(tableDataServiceDiscovery, coordinatorTvmClient, tableDataServiceTvmId);
        }

        auto gcService = MakeGcService(tableDataService);
        IYtCoordinatorService::TPtr ytCoordinatorService = isNative ? MakeYtCoordinatorService() : MakeFileYtCoordinatorService();
        auto coordinator = MakeFmrCoordinator(coordinatorSettings, ytCoordinatorService, gcService);
        auto coordinatorServer = MakeFmrCoordinatorServer(coordinator, coordinatorServerSettings, coordinatorTvmClient);
        coordinatorServer->Start();

        while (!isInterrupted) {
            Sleep(TDuration::Seconds(1));
        }
        coordinatorServer->Stop();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
