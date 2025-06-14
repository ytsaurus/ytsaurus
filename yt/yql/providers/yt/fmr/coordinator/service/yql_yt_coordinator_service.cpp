#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/yson/node/node_io.h>
#include <util/stream/file.h>
#include <util/system/interrupt_signals.h>
#include <yt/yql/providers/yt/fmr/coordinator/server/yql_yt_coordinator_server.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/gc_service/impl/yql_yt_gc_service_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/client/impl/yql_yt_table_data_service_client_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/file/yql_yt_file_service_discovery.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>
#include <yql/essentials/utils/mem_limit.h>

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

    void InitLogger() {
        NLog::ELevel level = NLog::ELevelHelpers::FromInt(Verbosity);
        NLog::EComponentHelpers::ForEach([level](NLog::EComponent c) {
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
        NYql::NLog::YqlLoggerScope logger(&Cerr);
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
        opts.SetFreeArgsMax(0);

        auto res = NLastGetopt::TOptsParseResult(&opts, argc, argv);

        options.InitLogger();

        TFmrCoordinatorSettings coordinatorSettings{};
        coordinatorSettings.WorkersNum = options.WorkersNum;

        if (options.FmrOperationSpecFilePath) {
            TFileInput input(options.FmrOperationSpecFilePath);
            auto fmrOperationSpec = NYT::NodeFromYsonStream(&input);
            coordinatorSettings.DefaultFmrOperationSpec = fmrOperationSpec;
        }
        TFmrCoordinatorServerSettings coordinatorServerSettings{.Port = options.Port, .Host = options.Host};
        ITableDataService::TPtr tableDataService = nullptr;
        if (options.TableDataServiceDiscoveryFilePath) {
            auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path = options.TableDataServiceDiscoveryFilePath});
            tableDataService = MakeTableDataServiceClient(tableDataServiceDiscovery);
        }

        auto gcService = MakeGcService(tableDataService);
        auto coordinator = MakeFmrCoordinator(coordinatorSettings, MakeYtCoordinatorService(), gcService);
        auto coordinatorServer = MakeFmrCoordinatorServer(coordinator, coordinatorServerSettings);
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
