#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/uri/http_url.h>
#include <util/system/interrupt_signals.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/table_data_service/server/yql_yt_table_data_service_server.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>
#include <yql/essentials/utils/mem_limit.h>

using namespace NYql::NFmr;
using namespace NYql;

volatile sig_atomic_t isInterrupted = 0;

struct TTableDataServiceWorkerRunOptions {
    ui16 Port;
    TString Host;
    int Verbosity;
    bool PrintStats = false;
    ui64 MaxDataWeight;

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
        NYql::NLog::YqlLoggerScope logger(&Cerr);
        TTableDataServiceWorkerRunOptions options;
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddHelpOption();
        opts.AddLongOption('p', "port", "Fast map reduce table data service worker port").StoreResult(&options.Port).DefaultValue(7000);
        opts.AddLongOption('h', "host", "Fast map reduce table data service worker host").StoreResult(&options.Host).DefaultValue("localhost");
        opts.AddLongOption('v', "verbosity", "Logging verbosity level").StoreResult(&options.Verbosity).DefaultValue(static_cast<int>(TLOG_ERR));
        opts.AddLongOption("mem-limit", "Set memory limit in megabytes").Handler1T<ui32>(0, SetAddressSpaceLimit);
        opts.AddLongOption('s', "print-stats", "Print stats").Optional().NoArgument().SetFlag(&options.PrintStats);
        opts.AddLongOption('w', "max-data-weight", "Max data weight limit for table data service").StoreResult(&options.MaxDataWeight).DefaultValue(10000000000);
        opts.SetFreeArgsMax(0);

        auto res = NLastGetopt::TOptsParseResult(&opts, argc, argv);

        options.InitLogger();

        TTableDataServiceServerSettings tableDataServiceSettings{
            .Host = options.Host,
            .Port = options.Port
        };
        auto tableDataService = MakeLocalTableDataService(TTableDataServiceSettings{.MaxDataWeight = options.MaxDataWeight});
        auto tableDataServiceServer = MakeTableDataServiceServer(tableDataService, tableDataServiceSettings);
        tableDataServiceServer->Start();

        while (!isInterrupted) {
            if (options.PrintStats) {
                YQL_CLOG(DEBUG, FastMapReduce) << tableDataService->GetStatistics().GetValueSync();
            }
            Sleep(TDuration::Seconds(2));
        }
        tableDataServiceServer->Stop();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
