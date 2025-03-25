#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/uri/http_url.h>
#include <util/system/interrupt_signals.h>
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
    ui64 WorkerId;
    ui64 WorkersNum;
    int Verbosity;

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
        TTableDataServiceWorkerRunOptions options;
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddHelpOption();
        opts.AddLongOption('p', "port", "Fast map reduce table data service worker port").StoreResult(&options.Port).DefaultValue(7000);
        opts.AddLongOption('h', "host", "Fast map reduce table data service worker host").StoreResult(&options.Host).DefaultValue("localhost");
        opts.AddLongOption('w', "worker-id", "Fast map reduce table data service worker id").Required().StoreResult(&options.WorkerId);
        opts.AddLongOption('n', "workers-num", "Fast map reduce table data service workers number").Required().StoreResult(&options.WorkersNum);
        opts.AddLongOption('v', "verbosity", "Logging verbosity level").StoreResult(&options.Verbosity).DefaultValue(static_cast<int>(TLOG_ERR));
        opts.AddLongOption("mem-limit", "Set memory limit in megabytes").Handler1T<ui32>(0, SetAddressSpaceLimit);
        opts.SetFreeArgsMax(0);

        auto res = NLastGetopt::TOptsParseResult(&opts, argc, argv);

        options.InitLogger();

        TTableDataServiceServerSettings tableDataServiceSettings{
            .WorkerId = options.WorkerId,
            .WorkersNum = options.WorkersNum,
            .Host = options.Host,
            .Port = options.Port
        };
        auto tableDataServiceServer = MakeTableDataServiceServer(tableDataServiceSettings);
        tableDataServiceServer->Start();

        while (!isInterrupted) {
            Sleep(TDuration::Seconds(1));
        }
        tableDataServiceServer->Stop();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
