#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/yt/core/misc/shutdown.h>

#include <util/system/env.h>

using namespace NYT;

class TFailingMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    Y_SAVELOAD_JOB(SleepSeconds_);

    TFailingMapper() = default;

    TFailingMapper(int sleepSeconds)
        : SleepSeconds_(sleepSeconds)
    { }

    void Do(TReader* /*reader*/, TWriter* /*writer*/) override
    {
        Sleep(TDuration::Seconds(SleepSeconds_));
        _exit(1);
    }

private:
    int SleepSeconds_ = 0;
};
REGISTER_MAPPER(TFailingMapper);

////////////////////////////////////////////////////////////////////////////////

class TFailingAndDeadlockingLogger
    : public ILogger
{
public:
    void Log(ELevel, const ::TSourceLocation&, const char*, va_list) override
    {
        Mutex_.Acquire();
        ythrow yexception() << "OOPS";
    }

private:
    TMutex Mutex_;
};

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv) {
    if (GetEnv("EXIT_CODE_FOR_TERMINATE")) {
        std::set_terminate([] {
            int exitCode = FromString<int>(GetEnv("EXIT_CODE_FOR_TERMINATE"));
            if (FromString<bool>(GetEnv("CALL_SHUTDOWN_HANDLERS"))) {
                NYT::Shutdown({
                    .GraceTimeout = TDuration::Seconds(5),
                    .AbortOnHang = false,
                    .HungExitCode = exitCode,
                });
                // NYT::Shutdown doesn't wait for threads from yt/cpp/mapreduce/http client
                if (TConfig::Get()->UseAsyncTxPinger) {
                    Y_FAIL("Shutdown should have never returned");
                } else {
                    _exit(exitCode);
                }
            } else {
                _exit(exitCode);
            }
        });
    }

    Initialize(argc, argv);

    TConfig::Get()->LogLevel = "debug";
    TString ytProxy = GetEnv("YT_PROXY");
    auto sleepSeconds = FromString<int>(GetEnv("SLEEP_SECONDS"));
    auto transactionTitle = GetEnv("TRANSACTION_TITLE");
    TString inputTable = GetEnv("INPUT_TABLE");
    TString outputTable = GetEnv("OUTPUT_TABLE");
    bool failAndDeadlockLogger = FromString<bool>(GetEnv("FAIL_AND_DEADLOCK_LOGGER", "0"));

    auto client = CreateClient(ytProxy, TCreateClientOptions().UseCoreHttpClient(true));
    {
        auto writer = client->CreateTableWriter<TNode>(TRichYPath(inputTable));
        writer->AddRow(TNode()("foo", "bar"));
    }

    ITransactionPtr transaction = client->StartTransaction(TStartTransactionOptions().Title(transactionTitle));

    if (failAndDeadlockLogger) {
        TFailingAndDeadlockingLogger logger;
        SetLogger(&logger);
        YT_LOG_DEBUG("Not so fast");
        Y_FAIL("YT_LOG_DEBUG should have never returned");
    } else {
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable)
                .MaxFailedJobCount(1),
            new TFailingMapper(sleepSeconds));
    }

    return 0;
}
