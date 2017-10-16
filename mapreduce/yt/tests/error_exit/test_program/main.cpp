#include <mapreduce/yt/interface/client.h>

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

    virtual void Do(TReader* /*reader*/, TWriter* /*writer*/) override
    {
        Sleep(TDuration::Seconds(SleepSeconds_));
        _exit(1);
    }

private:
    int SleepSeconds_ = 0;
};
REGISTER_MAPPER(TFailingMapper);

int main(int argc, const char** argv) {
    Initialize(argc, argv);

    const TString ytProxy = GetEnv("YT_PROXY");
    auto sleepSeconds = FromString<int>(GetEnv("SLEEP_SECONDS"));
    auto transactionTitle = GetEnv("TRANSACTION_TITLE");
    const TString inputTable = GetEnv("INPUT_TABLE");
    const TString outputTable = GetEnv("OUTPUT_TABLE");

    auto client = CreateClient(ytProxy);
    {
        auto writer = client->CreateTableWriter<TNode>(TRichYPath(inputTable));
        writer->AddRow(TNode()("foo", "bar"));
    }

    ITransactionPtr transaction = client->StartTransaction(TStartTransactionOptions().Title(transactionTitle));

    client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(inputTable)
            .AddOutput<TNode>(outputTable)
            .MaxFailedJobCount(1),
        new TFailingMapper(sleepSeconds));

    return 0;
}
