#include <yt/cpp/mapreduce/interface/client.h>

#include <util/system/env.h>

using namespace NYT;

class TMockMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* /*reader*/, TWriter* /*writer*/) override
    {
        // do nothing
    }
};

REGISTER_MAPPER(TMockMapper);

int main(int argc, const char** argv) {
    const TString ytProxy = GetEnv("YT_PROXY");
    auto initialize = FromString<bool>(GetEnv("INITIALIZE", "true"));
    const TString inputTable = "//input";
    const TString outputTable = "//output";

    if (initialize) {
        Initialize(argc, argv);
    }

    auto client = CreateClient(ytProxy, TCreateClientOptions().UseCoreHttpClient(true));
    {
        auto writer = client->CreateTableWriter<TNode>(TRichYPath(inputTable));
        writer->AddRow(TNode()("foo", "bar"));
    }

    client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(inputTable)
            .AddOutput<TNode>(outputTable),
        new TMockMapper);

    return 0;
}
