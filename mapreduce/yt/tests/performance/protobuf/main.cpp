#include <mapreduce/yt/tests/performance/protobuf/row.pb.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/serialize.h>

#include <util/system/env.h>

#include <library/getopt/last_getopt.h>

using namespace NYT;
using namespace NYT::NDetail;
using namespace NTableClientBenchmark;

class TConsumingMapper
    : public IMapper<TTableReader<TIntermediateSemidupsDataProto>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        ui32 maxTableIndex = 0;
        for (const auto& cursor : *reader) {
            maxTableIndex = Max(maxTableIndex, cursor.GetRow().GetTableIndex());
        }
        writer->AddRow(TNode()("max", maxTableIndex));
    }
};
REGISTER_MAPPER(TConsumingMapper)

using TRunner = std::function<void(const IClientBasePtr&, const TRichYPath&, const TRichYPath&)>;

void RunConsumingMapper(const IClientBasePtr& client, const TRichYPath& input, const TRichYPath& output)
{
    client->Map(
        TMapOperationSpec()
            .AddInput<TIntermediateSemidupsDataProto>(input)
            .AddOutput<TNode>(output),
        new TConsumingMapper);
}

void RunOperation(
    const IClientBasePtr& client,
    TStringBuf type,
    const TRichYPath& input,
    const TRichYPath& output)
{
    static const THashMap<TString, TRunner> runners = {
        {"consuming-mapper", RunConsumingMapper},
    };

    auto it = runners.find(type); 
    if (it == runners.end()) {
        ythrow yexception() << "Unknown operation type \"" << type << "\"";
    }
    it->second(client, input, output);
}

int main(int argc, char** argv)
{
    Initialize(argc, argv);

    TString input, output, type;

    auto opts = NLastGetopt::TOpts::Default();

    opts.SetTitle("Tool to run operations");

    opts.AddLongOption("input", "Input table path").StoreResult(&input).Required();
    opts.AddLongOption("output", "Output table path").StoreResult(&output).Required();
    opts.AddLongOption("operation", "Type of operation to run").StoreResult(&type).Required();

    NLastGetopt::TOptsParseResult args(&opts, argc, argv);

    auto proxy = GetEnv("YT_PROXY");
    Y_ENSURE(proxy, "Specify proxy in YT_PROXY");

    auto client = CreateClient(proxy);
    RunOperation(client, type, TRichYPath(TString(input)), TRichYPath(TString(output)));

    return 0;
}
