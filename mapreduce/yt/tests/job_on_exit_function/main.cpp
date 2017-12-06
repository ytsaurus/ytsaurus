#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/node/node_io.h>

#include <library/unittest/utmain.h>
#include <library/unittest/registar.h>

#include <util/system/env.h>
#include <util/generic/vector.h>

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateTestClient()
{
    auto host = GetEnv("YT_PROXY");
    if (host.empty()) {
        ythrow yexception() << "YT_PROXY is not specified" << Endl;
    }
    return CreateClient(host);
}

////////////////////////////////////////////////////////////////////////////////

class TIdMapper
    : public IMapper<TNodeReader, TNodeWriter>
{
public:
    virtual void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
        Cerr << "Inside mapper" << Endl;
    }
};
REGISTER_MAPPER(TIdMapper);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::Initialize(argc, argv,
        TInitializeOptions().JobOnExitFunction([] {
            Cerr << "Inside JobOnExitFunction" << Endl;
            ::exit(1);
        }));

    auto client = CreateTestClient();
    {
        auto writer = client->CreateTableWriter<TNode>("//tmp/input");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

    auto op = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>("//tmp/input")
            .AddOutput<TNode>("//tmp/output")
            .MaxFailedJobCount(1),
        new TIdMapper,
        TOperationOptions().Wait(false));

    auto opResult = op->Watch();
    opResult.Wait();

    try {
        opResult.GetValue();
        Y_FAIL("Operation was expected to throw");
    } catch (const TOperationFailedError& ) {
    }

    auto failedJobInfo = op->GetFailedJobInfo();
    Y_VERIFY(failedJobInfo.size() == 1);
    TString expectedString = "Inside mapper\nInside JobOnExitFunction\n";
    Y_VERIFY(failedJobInfo[0].Stderr == expectedString, "%s != %s", ~failedJobInfo[0].Stderr, ~expectedString);
    return 0;
}
