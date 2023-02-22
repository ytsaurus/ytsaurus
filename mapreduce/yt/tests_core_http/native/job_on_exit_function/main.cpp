#include <mapreduce/yt/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/interface/config.h>
#include <mapreduce/yt/interface/client.h>
#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/utmain.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>
#include <util/generic/vector.h>

using namespace NYT;
using namespace NYT::NTesting;

class TIdMapper
    : public IMapper<TNodeReader, TNodeWriter>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
        Cerr << "Inside mapper" << Endl;
    }
};
REGISTER_MAPPER(TIdMapper);

int main(int argc, const char** argv)
{
    NYT::Initialize(argc, argv,
        TInitializeOptions().JobOnExitFunction([] {
            Cerr << "Inside JobOnExitFunction" << Endl;
            ::exit(1);
        }));

    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
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

    UNIT_ASSERT_EXCEPTION(opResult.GetValue(), TOperationFailedError);

    auto failedJobInfo = op->GetFailedJobInfo();
    UNIT_ASSERT_VALUES_EQUAL(failedJobInfo.size(), 1);
    TString expectedString = "Inside mapper\nInside JobOnExitFunction\n";
    UNIT_ASSERT_VALUES_EQUAL(failedJobInfo[0].Stderr, expectedString);
    return 0;
}
