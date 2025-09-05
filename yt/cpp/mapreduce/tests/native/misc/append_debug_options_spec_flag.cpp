#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/yt/memory/new.h>

#include <util/system/env.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;
using namespace testing;

class TEmptyMapper
    : public IMapper<TNodeReader, TNodeWriter>
{
public:
    TEmptyMapper() = default;

    void Do(TReader* reader, TWriter* /*writer*/) override
    {
        for (; reader->IsValid(); reader->Next()) {
        }
    }
};
REGISTER_MAPPER(TEmptyMapper);

TEST(AppendDebugOptionsSpecFlag, NotICommandJob)
{
    auto config = MakeIntrusive<TConfig>();
    config->AppendDebugOptions = true;
    TTestFixture fixture(TCreateClientOptions()
        .Config(config));

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TYPath(workingDir + "/input");
    auto outputTable = TYPath(workingDir + "/output");

    client->CreateTableWriter<TNode>(inputTable)->Finish();

    auto operation = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(inputTable)
            .AddOutput<TNode>(outputTable),
        new TEmptyMapper);

    EXPECT_TRUE(operation->GetAttributes().FullSpec->AsMap()["mapper"].AsMap()["append_debug_options"].AsBool());
}

TEST(AppendDebugOptionsSpecFlag, ICommandJob)
{
    auto config = MakeIntrusive<TConfig>();
    config->AppendDebugOptions = true;
    TTestFixture fixture(TCreateClientOptions()
        .Config(config));

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TYPath(workingDir + "/input");
    auto outputTable = TYPath(workingDir + "/output");

    client->CreateTableWriter<TNode>(inputTable)->Finish();

    auto operation = client->RawMap(
        TRawMapOperationSpec()
            .AddInput(inputTable)
            .AddOutput(outputTable)
            .Format(TFormat::Json()),
        new TCommandRawJob("cat"));

    EXPECT_FALSE(operation->GetAttributes().FullSpec->AsMap()["mapper"].AsMap()["append_debug_options"].AsBool());
}
