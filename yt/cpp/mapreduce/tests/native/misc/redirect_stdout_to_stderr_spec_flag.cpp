#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/yt/memory/new.h>

#include <library/cpp/yt/string/format.h>

#include <util/generic/vector.h>

#include <util/system/env.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;
using namespace testing;

class TOutputToStdoutMapper
    : public IMapper<TNodeReader, TNodeWriter>
{
public:
    TOutputToStdoutMapper() = default;

    TOutputToStdoutMapper(TStringBuf str)
        : Stdout_(str)
    { }

    void Do(TReader* reader, TWriter* /*writer*/) override
    {
        for (; reader->IsValid(); reader->Next()) {
        }
        Cout << Stdout_;
    }

    Y_SAVELOAD_JOB(Stdout_);

private:
    TString Stdout_;
};
REGISTER_MAPPER(TOutputToStdoutMapper);

class TOutputToTablesMapper
    : public IMapper<TNodeReader, TNodeWriter>
{
public:
    TOutputToTablesMapper() = default;

    TOutputToTablesMapper(const TVector<TNode>& tablesData)
        : TablesData_(TNode::CreateList(tablesData))
    { }

    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
        }

        const auto& tablesData = TablesData_.AsList();

        for (size_t tableIndex = 0; tableIndex < tablesData.size(); ++tableIndex) {
            const auto& tableData = tablesData.at(tableIndex);
            writer->AddRow(tableData, tableIndex);
        }
    }

    Y_SAVELOAD_JOB(TablesData_);

private:
    TNode TablesData_;
};
REGISTER_MAPPER(TOutputToTablesMapper);

void CheckTableOutputForConfig(const TConfigPtr& config = nullptr)
{
    TTestFixture fixture(TCreateClientOptions()
        .Config(config));
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTablePath = workingDir + "/t_in";
    {
        auto writer= client->CreateTableWriter<TNode>(inputTablePath);
        writer->AddRow(TNode()("a", "1"));
        writer->Finish();
    }

    auto spec = TMapOperationSpec()
        .AddInput<TNode>(inputTablePath)
        .JobCount(1)
        .MaxFailedJobCount(1);

    TVector<TNode> expectedTablesData = {
        TNode()("a", 1),
        TNode()("b", 2)
    };

    TVector<TYPath> outputTablesPath;
    for (size_t i = 0; i < expectedTablesData.size(); ++i)
    {
        outputTablesPath.emplace_back(workingDir + Format("/t%v", i));
    }

    for (const auto& outputTablePath : outputTablesPath) {
        spec.AddOutput<TNode>(outputTablePath);
    }

    auto op = client->Map(spec,
        new TOutputToTablesMapper(expectedTablesData));

    op->Watch().Wait();

    for (size_t i = 0; i < expectedTablesData.size(); ++i) {
        auto reader = client->CreateTableReader<TNode>(outputTablesPath[i]);

        TVector<TNode> tableData;
        for (auto& cursor : *reader) {
            tableData.push_back(cursor.GetRow());
        }
        EXPECT_EQ(std::ssize(tableData), 1);
        EXPECT_EQ(tableData[0], expectedTablesData[i]);
    }
}

TEST(RedirectStdoutToStderrSpecFlag, Stdout)
{
    auto config = MakeIntrusive<TConfig>();
    config->RedirectStdoutToStderr = true;
    TTestFixture fixture(TCreateClientOptions()
        .Config(config));

    TStringBuf expectedStderr = "content";

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto inputTablePath = workingDir + "/t_in";
    auto outputTablePath = workingDir + "/t_out";
    {
        auto writer= client->CreateTableWriter<TNode>(inputTablePath);
        writer->AddRow(TNode()("a", "1"));
        writer->Finish();
    }

    auto spec = TMapOperationSpec()
        .AddInput<TNode>(inputTablePath)
        .AddOutput<TNode>(outputTablePath)
        .JobCount(1)
        .MaxFailedJobCount(1);

    auto op = client->Map(spec,
        new TOutputToStdoutMapper(expectedStderr));

    auto jobs = op->ListJobs().Jobs;
    EXPECT_EQ(std::ssize(jobs), 1);
    auto jobId = jobs[0].Id;
    EXPECT_TRUE(jobId);

    auto jobStderrStream = client->GetJobStderr(op->GetId(), *jobId);
    EXPECT_THAT(jobStderrStream->ReadAll(), HasSubstr(expectedStderr));
}

TEST(RedirectStdoutToStderrSpecFlag, TableOutputWithDefaultFlagValue)
{
    CheckTableOutputForConfig();
}

TEST(RedirectStdoutToStderrSpecFlag, TableOutputWithFalseFlagValue)
{
    auto config = MakeIntrusive<TConfig>();
    config->RedirectStdoutToStderr = false;
    CheckTableOutputForConfig(config);
}

TEST(RedirectStdoutToStderrSpecFlag, TableOutputWithTrueFlagValue)
{
    auto config = MakeIntrusive<TConfig>();
    config->RedirectStdoutToStderr = true;
    CheckTableOutputForConfig(config);
}

TEST(RedirectStdoutToStderrFlag, DisableForICommandJob)
{
    auto config = MakeIntrusive<TConfig>();
    config->RedirectStdoutToStderr = true;
    TTestFixture fixture(TCreateClientOptions()
        .Config(config));

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TRichYPath(workingDir + "/input");
    auto outputTable = TRichYPath(workingDir + "/output");
    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("a", "foo")("b", "bar"));
        writer->AddRow(TNode()("a", "baz")("b", "anime"));
        writer->Finish();
    }
    client->RawMap(
        TRawMapOperationSpec()
            .AddInput(inputTable)
            .AddOutput(outputTable)
            .Format(TFormat::Json()),
        new TCommandRawJob("grep nim"));

    TVector<TNode> rows = ReadTable(client, outputTable.Path_);
    EXPECT_EQ(rows, TVector{TNode()("a", "baz")("b", "anime")});
}
