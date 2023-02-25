#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/library/skynet_table/skynet_table_writer.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>


using namespace NYT;
using namespace NYT::NTesting;
using namespace NYtSkynetTable;

class TSkynetGeneratingTable
    : public IVanillaJob<TTableWriter<TSkynetTableRow>>
{
public:
    void Do(TWriter* writer) override
    {
        auto skynetTableWriter = CreateSkynetWriter(writer);
        {
            auto fileWriter = skynetTableWriter->AppendFile("bar");
            (*fileWriter) << "GG\n";
        }
        {
            auto fileWriter = skynetTableWriter->AppendFile("foo");
            (*fileWriter) << "LOL\n";
        }
        writer->Finish();
    }
};
REGISTER_VANILLA_JOB(TSkynetGeneratingTable);

Y_UNIT_TEST_SUITE(SkynetTable) {
    Y_UNIT_TEST(ClientWrite)
    {
        auto client = CreateTestClient();
        auto writer = CreateSkynetWriter(client, "//tmp/skynet-table");
        {
            auto fileWriter = writer->AppendFile("bar");
            (*fileWriter) << "GG\n";
        }
        {
            auto fileWriter = writer->AppendFile("foo");
            (*fileWriter) << "LOL\n";
        }
        writer->Finish();

        std::vector<TNode> result;
        auto reader = client->CreateTableReader<TNode>("//tmp/skynet-table");
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            UNIT_ASSERT(!row["sha1"].AsString().empty());
            UNIT_ASSERT(!row["md5"].AsString().empty());
            result.push_back(TNode()("filename", row["filename"])("data", row["data"]));
        }
        UNIT_ASSERT_VALUES_EQUAL(result, std::vector<TNode>({
            TNode()("filename", "bar")("data", "GG\n"),
            TNode()("filename", "foo")("data", "LOL\n"),
        }));
    }

    Y_UNIT_TEST(OperationWrite)
    {
        auto client = CreateTestClient();
        CreateSkynetTable(client, "//tmp/skynet-client");

        client->RunVanilla(
            TVanillaOperationSpec()
            .AddTask(
                TVanillaTask()
                .AddOutput<TSkynetTableRow>("//tmp/skynet-client")
                .Job(new TSkynetGeneratingTable)
                .JobCount(1)
                .Name("foo")
            )
        );

        std::vector<TNode> result;
        auto reader = client->CreateTableReader<TNode>("//tmp/skynet-table");
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            UNIT_ASSERT(!row["sha1"].AsString().empty());
            UNIT_ASSERT(!row["md5"].AsString().empty());
            result.push_back(TNode()("filename", row["filename"])("data", row["data"]));
        }
        UNIT_ASSERT_VALUES_EQUAL(result, std::vector<TNode>({
            TNode()("filename", "bar")("data", "GG\n"),
            TNode()("filename", "foo")("data", "LOL\n"),
        }));
    }
}
