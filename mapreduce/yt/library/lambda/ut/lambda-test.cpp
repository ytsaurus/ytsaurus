#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>
#include <mapreduce/yt/library/lambda/yt_lambda.h>
#include <library/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

static void CreateTable(IClientPtr client, TString tableName, const TVector<TNode>& table) {
    auto writer = client->CreateTableWriter<TNode>(tableName);
    for (auto& elem : table) {
        writer->AddRow(elem);
    }
    writer->Finish();
}

static void CompareTable(IClientPtr client, TString tableName, const TVector<TNode>& expected) {
    auto reader = client->CreateTableReader<TNode>(tableName);
    TVector<TNode> table;
    for (; reader->IsValid(); reader->Next()) {
        table.push_back(reader->GetRow());
    }
    UNIT_ASSERT_VALUES_EQUAL(expected, table);
}

const TVector<TNode> InputTableData = {
    TNode()("Key", "first")("Val", 1u),
    TNode()("Key", "second")("Val", 20u),
    TNode()("Key", "third")("Val", 300u),
    TNode()("Key", "first")("Val", 4000u),
};

Y_UNIT_TEST_SUITE(Lambda) {
    Y_UNIT_TEST(CopyIf) {
        auto client = CreateTestClient();
        CreateTable(client, "//testing/input", InputTableData);

        CopyIf<TNode>(client, "//testing/input",  "//testing/output",
            [](auto& row) { return row["Val"].AsUint64() < 100; });

        TVector<TNode> expectedOutput = {
            TNode()("Key", "first")("Val", 1u),
            TNode()("Key", "second")("Val", 20u),
        };

        CompareTable(client, "//testing/output", expectedOutput);
    }

    Y_UNIT_TEST(TransformCopyIf) {
        auto client = CreateTestClient();
        CreateTable(client, "//testing/input", InputTableData);

        TransformCopyIf<TNode, TNode>(client, "//testing/input",  "//testing/output",
            [](auto& src, auto& dst) {
                if (src["Val"].AsUint64() >= 1000)
                    return false;
                dst["Key1"] = src["Key"];
                dst["Key2"] = src["Key"].AsString() + "Stuff";
                dst["Val"] = src["Val"];
                return true;
            });

        TVector<TNode> expectedOutput = {
            TNode()("Key1", "first")("Key2", "firstStuff")("Val", 1u),
            TNode()("Key1", "second")("Key2", "secondStuff")("Val", 20u),
            TNode()("Key1", "third")("Key2", "thirdStuff")("Val", 300u),
        };

        CompareTable(client, "//testing/output", expectedOutput);
    }

    Y_UNIT_TEST(AdditiveMapReduceSorted) {
        auto client = CreateTestClient();
        CreateTable(client, "//testing/input", InputTableData);

        AdditiveMapReduceSorted<TNode, TNode>(client, "//testing/input",  "//testing/output",
            { "Key1", "Key2" },
            [](auto& src, auto& dst) {
                dst["Key1"] = src["Key"];
                dst["Key2"] = TString(src["Key"].AsString().back()) + src["Key"].AsString();
                dst["Val"] = src["Val"];
                return true;
            },
            [](auto& src, auto& dst) {
                dst["Val"] = dst["Val"].AsUint64() + src["Val"].AsUint64();
            });

        TVector<TNode> expectedOutput = {
            TNode()("Key1", "first")("Key2", "tfirst")("Val", 4001u),
            TNode()("Key1", "second")("Key2", "dsecond")("Val", 20u),
            TNode()("Key1", "third")("Key2", "dthird")("Val", 300u),
        };

        CompareTable(client, "//testing/output", expectedOutput);
    }
}
