#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/errors.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;


Y_UNIT_TEST_SUITE(AlterTable) {
    Y_UNIT_TEST(Schema)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        client->Create(
            workingDir + "/table", NT_TABLE, TCreateOptions()
            .Attributes(
                TNode()
                ("schema", TNode()
                 .Add(TNode()("name", "eng")("type", "string"))
                 .Add(TNode()("name", "number")("type", "int64")))));
        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/table");
            writer->AddRow(TNode()("eng", "one")("number", 1));
            writer->Finish();
        }

        auto addRowWithRusColumn = [&] {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/table");
            writer->AddRow(TNode()("eng", "one")("number", 1)("rus", "odin"));
            writer->Finish();
        };

        UNIT_ASSERT_EXCEPTION(addRowWithRusColumn(), TErrorResponse);

        // Can't change type of column.
        UNIT_ASSERT_EXCEPTION(
            client->AlterTable(
                workingDir + "/table",
                TAlterTableOptions()
                .Schema(TTableSchema()
                    .AddColumn(TColumnSchema().Name("eng").Type(VT_INT64))
                    .AddColumn(TColumnSchema().Name("number").Type(VT_INT64)))),
            TErrorResponse);

        client->AlterTable(
            workingDir + "/table",
            TAlterTableOptions()
            .Schema(TTableSchema()
                .AddColumn(TColumnSchema().Name("eng").Type(VT_STRING))
                .AddColumn(TColumnSchema().Name("number").Type(VT_INT64))
                .AddColumn(TColumnSchema().Name("rus").Type(VT_STRING))));

        // No exception.
        addRowWithRusColumn();
    }

    Y_UNIT_TEST(WithTransaction)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto tx = client->StartTransaction();
        tx->Create(
            workingDir + "/table", NT_TABLE, TCreateOptions()
            .Attributes(
                TNode()
                ("schema", TNode()
                 .Add(TNode()("name", "eng")("type", "string"))
                 .Add(TNode()("name", "number")("type", "int64")))));

        const auto newSchema = TTableSchema()
                    .AddColumn(TColumnSchema().Name("eng").Type(VT_STRING))
                    .AddColumn(TColumnSchema().Name("number").Type(VT_INT64))
                    .AddColumn(TColumnSchema().Name("rus").Type(VT_STRING));

        UNIT_ASSERT_EXCEPTION(client->AlterTable(workingDir + "/table", TAlterTableOptions().Schema(newSchema)), TErrorResponse);

        // No exception.
        tx->AlterTable(workingDir + "/table", TAlterTableOptions().Schema(newSchema));
    }
}
