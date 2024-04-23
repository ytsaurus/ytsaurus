#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/errors.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NTesting;


TEST(AlterTable, Schema)
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

    EXPECT_THROW(addRowWithRusColumn(), TErrorResponse);

    // Can't change type of column.
    EXPECT_THROW(
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

TEST(AlterTable,WithTransaction)
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

    EXPECT_THROW(client->AlterTable(workingDir + "/table", TAlterTableOptions().Schema(newSchema)), TErrorResponse);

    // No exception.
    tx->AlterTable(workingDir + "/table", TAlterTableOptions().Schema(newSchema));
}

