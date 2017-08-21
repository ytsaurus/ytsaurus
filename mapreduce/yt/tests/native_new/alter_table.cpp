#include "lib.h"

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/http/error.h>

#include <library/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;


SIMPLE_UNIT_TEST_SUITE(AlterTable) {
    SIMPLE_UNIT_TEST(Schema)
    {
        auto client = CreateTestClient();
        client->Create(
            "//testing/table", NT_TABLE, TCreateOptions()
            .Attributes(
                TNode()
                ("schema", TNode()
                 .Add(TNode()("name", "eng")("type", "string"))
                 .Add(TNode()("name", "number")("type", "int64")))));
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("eng", "one")("number", 1));
            writer->Finish();
        }

        auto addRowWithRusColumn = [&] {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("eng", "one")("number", 1)("rus", "odin"));
            writer->Finish();
        };

        UNIT_ASSERT_EXCEPTION(addRowWithRusColumn(), TErrorResponse);

        // Can't change type of column.
        UNIT_ASSERT_EXCEPTION(
            client->AlterTable(
                "//testing/table",
                TAlterTableOptions()
                .Schema(TTableSchema()
                    .AddColumn(TColumnSchema().Name("eng").Type(VT_INT64))
                    .AddColumn(TColumnSchema().Name("number").Type(VT_INT64)))),
            TErrorResponse);

        client->AlterTable(
            "//testing/table",
            TAlterTableOptions()
            .Schema(TTableSchema()
                .AddColumn(TColumnSchema().Name("eng").Type(VT_STRING))
                .AddColumn(TColumnSchema().Name("number").Type(VT_INT64))
                .AddColumn(TColumnSchema().Name("rus").Type(VT_STRING))));

        // No exception.
        addRowWithRusColumn();
    }

    SIMPLE_UNIT_TEST(WithTransaction)
    {
        auto client = CreateTestClient();
        auto tx = client->StartTransaction();
        tx->Create(
            "//testing/table", NT_TABLE, TCreateOptions()
            .Attributes(
                TNode()
                ("schema", TNode()
                 .Add(TNode()("name", "eng")("type", "string"))
                 .Add(TNode()("name", "number")("type", "int64")))));

        const auto newSchema = TTableSchema()
                    .AddColumn(TColumnSchema().Name("eng").Type(VT_STRING))
                    .AddColumn(TColumnSchema().Name("number").Type(VT_INT64))
                    .AddColumn(TColumnSchema().Name("rus").Type(VT_STRING));

        UNIT_ASSERT_EXCEPTION(client->AlterTable("//testing/table", TAlterTableOptions().Schema(newSchema)), TErrorResponse);

        // No exception.
        tx->AlterTable("//testing/table", TAlterTableOptions().Schema(newSchema));
    }
}
