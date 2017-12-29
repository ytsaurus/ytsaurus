#include "lib.h"

#include <mapreduce/yt/common/config.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/io.h>

#include <mapreduce/yt/http/abortable_http_response.h>

#include <library/unittest/registar.h>

#include <util/random/fast.h>

using namespace NYT;
using namespace NYT::NTesting;

SIMPLE_UNIT_TEST_SUITE(Schema) {
    SIMPLE_UNIT_TEST(Required)
    {
        auto client = CreateTestClient();

        client->Create("//testing/table", NT_TABLE,
            TCreateOptions().Attributes(
                TNode()
                ("schema", TTableSchema().AddColumn(TColumnSchema().Name("value").Type(VT_STRING).Required(true)).ToNode())
            ));

        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath("//testing/table").Append(true));
            writer->AddRow(TNode()("value", "foo"));
            writer->Finish();
        }
        try {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath("//testing/table").Append(true));
            writer->AddRow(TNode()("value", TNode::CreateEntity()));
            writer->Finish();
            UNIT_FAIL("expected to throw");
        } catch (const yexception& ex) {
            if (!TString(ex.what()).Contains("cannot have \"null\" value")) {
                throw;
            }
        }
        try {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath("//testing/table").Append(true));
            writer->AddRow(TNode::CreateMap());
            writer->Finish();
            UNIT_FAIL("expected to throw");
        } catch (const yexception& ex) {
            if (!TString(ex.what()).Contains("cannot have \"null\" value")) {
                throw;
            }
        }
    }

    SIMPLE_UNIT_TEST(SchemaAsPathAttribute) {
        auto schema = TTableSchema()
            .AddColumn(TColumnSchema().Name("key").Type(VT_STRING))
            .AddColumn(TColumnSchema().Name("value").Type(VT_INT64));

        auto client = CreateTestClient();
        auto writer = client->CreateTableWriter<TNode>(TRichYPath("//testing/table").Schema(schema));

        TVector<TNode> expected = {
            TNode()("key", "one")("value", 1),
        };
        {
            for (const auto& e : expected) {
                writer->AddRow(e);
            }
            writer->Finish();
        }

        auto actualSchema = client->Get("//testing/table/@schema");
        UNIT_ASSERT_VALUES_EQUAL(actualSchema, schema.ToNode());

        TVector<TNode> actual = ReadTable(client, "//testing/table");
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }
}
