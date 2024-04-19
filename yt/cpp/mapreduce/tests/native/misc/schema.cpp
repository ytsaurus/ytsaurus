#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/client/skiff.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>

using namespace NYT;
using namespace NYT::NTesting;

Y_UNIT_TEST_SUITE(Schema) {
    Y_UNIT_TEST(Required)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        client->Create(workingDir + "/table", NT_TABLE,
            TCreateOptions().Attributes(
                TNode()
                ("schema", TTableSchema().AddColumn(TColumnSchema().Name("value").Type(VT_STRING, true)).ToNode())
            ));

        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(workingDir + "/table").Append(true));
            writer->AddRow(TNode()("value", "foo"));
            writer->Finish();
        }
        try {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(workingDir + "/table").Append(true));
            writer->AddRow(TNode()("value", TNode::CreateEntity()));
            writer->Finish();
            UNIT_FAIL("expected to throw");
        } catch (const std::exception& ex) {
            if (!TString(ex.what()).Contains("cannot have \"null\" value")) {
                throw;
            }
        }
        try {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(workingDir + "/table").Append(true));
            writer->AddRow(TNode::CreateMap());
            writer->Finish();
            UNIT_FAIL("expected to throw");
        } catch (const std::exception& ex) {
            if (!TString(ex.what()).Contains("cannot have \"null\" value")) {
                throw;
            }
        }
    }

    Y_UNIT_TEST(SchemaAsPathAttribute) {
        auto schema = TTableSchema()
            .AddColumn(TColumnSchema().Name("key").Type(VT_STRING))
            .AddColumn(TColumnSchema().Name("value").Type(VT_INT64));

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto writer = client->CreateTableWriter<TNode>(TRichYPath(workingDir + "/table").Schema(schema));

        TVector<TNode> expected = {
            TNode()("key", "one")("value", 1),
        };
        {
            for (const auto& e : expected) {
                writer->AddRow(e);
            }
            writer->Finish();
        }

        TTableSchema actualSchema;
        Deserialize(actualSchema, client->Get(workingDir + "/table/@schema"));
        UNIT_ASSERT(AreSchemasEqual(actualSchema, schema));

        TVector<TNode> actual = ReadTable(client, workingDir + "/table");
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(DeletedColumn) {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        client->Create(workingDir + "/table", NT_TABLE,
            TCreateOptions().Attributes(
                TNode()
                ("schema", TTableSchema()
                    .AddColumn(TColumnSchema().Name("value").Type(VT_STRING, true))
                    .AddColumn(TColumnSchema().Name("data").Type(VT_STRING, true)).ToNode())
            ));

        auto updatedSchema = TTableSchema()
                    .AddColumn(TColumnSchema().Name("value").Type(VT_STRING, true))
                    .AddColumn(TColumnSchema().StableName("data").Deleted(true));

        TAlterTableOptions alterOptions;
        alterOptions.Schema(updatedSchema);

        client->AlterTable(workingDir + "/table", alterOptions);

        TTableSchema actualSchema;
        Deserialize(actualSchema, client->Get(workingDir + "/table/@schema"));
        UNIT_ASSERT(AreSchemasEqual(actualSchema, updatedSchema));

        // Verify that the deleted column is excluded from the skiff schema.
        auto skiffSchema = NYT::NDetail::CreateSkiffSchema(actualSchema);

        UNIT_ASSERT_VALUES_EQUAL(std::ssize(skiffSchema->GetChildren()), 2);
        UNIT_ASSERT_VALUES_EQUAL(skiffSchema->GetChildren()[0]->GetName(), "value");
    }
}
