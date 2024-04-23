#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/client/skiff.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/random/fast.h>

using namespace NYT;
using namespace NYT::NTesting;

TEST(Schema, Required)
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
        FAIL() << "expected to throw";
    } catch (const std::exception& ex) {
        if (!TString(ex.what()).Contains("cannot have \"null\" value")) {
            throw;
        }
    }
    try {
        auto writer = client->CreateTableWriter<TNode>(TRichYPath(workingDir + "/table").Append(true));
        writer->AddRow(TNode::CreateMap());
        writer->Finish();
        FAIL() << "expected to throw";
    } catch (const std::exception& ex) {
        if (!TString(ex.what()).Contains("cannot have \"null\" value")) {
            throw;
        }
    }
}

TEST(Schema, SchemaAsPathAttribute) {
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
    EXPECT_TRUE(AreSchemasEqual(actualSchema, schema));

    TVector<TNode> actual = ReadTable(client, workingDir + "/table");
    EXPECT_EQ(actual, expected);
}

TEST(Schema, DeletedColumn) {
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
    EXPECT_TRUE(AreSchemasEqual(actualSchema, updatedSchema));

    // Verify that the deleted column is excluded from the skiff schema.
    auto skiffSchema = NYT::NDetail::CreateSkiffSchema(actualSchema);

    EXPECT_EQ(std::ssize(skiffSchema->GetChildren()), 2);
    EXPECT_EQ(skiffSchema->GetChildren()[0]->GetName(), "value");
}
