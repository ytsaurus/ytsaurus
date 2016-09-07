#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/yt/interface/client.h>

#include <mapreduce/yt/common/helpers.h>

namespace NYT {
namespace NNativeTest {

////////////////////////////////////////////////////////////////////////////////

class TSchema
    : public NTest::TTest
{
public:
    void SetUp() override
    {
        TTest::SetUp();
        Client_ = CreateClient(ServerName());
        RemoveTables();
    }

    void TearDown() override
    {
        RemoveTables();
        TTest::TearDown();
    }

    IClientPtr Client() { return Client_; }
    const char* Input() { return "tmp/input"; }

    TTableSchema CreateSchema()
    {
        return TTableSchema()
            .AddColumn(TColumnSchema().Name("a").Type(VT_INT64))
            .AddColumn(TColumnSchema().Name("b").Type(VT_UINT64))
            .AddColumn(TColumnSchema().Name("c").Type(VT_DOUBLE))
            .AddColumn(TColumnSchema().Name("d").Type(VT_BOOLEAN))
            .AddColumn(TColumnSchema().Name("e").Type(VT_STRING))
            .AddColumn(TColumnSchema().Name("f").Type(VT_ANY));
    }

    void PrintSchema()
    {
        auto schema = Client()->Get(Input() + Stroka("/@schema"));
        Cout << NodeToYsonString(schema) << Endl << Endl;
    }

    void WriteTable(TTableWriterPtr<TNode> writer)
    {
        for (int i = 0; i < 8; ++i) {
            TNode row;
            row("a", i);
            row("b", static_cast<ui64>(i * 2));
            row("c", i * 9.81);
            row("d", i % 2 == 0);
            row("e", Sprintf("foo %d", i));

            if (i % 2 == 0) {
                row("f", i + 5);
            } else {
                row("f", "bar");
            };
            writer->AddRow(row);
        }
        writer->Finish();
    }

    void ReadTable(TTableReaderPtr<TNode> reader)
    {
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            Cout <<
                "a = " << row["a"].AsInt64() <<
                ", b = " << row["b"].AsUint64() <<
                ", c = " << row["c"].AsDouble() <<
                ", d = " << row["d"].AsBool() <<
                ", e = " << row["e"].AsString();

            if (row["f"].IsString()) {
                Cout << ", f = " << row["f"].AsString() << Endl;
            } else {
                Cout << ", f = " << row["f"].AsInt64() << Endl;
            }
        }
    }

private:
    void RemoveTables()
    {
        TRemoveOptions options;
        options.Force(true);

        Client()->Remove(Input(), options);
    }

    IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TSchema, AsPathAttribute)
{
    Client()->Create(Input(), NT_TABLE);

    TRichYPath path(Input());
    path.Schema(CreateSchema());

    auto writer = Client()->CreateTableWriter<TNode>(path);
    WriteTable(writer);

    PrintSchema();

    auto reader = Client()->CreateTableReader<TNode>(Input());
    ReadTable(reader);
}

YT_TEST(TSchema, AlterTable)
{
    Client()->Create(Input(), NT_TABLE);

    auto schema = CreateSchema();

    Client()->AlterTable(Input(), TAlterTableOptions().Schema(schema));

    auto writer = Client()->CreateTableWriter<TNode>(Input());
    WriteTable(writer);

    PrintSchema();

    auto reader = Client()->CreateTableReader<TNode>(Input());
    ReadTable(reader);

    schema.AddColumn(TColumnSchema().Name("g").Type(VT_INT64));

    Client()->AlterTable(Input(), TAlterTableOptions().Schema(schema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNativeTest
} // namespace NYT

