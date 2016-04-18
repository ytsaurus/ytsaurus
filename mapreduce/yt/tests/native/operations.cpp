#include <mapreduce/yt/tests/native/sample.pb.h>

#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/yt/interface/client.h>

namespace NYT {
namespace NNativeTest {

////////////////////////////////////////////////////////////////////////////////

class TOperation
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
    const char* Input2() { return "tmp/input2"; }
    const char* Output() { return "tmp/output"; }

private:
    void RemoveTables()
    {
        TRemoveOptions options;
        options.Force(true);

        Client()->Remove(Input(), options);
        Client()->Remove(Input2(), options);
        Client()->Remove(Output(), options);
    }

    IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

class TIdMapperNode
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    virtual void Do(
        TTableReader<TNode>* input,
        TTableWriter<TNode>* output) override
    {
        for (; input->IsValid(); input->Next()) {
            output->AddRow(input->GetRow());
        }
    }
};
REGISTER_MAPPER(TIdMapperNode);

YT_TEST(TOperation, IdMapperNode)
{
    auto writer = Client()->CreateTableWriter<TNode>(Input());
    for (int i = 0; i < 8; ++i) {
        writer->AddRow(TNode()("a", i)("b", i * 2));
    }
    writer->Finish();

    Client()->Map(
        TMapOperationSpec()
            .AddInput<TNode>(Input())
            .AddOutput<TNode>(Output()),
        new TIdMapperNode
    );

    Client()->Sort(
        TSortOperationSpec()
            .AddInput(Output())
            .Output(Output())
            .SortBy("a")
    );

    auto reader = Client()->CreateTableReader<TNode>(Output());
    for (; reader->IsValid(); reader->Next()) {
        Cout << "a = " << reader->GetRow()["a"].AsInt64() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TIdMapperYaMR
    : public IMapper<TTableReader<TYaMRRow>, TTableWriter<TYaMRRow>>
{
public:
    virtual void Do(
        TTableReader<TYaMRRow>* input,
        TTableWriter<TYaMRRow>* output) override
    {
        for (; input->IsValid(); input->Next()) {
            output->AddRow(input->GetRow());
        }
    }
};
REGISTER_MAPPER(TIdMapperYaMR);

YT_TEST(TOperation, IdMapperYaMR)
{
    auto writer = Client()->CreateTableWriter<TYaMRRow>(Input());
    for (int i = 0; i < 8; ++i) {
        auto key = Sprintf("%d", i);
        auto subkey = Sprintf("%d", i * 2);
        auto value = Sprintf("%d", i * 4);
        writer->AddRow({key, subkey, value});
    }
    writer->Finish();

    Client()->Map(
        TMapOperationSpec()
            .AddInput<TYaMRRow>(Input())
            .AddOutput<TYaMRRow>(Output()),
        new TIdMapperYaMR
    );

    Client()->Sort(
        TSortOperationSpec()
            .AddInput(Output())
            .Output(Output())
            .SortBy("key")
    );

    auto reader = Client()->CreateTableReader<TYaMRRow>(Output());
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout <<
            "key = " << row.Key <<
            ", subkey = " << row.SubKey <<
            ", value = " << row.Value <<
        Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TIdMapperProto
    : public IMapper<TTableReader<TSampleProto>, TTableWriter<TSampleProto>>
{
public:
    virtual void Do(
        TTableReader<TSampleProto>* input,
        TTableWriter<TSampleProto>* output) override
    {
        for (; input->IsValid(); input->Next()) {
            output->AddRow(input->GetRow());
        }
    }
};
REGISTER_MAPPER(TIdMapperProto);

YT_TEST(TOperation, IdMapperProto)
{
    auto writer = Client()->CreateTableWriter<TSampleProto>(Input());
    for (int i = 0; i < 8; ++i) {
        TSampleProto row;
        row.set_a(i);
        row.set_b(static_cast<unsigned int>(i));
        row.set_c(i * 9.81);
        row.set_d(i % 2 == 0);
        row.set_e(Sprintf("foo %d", i));
        writer->AddRow(row);
    }
    writer->Finish();

    Client()->Map(
        TMapOperationSpec()
            .AddInput<TSampleProto>(Input())
            .AddOutput<TSampleProto>(Output()),
        new TIdMapperProto
    );

    Client()->Sort(
        TSortOperationSpec()
            .AddInput(Output())
            .Output(Output())
            .SortBy("column_a")
    );

    auto reader = Client()->CreateTableReader<TSampleProto>(Output());
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout <<
            "a = " << row.a() <<
            ", b = " << row.b() <<
            ", c = " << row.c() <<
            ", d = " << row.d() <<
            ", e = " << row.e() <<
        Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleReducer
    : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    virtual void Do(
        TTableReader<TNode>* input,
        TTableWriter<TNode>* output) override
    {
        auto key = input->GetRow()["key"];
        TStringStream str;
        for (; input->IsValid(); input->Next()) {
            const auto& row = input->GetRow();
            str <<
                input->GetTableIndex() << " " <<
                input->GetRowIndex() << " " <<
                row["subkey"].AsInt64() << " " <<
                row["value"].AsString() << "; ";
        }
        output->AddRow(TNode()("key", key)("value", str.Str()));
    }
};
REGISTER_REDUCER(TSimpleReducer);

YT_TEST(TOperation, SimpleReduce)
{
    {
        auto writer = Client()->CreateTableWriter<TNode>(
            TRichYPath(Input()).SortedBy({"key", "subkey"}));
        writer->AddRow(TNode()("key", 0)("subkey", 0)("value", "a"));
        writer->AddRow(TNode()("key", 0)("subkey", 1)("value", "b"));
        writer->AddRow(TNode()("key", 1)("subkey", 0)("value", "c"));
        writer->AddRow(TNode()("key", 1)("subkey", 1)("value", "d"));
        writer->Finish();
    }
    {
        auto writer = Client()->CreateTableWriter<TNode>(
            TRichYPath(Input2()).SortedBy({"key", "subkey"}));
        writer->AddRow(TNode()("key", 0)("subkey", 0)("value", "w"));
        writer->AddRow(TNode()("key", 0)("subkey", 1)("value", "x"));
        writer->AddRow(TNode()("key", 1)("subkey", 0)("value", "y"));
        writer->AddRow(TNode()("key", 1)("subkey", 1)("value", "z"));
        writer->Finish();
    }

    Client()->Reduce(
        TReduceOperationSpec()
            .AddInput<TNode>(Input())
            .AddInput<TNode>(Input2())
            .AddOutput<TNode>(TRichYPath(Output()).SortedBy({"key", "subkey"}))
            .ReduceBy("key")
            .SortBy({"key", "subkey"}),
        new TSimpleReducer
    );

    auto reader = Client()->CreateTableReader<TNode>(Output());
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout << row["key"].AsInt64() << " - " << row["value"].AsString() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NNativeTest
} // namespace NYT

