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
    const char* Output() { return "tmp/output"; }

private:
    void RemoveTables()
    {
        Client()->Remove(Input(), TRemoveOptions().Force(true));
        Client()->Remove(Output(), TRemoveOptions().Force(true));
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

} // namespace NNativeTest
} // namespace NYT

