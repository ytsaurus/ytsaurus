#include <mapreduce/yt/tests/native/sample.pb.h>
#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/yt/interface/client.h>

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/helpers.h>

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

class TIdMapperTypeProto
    : public IMapper<TTableReader<TTypeProto>, TTableWriter<TTypeProto>>
{
public:
    virtual void Do(
        TTableReader<TTypeProto>* input,
        TTableWriter<TTypeProto>* output) override
    {
        for (; input->IsValid(); input->Next()) {
            output->AddRow(input->GetRow());
        }
    }
};
REGISTER_MAPPER(TIdMapperTypeProto);

YT_TEST(TOperation, IdMapperTypeProto)
{
    auto writer = Client()->CreateTableWriter<TTypeProto>(Input());

    TTypeProto row;
    row.SetDouble(0.25);
    row.SetFloat(4.0);
    row.SetInt64(345ll);
    row.SetUInt64(27346ull);
    row.SetInt32(39485734);
    row.SetFixed64(8324765ull);
    row.SetFixed32(298734u);
    row.SetBool(true);
    row.SetString("abcdefgh");

    auto* message = row.MutableMessage();
    message->SetFoo(83610);
    message->SetBar("qwerty");

    row.SetBytes("xyz");
    row.SetUInt32(9428u);
    row.SetEnum(TTypeProto::BAR);
    row.SetSFixed32(20562);
    row.SetSFixed64(65587ll);
    row.SetSInt32(1572);
    row.SetSInt64(944825ll);

    writer->AddRow(row);
    writer->Finish();

    Client()->Map(
        TMapOperationSpec()
            .AddInput<TTypeProto>(Input())
            .AddOutput<TTypeProto>(Output()),
        new TIdMapperTypeProto
    );

    auto reader = Client()->CreateTableReader<TTypeProto>(Output());
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout <<
            "Double = " << row.GetDouble() << Endl <<
            "Float = " << row.GetFloat() << Endl <<
            "Int64 = " << row.GetInt64() << Endl <<
            "UInt64 = " << row.GetUInt64() << Endl <<
            "Int32 = " << row.GetInt32() << Endl <<
            "Fixed64 = " << row.GetFixed64() << Endl <<
            "Fixed32 = " << row.GetFixed32() << Endl <<
            "Bool = " << row.GetBool() << Endl <<
            "String = " << row.GetString() << Endl <<
            "Message.Foo = " << row.GetMessage().GetFoo() << Endl <<
            "Message.Bar = " << row.GetMessage().GetBar() << Endl <<
            "Bytes = " << row.GetBytes() << Endl <<
            "UInt32 = " << row.GetUInt32() << Endl <<
            "Enum = " << static_cast<int>(row.GetEnum()) << Endl <<
            "SFixed32 = " << row.GetSFixed32() << Endl <<
            "SFixed64 = " << row.GetSFixed64() << Endl <<
            "SInt32 = " << row.GetSInt32() << Endl <<
            "SInt64 = " << row.GetSInt64() << Endl;
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

class TMapperWithFile
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TMapperWithFile(const Stroka& fileName = Stroka())
        : FileName_(fileName)
    { }

    Y_SAVELOAD_JOB(FileName_);

    virtual void Do(
        TTableReader<TNode>* input,
        TTableWriter<TNode>* output) override
    {
        for (; input->IsValid(); input->Next()) {
            TFileInput file(FileName_);
            output->AddRow(TNode()("b", file.ReadAll()));
        }
    }

private:
    Stroka FileName_;
};
REGISTER_MAPPER(TMapperWithFile);

class TMapperWithSecureVault
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    virtual void Do(
        TTableReader<TNode>* input,
        TTableWriter<TNode>* output) override
    {
        for (; input->IsValid(); input->Next()) {
            output->AddRow(TNode()
                ("b", NYT::NodeToYsonString(SecureVault())));
        }
    }

private:
    Stroka FileName_;
};
REGISTER_MAPPER(TMapperWithSecureVault);

class TOperationWith
    : public TOperation
{
protected:
    void WriteInput()
    {
        auto writer = Client()->CreateTableWriter<TNode>(Input());
        writer->AddRow(TNode()("a", 1));
        writer->Finish();
    }

    void ReadOutput()
    {
        auto reader = Client()->CreateTableReader<TNode>(Output());
        for (; reader->IsValid(); reader->Next()) {
            Cout << "b = " << reader->GetRow()["b"].AsString() << Endl;
        }
    }
};

YT_TEST(TOperationWith, CypressTable)
{
    WriteInput();
    {
        auto writer = Client()->CreateTableWriter<TNode>(Input2());
        writer->AddRow(TNode()("key", "2")("value", "3"));
        writer->AddRow(TNode()("key", "4")("value", "5"));
        writer->AddRow(TNode()("key", "6")("value", "7"));
        writer->Finish();
    }

    Stroka sandboxName("table_in_sandbox");
    TNode format("yson");
    format.Attributes()("format", "text");
    Client()->Map(
        TMapOperationSpec()
            .AddInput<TNode>(Input())
            .AddOutput<TNode>(Output())
            .MapperSpec(TUserJobSpec()
                .AddFile(TRichYPath(TConfig::Get()->Prefix + Input2())
                    .Format(format)
                    .FileName(sandboxName)
                    .AddRange(TReadRange::FromRowIndexes(1,2)))),
        new TMapperWithFile(sandboxName)
    );

    ReadOutput();
}

YT_TEST(TOperationWith, CypressFile)
{
    WriteInput();
    {
        auto writer = Client()->CreateFileWriter(Input2());
        *writer << "file content" << Endl;
        writer->Finish();
    }

    Stroka sandboxName("file_in_sandbox");
    Client()->Map(
        TMapOperationSpec()
            .AddInput<TNode>(Input())
            .AddOutput<TNode>(Output())
            .MapperSpec(TUserJobSpec()
                .AddFile(TRichYPath(TConfig::Get()->Prefix + Input2())
                    .FileName(sandboxName))),
        new TMapperWithFile(sandboxName)
    );

    ReadOutput();
}

YT_TEST(TOperationWith, LocalFile)
{
    WriteInput();
    Stroka localName("local_file");
    {
        TFileOutput stream(localName);
        stream << "file content" << Endl;
    }

    Client()->Map(
        TMapOperationSpec()
            .AddInput<TNode>(Input())
            .AddOutput<TNode>(Output())
            .MapperSpec(TUserJobSpec().AddLocalFile(localName)),
        new TMapperWithFile(localName)
    );

    ReadOutput();
}

YT_TEST(TOperationWith, SecureVault)
{
    WriteInput();
    auto vault = TNode()
        ("var1", "val1")
        ("var2", TNode()("foo", "bar"));

    Client()->Map(
        TMapOperationSpec()
            .AddInput<TNode>(Input())
            .AddOutput<TNode>(Output()),
        new TMapperWithSecureVault,
        TOperationOptions().SecureVault(vault)
    );

    ReadOutput();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNativeTest
} // namespace NYT

