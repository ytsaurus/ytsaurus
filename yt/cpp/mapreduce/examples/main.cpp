#include <yt/cpp/mapreduce/examples/example.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <util/random/random.h>
#include <util/string/printf.h>

using namespace NYT;

namespace NExample {

////////////////////////////////////////////////////////////////////////////////

const TString SERVER("hahn");
const TString PREFIX("//tmp/cpp/cpp_examples");

void CreateSandbox()
{
    auto client = CreateClient(SERVER);
    client->Create(
        PREFIX,
        NT_MAP,
        TCreateOptions()
            .Recursive(true)
            .IgnoreExisting(true)
    );
}

void RemoveTable(IClientPtr client, const TYPath& path)
{
    client->Remove(path, TRemoveOptions().Force(true));
}

////////////////////////////////////////////////////////////////////////////////

// identity mapper
// TNode

class TIdMapperNode
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(
        TTableReader<TNode>* input,
        TTableWriter<TNode>* output) override
    {
        for (; input->IsValid(); input->Next()) {
            output->AddRow(input->GetRow());
        }
    }
};
REGISTER_MAPPER(TIdMapperNode);

void IdMapperNode()
{
    auto client = CreateClient(SERVER);

    TString input(PREFIX + "/input");
    RemoveTable(client, input);

    TString output(PREFIX + "/output");
    RemoveTable(client, output);

    auto writer = client->CreateTableWriter<TNode>(input);
    for (int i = 0; i < 16; ++i) {
        writer->AddRow(TNode()
            ("a", i)
            ("b", i * 3.14)
            ("c", "foo")
        );
    }
    writer->Finish();

    client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(input)
            .AddOutput<TNode>(output),
        new TIdMapperNode
    );

    auto reader = client->CreateTableReader<TNode>(output);
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout <<
            "a = " << row["a"].AsInt64() <<
            ", b = " << row["b"].AsDouble() <<
            ", c = " << row["c"].AsString() <<
        Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

// identity mapper
// TYaMRRow

class TIdMapperYaMR
    : public IMapper<TTableReader<TYaMRRow>, TTableWriter<TYaMRRow>>
{
public:
    void Do(
        TTableReader<TYaMRRow>* input,
        TTableWriter<TYaMRRow>* output) override
    {
        for (; input->IsValid(); input->Next()) {
            output->AddRow(input->GetRow());
        }
    }
};
REGISTER_MAPPER(TIdMapperYaMR);

void IdMapperYaMR()
{
    auto client = CreateClient(SERVER);

    TString input(PREFIX + "/input");
    RemoveTable(client, input);

    TString output(PREFIX + "/output");
    RemoveTable(client, output);

    auto writer = client->CreateTableWriter<TYaMRRow>(input);
    for (int i = 0; i < 16; ++i) {
        auto key = Sprintf("%d", i);
        auto subKey = Sprintf("%lf", i * 3.14);

        TYaMRRow row;
        row.Key = key;
        row.SubKey = subKey;
        row.Value = "foo";
        writer->AddRow(row);
    }
    writer->Finish();

    client->Map(
        TMapOperationSpec()
            .AddInput<TYaMRRow>(input)
            .AddOutput<TYaMRRow>(output),
        new TIdMapperYaMR
    );

    auto reader = client->CreateTableReader<TYaMRRow>(output);
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

// identity mapper
// TSampleProto

class TIdMapperProto
    : public IMapper<TTableReader<TSampleProto>, TTableWriter<TSampleProto>>
{
public:
    void Do(
        TTableReader<TSampleProto>* input,
        TTableWriter<TSampleProto>* output) override
    {
        for (; input->IsValid(); input->Next()) {
            output->AddRow(input->GetRow());
        }
    }
};
REGISTER_MAPPER(TIdMapperProto);

void IdMapperProto()
{
    auto client = CreateClient(SERVER);

    TString input(PREFIX + "/input");
    RemoveTable(client, input);

    TString output(PREFIX + "/output");
    RemoveTable(client, output);

    auto writer = client->CreateTableWriter<TSampleProto>(input);
    for (int i = 0; i < 16; ++i) {
        TSampleProto row;
        row.set_a(i);
        row.set_b(i * 3.14);
        row.set_c("foo");
        row.set_d(i * 3);
        row.set_e(true);
        row.set_f(i * 5);
        row.set_g(i * 7);
        row.set_h(i * 2.71);
        row.set_i("bar");
        writer->AddRow(row);
    }
    writer->Finish();

    client->Map(
        TMapOperationSpec()
            .AddInput<TSampleProto>(input)
            .AddOutput<TSampleProto>(output),
        new TIdMapperProto
    );

    auto reader = client->CreateTableReader<TSampleProto>(output);
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout <<
            "a = " << row.a() <<
            ", b = " << row.b() <<
            ", c = " << row.c() <<
            ", d = " << row.d() <<
            ", e = " << row.e() <<
            ", f = " << row.f() <<
            ", g = " << row.g() <<
            ", h = " << row.h() <<
            ", i = " << row.i() <<
        Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TManyTablesMapperNode
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
    i64 StateValue_;

public:
    TManyTablesMapperNode()
    { }

    TManyTablesMapperNode(i64 stateValue)
        : StateValue_(stateValue)
    { }

    void Save(IOutputStream& stream) const override
    {
        stream << StateValue_;
    }

    void Load(IInputStream& stream) override
    {
        stream >> StateValue_;
    }

    void Do(
        TTableReader<TNode>* input,
        TTableWriter<TNode>* output) override
    {
        for (; input->IsValid(); input->Next()) {
            const auto& inputRow = input->GetRow();
            TNode outputRow;
            outputRow["input_index_1"] = static_cast<int>(input->GetTableIndex());
            outputRow["input_index_2"] = inputRow["input_index"];
            outputRow["value"] = inputRow["value"];
            outputRow["stateValue"] = StateValue_;

            output->AddRow(outputRow, inputRow["output_index"].AsInt64());
        }
    }
};
REGISTER_MAPPER(TManyTablesMapperNode);

void ManyTablesMapperNode()
{
    const int inputCount = 8;
    const int outputCount = 8;

    auto client = CreateClient(SERVER);

    TMapOperationSpec spec;

    TVector<TString> inputs;
    for (int i = 0; i < inputCount; ++i) {
        TString input = PREFIX + Sprintf("/input%d", i);
        inputs.push_back(input);
        RemoveTable(client, input);

        auto writer = client->CreateTableWriter<TNode>(input);
        for (int j = 0; j < 16; ++j) {
            TNode row;
            row["output_index"] = static_cast<int>(RandomNumber<ui32>(outputCount));
            row["input_index"] = i;
            row["value"] = static_cast<int>(RandomNumber<ui32>());
            writer->AddRow(row);
        }
        writer->Finish();

        spec.AddInput<TNode>(input);
   }

    TVector<TString> outputs;
    for (int i = 0; i < outputCount; ++i) {
        TString output = PREFIX + Sprintf("/output%d", i);
        outputs.push_back(output);
        RemoveTable(client, output);

        spec.AddOutput<TNode>(output);
    }

    client->Map(spec, new TManyTablesMapperNode(12345));

    for (int o = 0; o < outputCount; ++o) {
        Cout << outputs[o] << Endl;
        auto reader = client->CreateTableReader<TNode>(outputs[o]);
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            for (const auto& it : row.AsMap()) {
                Cout << it.first << " = " << it.second.AsInt64() << ", ";
            }
            Cout << Endl;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TJoinReducerProto
    : public IReducer<TTableReader<Message>, TTableWriter<Message>>
{
public:
    void Do(
        TTableReader<Message>* input,
        TTableWriter<Message>* output) override
    {
        // record with table index 0 is always the first
        i64 key = input->GetRow<TJoinInputLeft>().key();

        double left = 0, right = 0;
        for (; input->IsValid(); input->Next()) {
            switch (input->GetTableIndex()) {
                case 0:
                    left = input->GetRow<TJoinInputLeft>().left();
                    break;
                case 1:
                    right = input->GetRow<TJoinInputRight>().right();
                    break;
            }
        }

        TJoinOutputSum sum;
        sum.set_key(key);
        sum.set_sum(left + right);
        output->AddRow<TJoinOutputSum>(sum, 0);

        TJoinOutputProduct product;
        product.set_key(key);
        product.set_product(left * right);
        output->AddRow<TJoinOutputProduct>(product, 1);
    }
};
REGISTER_REDUCER(TJoinReducerProto)

void JoinReducerProto()
{
    auto client = CreateClient(SERVER);

    TString inputLeft = PREFIX + "/input_left";
    RemoveTable(client, inputLeft);

    TString inputRight = PREFIX + "/input_right";
    RemoveTable(client, inputRight);

    TString outputSum(PREFIX + "/output_sum");
    RemoveTable(client, outputSum);

    TString outputProduct(PREFIX + "/output_product");
    RemoveTable(client, outputProduct);

    {
        auto path = TRichYPath(inputLeft).SortedBy("key");
        auto writer = client->CreateTableWriter<TJoinInputLeft>(path);
        for (int i = 0; i < 16; ++i) {
            TJoinInputLeft row;
            row.set_key(i);
            row.set_left(i * 3.14);
            writer->AddRow(row);
        }
        writer->Finish();
    }
    {
        auto path = TRichYPath(inputRight).SortedBy("key");
        auto writer = client->CreateTableWriter<TJoinInputRight>(path);
        for (int i = 0; i < 16; ++i) {
            TJoinInputRight row;
            row.set_key(i);
            row.set_right(i * 2.71);
            writer->AddRow(row);
        }
        writer->Finish();
    }

    client->JoinReduce(
        TJoinReduceOperationSpec()
            .AddInput<TJoinInputLeft>(TRichYPath(inputLeft).Primary(true))
            .AddInput<TJoinInputRight>(inputRight)
            .AddOutput<TJoinOutputSum>(outputSum)
            .AddOutput<TJoinOutputProduct>(outputProduct)
            .JoinBy(TSortColumns("key")),
        new TJoinReducerProto);

    {
        auto reader = client->CreateTableReader<TJoinOutputSum>(outputSum);
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            Cout <<
                "key = " << row.key() <<
                ", sum = " << row.sum() <<
            Endl;
        }
    }
    {
        auto reader = client->CreateTableReader<TJoinOutputProduct>(outputProduct);
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            Cout <<
                "key = " << row.key() <<
                ", product = " << row.product() <<
            Endl;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void Tablets()
{
    auto client = CreateClient(SERVER);
    TString table(PREFIX + "/tablets");

    TNode schema;
    schema.Add(TNode()("name", "a")("type", "int64")("sort_order", "ascending"));
    schema.Add(TNode()("name", "b")("type", "int64"));

    RemoveTable(client, table);

    client->Create(
        table,
        NT_TABLE,
        TCreateOptions()
            .Recursive(true)
            .IgnoreExisting(true)
            .Attributes(TNode()
                ("dynamic", true)
                ("schema", schema))
    );

    client->MountTable(table);
    Sleep(TDuration::Seconds(15)); // TODO: wait for tablet cell status

    { // insert
        TNode::TListType rows;
        for (i64 i = 0; i < 10; ++i) {
            rows.push_back(TNode()("a", i)("b", 2 * i));
        }
        client->InsertRows(table, rows);
    }
    { // delete
        TNode::TListType keys;
        for (i64 i = 0; i < 3; ++i) {
            keys.push_back(TNode()("a", i));
        }
        client->DeleteRows(table, keys);
    }
    { // lookup
        TNode::TListType keys;
        for (i64 i = 0; i < 6; ++i) {
            keys.push_back(TNode()("a", i));
        }
        auto rows = client->LookupRows(table, keys);
        for (const auto& row : rows) {
            Cout <<
                "a = " << row["a"].AsInt64() <<
                ", b = " << row["b"].AsInt64() <<
            Endl;
        }
    }
    { // select
        TString query = Sprintf("* from [%s] where b > 7", table.data());
        auto rows = client->SelectRows(query);
        for (const auto& row : rows) {
            Cout <<
                "a = " << row["a"].AsInt64() <<
                ", b = " << row["b"].AsInt64() <<
            Endl;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ReadMultipleRangesNode()
{
    auto client = CreateClient(SERVER);

    TString input(PREFIX + "/input");
    RemoveTable(client, input);

    auto writer = client->CreateTableWriter<TNode>(
        TRichYPath(input).SortedBy("key"));
    for (int i = 0; i < 100; ++i) {
        writer->AddRow(TNode()
            ("key", i)
            ("value", "0123456789abcdef")
        );
    }
    writer->Finish();

    TRichYPath path(input);
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().RowIndex(10))
        .UpperLimit(TReadLimit().RowIndex(20)));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().Key(30))
        .UpperLimit(TReadLimit().Key(40)));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().RowIndex(50))
        .UpperLimit(TReadLimit().RowIndex(60)));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().Key(70))
        .UpperLimit(TReadLimit().Key(80)));

    auto reader = client->CreateTableReader<TNode>(path);
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout << row["key"].AsInt64() << ", row index " << reader->GetRowIndex() << Endl;
    }
}

void ReadMultipleRangesYaMR()
{
    auto client = CreateClient(SERVER);

    TString input(PREFIX + "/input");
    RemoveTable(client, input);

    auto writer = client->CreateTableWriter<TYaMRRow>(
        TRichYPath(input).SortedBy("key"));
    for (int i = 0; i < 100; ++i) {
        auto key = Sprintf("%02d", i);
        TYaMRRow row;
        row.Key = key;
        row.Value = "0123456789abcdef";
        writer->AddRow(row);
    }
    writer->Finish();

    TRichYPath path(input);
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().RowIndex(10))
        .UpperLimit(TReadLimit().RowIndex(20)));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().Key("30"))
        .UpperLimit(TReadLimit().Key("40")));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().RowIndex(50))
        .UpperLimit(TReadLimit().RowIndex(60)));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().Key("70"))
        .UpperLimit(TReadLimit().Key("80")));

    auto reader = client->CreateTableReader<TYaMRRow>(path);
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout << row.Key << ", row index " << reader->GetRowIndex() << Endl;
    }
}

} // namespace NExample

using namespace NExample;

int main(int argc, const char* argv[])
{
    Initialize(argc, argv);

    CreateSandbox();
    IdMapperNode();
    IdMapperYaMR();
    IdMapperProto();
    ManyTablesMapperNode();
    JoinReducerProto();

    ReadMultipleRangesNode();
    ReadMultipleRangesYaMR();

//    Tablets();

    return 0;
}

