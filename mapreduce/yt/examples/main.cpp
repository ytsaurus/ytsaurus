#include <mapreduce/yt/examples/example.pb.h>

#include <mapreduce/yt/interface/client.h>

#include <util/random/random.h>

using namespace NYT;

namespace NExample {

////////////////////////////////////////////////////////////////////////////////

const Stroka SERVER("barney.yt.yandex.net");
const Stroka PREFIX("//tmp/cpp/cpp_examples");

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

////////////////////////////////////////////////////////////////////////////////

// identity mapper
// TNode

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

void IdMapperNode()
{
    auto client = CreateClient(SERVER);

    Stroka input(PREFIX + "/input");
    client->Remove(input, TRemoveOptions().Force(true));
    client->Create(input, NT_TABLE);

    Stroka output(PREFIX + "/output");
    client->Remove(output, TRemoveOptions().Force(true));
    client->Create(output, NT_TABLE);

    auto writer = client->CreateTableWriter<TNode>(input);
    for (int i = 0; i < 16; ++i) {
        TNode row;
        row["a"] = i;
        row["b"] = i * 3.14;
        row["c"] = "foo";
        writer->AddRow(row);
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

void IdMapperYaMR()
{
    auto client = CreateClient(SERVER);

    Stroka input(PREFIX + "/input");
    client->Remove(input, TRemoveOptions().Force(true));
    client->Create(input, NT_TABLE);

    Stroka output(PREFIX + "/output");
    client->Remove(output, TRemoveOptions().Force(true));
    client->Create(output, NT_TABLE);

    auto writer = client->CreateTableWriter<TYaMRRow>(input);
    for (int i = 0; i < 16; ++i) {
        TYaMRRow row;
        row.Key = Sprintf("%d", i);
        row.SubKey = Sprintf("%lf", i * 3.14);
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

void IdMapperProto()
{
    auto client = CreateClient(SERVER);

    Stroka input(PREFIX + "/input");
    client->Remove(input, TRemoveOptions().Force(true));
    client->Create(input, NT_TABLE);

    Stroka output(PREFIX + "/output");
    client->Remove(output, TRemoveOptions().Force(true));
    client->Create(output, NT_TABLE);

    auto writer = client->CreateTableWriter<TSampleProto>(input);
    for (int i = 0; i < 16; ++i) {
        TSampleProto row;
        row.set_a(i);
        row.set_b(i * 3.14);
        row.set_c("foo");
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
        Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TManyTablesMapperNode
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    virtual void Do(
        TTableReader<TNode>* input,
        TTableWriter<TNode>* output) override
    {
        for (; input->IsValid(); input->Next()) {
            const auto& inputRow = input->GetRow();
            TNode outputRow;
            outputRow["input_index_1"] = static_cast<int>(input->GetTableIndex());
            outputRow["input_index_2"] = inputRow["input_index"];
            outputRow["value"] = inputRow["value"];

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

    yvector<Stroka> inputs;
    for (int i = 0; i < inputCount; ++i) {
        Stroka input = PREFIX + Sprintf("/input%d", i);
        inputs.push_back(input);
        client->Remove(input, TRemoveOptions().Force(true));
        client->Create(input, NT_TABLE);

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

    yvector<Stroka> outputs;
    for (int i = 0; i < outputCount; ++i) {
        Stroka output = PREFIX + Sprintf("/output%d", i);
        outputs.push_back(output);
        client->Remove(output, TRemoveOptions().Force(true));
        client->Create(output, NT_TABLE);

        spec.AddOutput<TNode>(output);
    }

    client->Map(spec, new TManyTablesMapperNode);

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
    virtual void Do(
        TTableReader<Message>* input,
        TTableWriter<Message>* output) override
    {
        Cerr << "begin key" << Endl;

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

        Cerr << "end key" << Endl;
    }
};
REGISTER_REDUCER(TJoinReducerProto)

void JoinReducerProto()
{
    auto client = CreateClient(SERVER);

    Stroka inputLeft = PREFIX + "/input_left";
    client->Remove(inputLeft, TRemoveOptions().Force(true));
    client->Create(inputLeft, NT_TABLE);

    Stroka inputRight = PREFIX + "/input_right";
    client->Remove(inputRight, TRemoveOptions().Force(true));
    client->Create(inputRight, NT_TABLE);

    Stroka outputSum(PREFIX + "/output_sum");
    client->Remove(outputSum, TRemoveOptions().Force(true));
    client->Create(outputSum, NT_TABLE);

    Stroka outputProduct(PREFIX + "/output_product");
    client->Remove(outputProduct, TRemoveOptions().Force(true));
    client->Create(outputProduct, NT_TABLE);

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
        auto writer = client->CreateTableWriter<TJoinInputLeft>(path);
        for (int i = 0; i < 16; ++i) {
            TJoinInputLeft row;
            row.set_key(i);
            row.set_left(i * 2.71);
            writer->AddRow(row);
        }
        writer->Finish();
    }

    client->Reduce(
        TReduceOperationSpec()
            .AddInput<TJoinInputLeft>(inputLeft)
            .AddInput<TJoinInputRight>(inputRight)
            .AddOutput<TJoinOutputSum>(outputSum)
            .AddOutput<TJoinOutputProduct>(outputProduct),
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
    Stroka table("");
    // table must exist and be mounted
    // schema { a = int64; b = int64; }

    { // insert
        TNode::TList rows;
        for (i64 i = 0; i < 10; ++i) {
            rows.push_back(TNode()("a", i)("b", 2 * i));
        }
        client->InsertRows(table, rows);
    }
    { // delete
        TNode::TList keys;
        for (i64 i = 0; i < 3; ++i) {
            keys.push_back(TNode()("a", i));
        }
        client->DeleteRows(table, keys);
    }
    { // lookup
        TNode::TList keys;
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
        Stroka query = Sprintf("* from [%s] where b > 7", ~table);
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

} // namespace NExample

using namespace NExample;

int main(int argc, const char* argv[])
{
    Initialize(argc, argv);

//    CreateSandbox();
//    IdMapperNode();
//    IdMapperYaMR();
//    IdMapperProto();
//    ManyTablesMapperNode();
//    JoinReducerProto();

//    Tablets();

    return 0;
}

