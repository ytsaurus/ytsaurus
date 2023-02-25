#include <yt/cpp/mapreduce/tests/performance/protobuf/row.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <util/random/fast.h>
#include <util/system/env.h>

#include <library/cpp/getopt/last_getopt.h>

using namespace NYT;
using namespace NYT::NDetail;
using namespace NTableClientBenchmark;

class TConsumingMapper
    : public IMapper<TTableReader<TIntermediateSemidupsDataProto>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        ui32 maxTableIndex = 0;
        for (const auto& cursor : *reader) {
            maxTableIndex = Max(maxTableIndex, cursor.GetRow().GetTableIndex());
        }
        writer->AddRow(TNode()("max", maxTableIndex));
    }
};
REGISTER_MAPPER(TConsumingMapper)

class TGenericReducer
    : public IReducer<TTableReader<Message>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* /* writer */) override
    {
        for (const auto& cursor : *reader) {
            if (cursor.GetTableIndex() % 2 == 0) {
                Max_ = Max(Max_, cursor.GetRow<TUrlDataRowProto>().GetLastAccess());
            } else {
                Max_ = Max(Max_, cursor.GetRow<TUrlDataRowProto1>().GetLastAccess());
            }
        }
    }

    void Finish(TWriter* writer) override
    {
        writer->AddRow(TNode()("max", Max_));
    }

private:
    ui64 Max_ = 0;
};
REGISTER_REDUCER(TGenericReducer)

class TProtoOneOfReducer
    : public IReducer<TTableReader<TProtoOneOf<TUrlDataRowProto, TUrlDataRowProto1>>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* /* writer */) override
    {
        for (const auto& cursor : *reader) {
            if (cursor.GetTableIndex() % 2 == 0) {
                Max_ = Max(Max_, cursor.GetRow<TUrlDataRowProto>().GetLastAccess());
            } else {
                Max_ = Max(Max_, cursor.GetRow<TUrlDataRowProto1>().GetLastAccess());
            }
        }
    }

    void Finish(TWriter* writer) override
    {
        writer->AddRow(TNode()("max", Max_));
    }

private:
    ui64 Max_ = 0;
};
REGISTER_REDUCER(TProtoOneOfReducer)

void RunOperation(
    const IClientBasePtr& client,
    TStringBuf type,
    const TVector<TRichYPath>& inputs,
    const TVector<TRichYPath>& outputs)
{
    if (type == "consuming-mapper") {
        Y_ENSURE(inputs.size() == 1);
        Y_ENSURE(outputs.size() == 1);
        client->Map(
            TMapOperationSpec()
                .AddInput<TIntermediateSemidupsDataProto>(inputs[0])
                .AddOutput<TNode>(outputs[0]),
            new TConsumingMapper);
    } else if (type == "generic-reduce" || type == "oneof-reduce") {
        auto spec = TReduceOperationSpec()
            .AddOutput<TNode>(outputs[0])
            .ReduceBy("access_time");
        for (int i = 0; i < ssize(inputs); ++i) {
            if (i % 2 == 0) {
                spec.AddInput<TUrlDataRowProto>(inputs[i]);
            } else {
                spec.AddInput<TUrlDataRowProto1>(inputs[i]);
            }
        }
        ::TIntrusivePtr<IReducerBase> reducer;
        if (type == "generic-reduce") {
            reducer = new TGenericReducer;
        } else {
            reducer = new TProtoOneOfReducer;
        }
        Y_ENSURE(outputs.size() == 1);
        client->Reduce(
            spec,
            reducer);
    } else {
        Y_FAIL();
    }
}

template <typename T>
void GenerateRow(T* row);

TString RandBytes()
{
    static TReallyFastRng32 RNG(42);
    ui64 value = RNG.GenRand64();
    return TString(reinterpret_cast<const char*>(&value), sizeof(value));
}

ui64 RandUint64()
{
    static TReallyFastRng32 RNG(42);
    return RNG.GenRand64();
}

template <typename T>
void GenerateUrlDataRowProto(T* row)
{
    row->SetHost(RandBytes());
    row->SetPath(RandBytes());
    row->SetRegion(RandBytes());
    row->SetLastAccess(RandUint64());
    row->SetExportTime(RandUint64());
    row->SetTextCRC(RandUint64());
    row->SetHttpModTime(RandUint64());
}

template <>
void GenerateRow<TUrlDataRowProto>(TUrlDataRowProto* row)
{
    GenerateUrlDataRowProto(row);
}

template <>
void GenerateRow<TUrlDataRowProto1>(TUrlDataRowProto1* row)
{
    GenerateUrlDataRowProto(row);
}

template <typename T>
void GenerateData(const IClientBasePtr& client, const TRichYPath& table, i64 rowCount)
{
    auto writer = client->CreateTableWriter<T>(table);
    T row;
    for (i64 i = 0; i < rowCount; ++i) {
        GenerateRow(&row);
        writer->AddRow(row);
    }
    writer->Finish();
}

int main(int argc, char** argv)
{
    Initialize(argc, argv);

    TVector<TString> inputs, outputs;
    TString command;
    TString operationType;
    i64 rowCount;

    auto opts = NLastGetopt::TOpts::Default();

    opts.SetTitle("Tool to benchmark protobuf format");

    opts.AddLongOption("command", "What to do").StoreResult(&command).Required();
    opts.AddLongOption("input", "Input table path").AppendTo(&inputs);
    opts.AddLongOption("output", "Output table path").AppendTo(&outputs);
    opts.AddLongOption("operation-type", "Type of operation to run").StoreResult(&operationType);
    opts.AddLongOption("row-count", "Number of rows to generate").StoreResult(&rowCount).DefaultValue(100000);

    NLastGetopt::TOptsParseResult args(&opts, argc, argv);

    auto proxy = GetEnv("YT_PROXY");
    Y_ENSURE(proxy, "Specify proxy in YT_PROXY");

    auto client = CreateClient(proxy);

    if (command == "generate") {
        Y_ENSURE(outputs.size() == 1);
        GenerateData<TUrlDataRowProto>(client, TRichYPath(outputs[0]), rowCount);
    } else if (command == "run-operation") {
        Y_ENSURE(operationType);
        TVector<TRichYPath> richInputs(begin(inputs), end(inputs));
        TVector<TRichYPath> richOutputs(begin(outputs), end(outputs));
        RunOperation(client, operationType, richInputs, richOutputs);
    } else {
        Y_FAIL();
    }

    return 0;
}
