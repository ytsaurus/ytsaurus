
#include <library/cpp/yson/node/node.h>
#include <library/cpp/yt/logging/logger.h>
#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/systest/operation.h>
#include <yt/systest/proto/run_spec.pb.h>
#include <yt/systest/table.h>

#include <yt/systest/run.h>

#include <stdio.h>

namespace NYT::NTest {

static std::unordered_map<TString, int> produceColumnPositions(const TTable& table, TRange<int> inputColumns)
{
    std::unordered_map<TString, int> columnPositions;
    const int numInputColumns = std::ssize(inputColumns);
    for (int i = 0; i < numInputColumns; ++i) {
        int position = inputColumns[i];
        columnPositions[table.DataColumns[position].Name] = i;
    }
    return columnPositions;
}

static std::vector<TNode> produceInput(
    const std::unordered_map<TString, int>& columnPositions,
    const TNode::TMapType& row,
    int numInputs)
{
    std::vector<TNode> result;
    result.resize(numInputs);

    for (const auto& entry : row) {
        auto inputCol = columnPositions.find(entry.first);
        if (inputCol == columnPositions.end()) {
            continue;
        }

        result[inputCol->second] = entry.second;
    }

    return result;
}

void populateOutput(TRange<TDataColumn> outputColumns, TRange<TNode> row, TNode::TMapType* outputMap)
{
    for (int i = 0; i < std::ssize(outputColumns); i++) {
        (*outputMap)[outputColumns[i].Name] = row[i];
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTestOperationMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    Y_SAVELOAD_JOB(SerializedOperation_);

    TTestOperationMapper();
    TTestOperationMapper(TString SerializedOperation);

    virtual void Start(TWriter* writer) override;
    void Do(TTableReader<TNode>* input, TTableWriter<TNode>* output) override;

private:
    TString SerializedOperation_;
    TTable Table_;
    std::unique_ptr<IMultiMapper> Operation_;
};
REGISTER_MAPPER(TTestOperationMapper);

////////////////////////////////////////////////////////////////////////////////

class TTestOperationReducer
    : public NYT::IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    Y_SAVELOAD_JOB(SerializedOperation_);

    TTestOperationReducer();
    TTestOperationReducer(TString SerializedOperation);

    virtual void Start(TWriter* writer) override;
    virtual void Do(TTableReader<TNode>* reader, TTableWriter<TNode>* writer) override;

private:
    TString SerializedOperation_;
    TTable Table_;
    std::unique_ptr<NTest::IReducer> Operation_;
    std::vector<TString> Prefix_;
    std::unordered_map<TString, int> PrefixPositions_;
};
REGISTER_REDUCER(TTestOperationReducer);

////////////////////////////////////////////////////////////////////////////////

TTestOperationReducer::TTestOperationReducer()
{
}

TTestOperationReducer::TTestOperationReducer(TString SerializedOperation)
    : SerializedOperation_(std::move(SerializedOperation))
{
}

void TTestOperationReducer::Start(TWriter* /*writer*/)
{
    NProto::TReduceRunSpec spec;
    spec.ParseFromStringOrThrow(SerializedOperation_);
    FromProto(&Table_, spec.table());
    Operation_ = CreateFromProto(Table_, spec.operation());
    for (int i = 0; i < spec.reduce_by_size(); ++i) {
        const auto& reduceBy = spec.reduce_by(i);
        Prefix_.push_back(reduceBy);
        PrefixPositions_[reduceBy] = i;
    }
}

void TTestOperationReducer::Do(TTableReader<TNode>* input, TTableWriter<TNode>* output)
{
    // It is guaranteed at all records passed into Do() have one reduce key.
    // https://yt.yandex-team.ru/docs/api/cpp/description

    if (!input->IsValid()) {
        return;
    }

    const int numInputColumns = std::ssize(Operation_->InputColumns());
    auto columnPositions = produceColumnPositions(
        Operation_->InputTable(),
        Operation_->InputColumns());

    std::vector<TNode> prefix;
    const auto firstRow = input->GetRow();
    const auto& rowMap = firstRow.AsMap();
    if (!Prefix_.empty() && prefix.empty()) {
        prefix = produceInput(PrefixPositions_, rowMap, std::ssize(Prefix_));
    }

    TCallState callState;
    Operation_->StartRange(&callState, prefix);

    for (; input->IsValid(); input->Next()) {
        const auto row = input->GetRow();
        const auto& rowMap = row.AsMap();
        Operation_->ProcessRow(&callState, produceInput(columnPositions, rowMap, numInputColumns));
    }

    auto outputRows = Operation_->FinishRange(&callState);
    const auto& outputColumns = Operation_->OutputColumns();

    for (const auto& row : outputRows) {
        auto outputNode = TNode::CreateMap();
        auto& nodeMap = outputNode.AsMap();
        for (int i = 0; i < std::ssize(prefix); ++i) {
            nodeMap[Prefix_[i]] = prefix[i];
        }
        populateOutput(outputColumns, TRange<TNode>(row), &nodeMap);
        output->AddRow(outputNode);
    }
}

////////////////////////////////////////////////////////////////////////////////

TTestOperationMapper::TTestOperationMapper()
{
}

TTestOperationMapper::TTestOperationMapper(TString SerializedOperation)
    : SerializedOperation_(std::move(SerializedOperation))
{
}

void TTestOperationMapper::Start(TWriter* /*writer*/)
{
    NProto::TMapRunSpec spec;
    spec.ParseFromStringOrThrow(SerializedOperation_);

    FromProto(&Table_, spec.table());
    Operation_ = CreateFromProto(Table_, spec.operation());
}

void TTestOperationMapper::Do(TTableReader<TNode>* input, TTableWriter<TNode>* output)
{
    const int numInputColumns = std::ssize(Operation_->InputColumns());
    auto columnPositions = produceColumnPositions(Operation_->InputTable(), Operation_->InputColumns());
    for (; input->IsValid(); input->Next()) {
        TCallState rowMapState;
        const auto row = input->GetRow();

        const auto input = produceInput(columnPositions, row.AsMap(), numInputColumns);

        auto rows = Operation_->Run(&rowMapState, input);
        const auto& outputColumns = Operation_->OutputColumns();

        for (const auto& row : rows) {
            auto outputNode = TNode::CreateMap();
            populateOutput(outputColumns, TRange<TNode>(row), &outputNode.AsMap());
            output->AddRow(outputNode);
        }
    }
}

template <typename T>
static void SetBaseOperationOptions(TOperationSpecBase<T>& base)
{
    base.MaxFailedJobCount(10000);
}

////////////////////////////////////////////////////////////////////////////////

void RunMap(IClientPtr client, const TString& pool,
            const TString& inputPath, const TString& outputPath,
            const TTable& table, const TTable& outputTable, const IMultiMapper& operation)
{
    const auto attributePath = BuildAttributes(outputTable) + outputPath;
    NYT::NLogging::TLogger Logger("test");
    YT_LOG_INFO("Map (OutputTable: %v)", attributePath);
    TMapOperationSpec spec;
    SetBaseOperationOptions(spec);
    spec.Pool(pool);
    spec.AddInput<TNode>(inputPath);
    spec.AddOutput<TNode>(attributePath);
    spec.Ordered(true);
    spec.JobCount(10);

    NProto::TMapRunSpec runSpec;
    ToProto(runSpec.mutable_table(), table);
    operation.ToProto(runSpec.mutable_operation());

    TString serializedOperation = runSpec.SerializeAsString();
    client->Map(spec, new TTestOperationMapper(serializedOperation));
}

void RunReduce(IClientPtr client, const TString& pool,
               const TString& inputPath, const TString& outputPath,
               const TTable& table, const TTable& outputTable, const TReduceOperation& operation)
{
    const auto& attributePath = BuildAttributes(outputTable) + outputPath;

    NYT::NLogging::TLogger Logger("test");
    YT_LOG_INFO("Reduce (OutputTable: %v)", attributePath);

    TReduceOperationSpec spec;
    SetBaseOperationOptions(spec);
    // TODO(orlovorlov) make reducer memory limit a configuration parameter.
    spec.ReducerSpec(TUserJobSpec().MemoryLimit(16LL << 30));
    spec.Pool(pool);
    spec.AddInput<TNode>(inputPath);
    spec.AddOutput<TNode>(attributePath);

    TVector<TString> reduceColumns(operation.ReduceBy.begin(), operation.ReduceBy.end());
    spec.SortBy(TSortColumns(reduceColumns));
    spec.ReduceBy(TSortColumns(reduceColumns));

    NProto::TReduceRunSpec runSpec;
    ToProto(runSpec.mutable_table(), table);
    for (const auto& reduceBy : operation.ReduceBy) {
        runSpec.add_reduce_by(reduceBy);
    }
    operation.Reducer->ToProto(runSpec.mutable_operation());

    TString serializedOperation = runSpec.SerializeAsString();
    client->Reduce(spec, new TTestOperationReducer(serializedOperation));
}

void RunSort(IClientPtr client, const TString& pool,
             const TString& inputPath, const TString& outputPath,
             const TSortColumns& sortColumns)
{
    TSortOperationSpec spec;
    spec.Pool(pool);
    spec.AddInput(inputPath);
    spec.Output(outputPath);
    spec.SortBy(sortColumns);
    client->Sort(spec);
}

}  // namespace NYT::NTest
