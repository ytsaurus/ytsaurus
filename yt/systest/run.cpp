
#include <library/cpp/yson/node/node.h>
#include <library/cpp/yt/logging/logger.h>
#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/systest/operation.h>
#include <yt/systest/proto/mapper.pb.h>
#include <yt/systest/table.h>

#include <yt/systest/run.h>

#include <stdio.h>

namespace NYT::NTest {

static std::vector<TNode> ProduceInput(
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

TTestOperationMapper::TTestOperationMapper()
{
}

TTestOperationMapper::TTestOperationMapper(TString SerializedOperation)
        : SerializedOperation_(std::move(SerializedOperation))
{
}

void TTestOperationMapper::Start(TWriter* /*writer*/)
{
    NProto::TMapper mapperProto;
    mapperProto.ParseFromStringOrThrow(SerializedOperation_);

    FromProto(&Table_, mapperProto.table());
    Operation_ = CreateFromProto(Table_, mapperProto.operation());
}

void TTestOperationMapper::Do(TTableReader<TNode>* input, TTableWriter<TNode>* output)
{
    const int numInputColumns = std::ssize(Operation_->InputColumns());
    std::unordered_map<TString, int> columnPositions;
    for (int i = 0; i < numInputColumns; ++i) {
        int position = Operation_->InputColumns()[i];
        columnPositions[Operation_->InputTable().DataColumns[position].Name] = i;
    }

    for (; input->IsValid(); input->Next()) {
        TCallState rowMapState;
        const auto row = input->GetRow();

        const auto input = ProduceInput(columnPositions, row.AsMap(), numInputColumns);

        auto rows = Operation_->Run(&rowMapState, input);
        const auto& outputColumns = Operation_->OutputColumns();

        for (const auto& row : rows) {
            auto outputNode = TNode::CreateMap();
            auto& outputMap = outputNode.AsMap();
            for (int i = 0; i < std::ssize(outputColumns); i++) {
                outputMap[outputColumns[i].Name] = row[i];
            }
            output->AddRow(outputNode);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void RunMap(IClientPtr client, const TTestHome& home,
            const TString& inputPath, const TString& outputPath,
            const TTable& table, const IMultiMapper& operation)
{
    TMapOperationSpec spec;
    spec.AddInput<TNode>(inputPath);
    spec.AddOutput<TNode>(outputPath);
    spec.CoreTablePath(home.CoreTable());
    spec.StderrTablePath(home.StderrTable());
    spec.JobCount(10);
    spec.Ordered(true);

    NProto::TMapper mapperProto;
    ToProto(mapperProto.mutable_table(), table);
    operation.ToProto(mapperProto.mutable_operation());

    TString serializedOperation = mapperProto.SerializeAsString();
    client->Map(spec, new TTestOperationMapper(serializedOperation));
}

}  // namespace NYT::NTest
