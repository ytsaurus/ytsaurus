
#include <library/cpp/yt/logging/logger.h>
#include <yt/systest/operation.h>
#include <yt/systest/operation/reduce.h>
#include <yt/systest/operation/util.h>
#include <yt/systest/table.h>
#include <yt/systest/util.h>

namespace NYT::NTest {

TSumReducer::TSumReducer(const TTable& input, int columnIndex, TDataColumn outputColumn)
    : IReducer(input)
    , Result_(0)
{
    if (outputColumn.Type != NProto::EColumnType::EInt64) {
        THROW_ERROR_EXCEPTION("TSumReducer output column must have type int64");
    }
    InputColumnIndex_[0] = columnIndex;
    OutputColumns_[0] = outputColumn;
}

TSumReducer::TSumReducer(const TTable& input, const NProto::TSumReducer& proto)
    : IReducer(input)
    , Result_(0)
{
    InputColumnIndex_[0] = proto.input_column_index();
    FromProto(&OutputColumns_[0], proto.output_column());
}

TRange<int> TSumReducer::InputColumns() const
{
    return TRange<int>(InputColumnIndex_, 1);
}

TRange<TDataColumn> TSumReducer::OutputColumns() const
{
    return TRange<TDataColumn>(OutputColumns_, 1);
}

void TSumReducer::ToProto(NProto::TReducer* proto) const
{
    auto* operationProto = proto->mutable_sum();
    operationProto->set_input_column_index(InputColumnIndex_[0]);
    NTest::ToProto(operationProto->mutable_output_column(), OutputColumns_[0]);
}

void TSumReducer::StartRange(TCallState* /*state*/, TRange<TNode> /*key*/)
{
    Result_ = 0;
}

void TSumReducer::ProcessRow(TCallState* /*state*/, TRange<TNode> input)
{
    Result_ += input[0].AsInt64();
}

std::vector<std::vector<TNode>> TSumReducer::FinishRange(TCallState* /*state*/)
{
    return {{Result_}};
}

////////////////////////////////////////////////////////////////////////////////

TSumHashReducer::TSumHashReducer(const TTable& input, std::vector<int> indices, TDataColumn outputColumn)
    : IReducer(input)
    , InputColumns_(indices)
    , Result_(0)
{
    if (outputColumn.Type != NProto::EColumnType::EInt8) {
        THROW_ERROR_EXCEPTION("TSumHashReducer output column must have type int8");
    }
    OutputColumns_[0] = outputColumn;
}

TSumHashReducer::TSumHashReducer(const TTable& input, const NProto::TSumHashReducer& proto)
    : IReducer(input)
    , Result_(0)
{
    InputColumns_.reserve(proto.input_column_index_size());
    for (int index : proto.input_column_index()) {
        InputColumns_.push_back(index);
    }
    FromProto(&OutputColumns_[0], proto.output_column());
}

TRange<int> TSumHashReducer::InputColumns() const
{
    return TRange<int>(InputColumns_.begin(), InputColumns_.end());
}

TRange<TDataColumn> TSumHashReducer::OutputColumns() const
{
    return TRange<TDataColumn>(OutputColumns_, 1);
}

void TSumHashReducer::ToProto(NProto::TReducer* proto) const
{
    auto* operationProto = proto->mutable_sum_hash();
    for (int index : InputColumns_) {
        operationProto->add_input_column_index(index);
    }
    NTest::ToProto(operationProto->mutable_output_column(), OutputColumns_[0]);
}

void TSumHashReducer::StartRange(TCallState* /*state*/, TRange<TNode> /*key*/)
{
    Result_ = 0;
}

void TSumHashReducer::ProcessRow(TCallState* /*state*/, TRange<TNode> input)
{
    Result_ += static_cast<int64_t>(RowHash(input));
}

std::vector<std::vector<TNode>> TSumHashReducer::FinishRange(TCallState* /*state*/)
{
    return {{static_cast<int8_t>(Result_ & 0xff)}};
}

////////////////////////////////////////////////////////////////////////////////

TConcatenateColumnsReducer::TConcatenateColumnsReducer(const TTable& input, std::vector<std::unique_ptr<IReducer>> operations)
    : IReducer(input)
    , Operations_(std::move(operations))
{
    std::vector<const IOperation*> operationPtrs;
    for (const auto& operation : Operations_) {
        operationPtrs.push_back(operation.get());
        auto operationColumns = operation->OutputColumns();
        std::copy(operationColumns.begin(), operationColumns.end(), std::back_inserter(OutputColumns_));
    }
    InputColumns_ = CollectInputColumns(operationPtrs);
}

TConcatenateColumnsReducer::TConcatenateColumnsReducer(const TTable& input, const NProto::TConcatenateColumnsReducer& proto)
    : IReducer(input)
{
    Operations_.reserve(proto.operations_size());
    std::vector<const IOperation*> operationPtrs;
    for (const auto& operationProto : proto.operations())
    {
        auto operation = CreateFromProto(input, operationProto);
        auto operationColumns = operation->OutputColumns();

        std::copy(operationColumns.begin(), operationColumns.end(), std::back_inserter(OutputColumns_));
        Operations_.push_back(std::move(operation));
        operationPtrs.push_back(Operations_.back().get());
    }

    InputColumns_ = CollectInputColumns(operationPtrs);
}

TRange<int> TConcatenateColumnsReducer::InputColumns() const
{
    return InputColumns_;
}

TRange<TDataColumn> TConcatenateColumnsReducer::OutputColumns() const
{
    return OutputColumns_;
}

void TConcatenateColumnsReducer::StartRange(TCallState* state, TRange<TNode> key)
{
    for (auto& operation : Operations_) {
        operation->StartRange(state, key);
    }
}

void TConcatenateColumnsReducer::ProcessRow(TCallState* state, TRange<TNode> input)
{
    for (const auto& operation : Operations_) {
        const auto operationInput = PopulateOperationInput(
                InputColumns_, operation->InputColumns(), input);
        operation->ProcessRow(state, operationInput);
    }
}

std::vector<std::vector<TNode>> TConcatenateColumnsReducer::FinishRange(TCallState* state)
{
    std::vector<TNode> result;
    for (const auto& operation : Operations_) {
        auto innerNodes = operation->FinishRange(state);
        if (std::ssize(innerNodes) != 1) {
            // Consider introducing a separate interface for reducers that return exactly one row.
            THROW_ERROR_EXCEPTION("TConcatenateColumnsReducer expects inner reducer to return exactly one row, got %v rows",
                std::ssize(innerNodes));
        }
        std::move(innerNodes[0].begin(), innerNodes[0].end(), std::back_inserter(result));
    }
    return {result};
}

void TConcatenateColumnsReducer::ToProto(NProto::TReducer* proto) const
{
    auto* protoOperation = proto->mutable_concatenate_columns();
    for (const auto& operation : Operations_) {
        operation->ToProto(protoOperation->add_operations());
    }
}

}  // namespace NYT::NTest
