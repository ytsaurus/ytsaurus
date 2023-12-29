
#include <yt/systest/operation/util.h>
#include <yt/systest/operation/multi_map.h>

namespace NYT::NTest {

TFilterMultiMapper::TFilterMultiMapper(const TTable& input, int columnIndex, TNode value)
    : IMultiMapper(input)
    , ColumnIndex_(columnIndex)
    , Value_(std::move(value))
{
    PopulateInputColumns();
}

TFilterMultiMapper::TFilterMultiMapper(const TTable& input, const NProto::TFilterMultiMapper& proto)
    : IMultiMapper(input)
    , ColumnIndex_(proto.input_column_index())
{
    TStringInput stream(proto.value());
    Value_.Load(&stream);
    PopulateInputColumns();
}

void TFilterMultiMapper::PopulateInputColumns()
{
    InputColumnIndex_.reserve(std::ssize(Input_.DataColumns));
    for (int i = 0; i < std::ssize(Input_.DataColumns); i++) {
        InputColumnIndex_.push_back(i);
    }
}

TRange<int> TFilterMultiMapper::InputColumns() const
{
    return InputColumnIndex_;
}

TRange<TDataColumn> TFilterMultiMapper::OutputColumns() const
{
    return Input_.DataColumns;
}

std::vector<std::vector<TNode>> TFilterMultiMapper::Run(TCallState* /*state*/, TRange<TNode> input) const
{
    if (input[ColumnIndex_] == Value_) {
        return {std::vector<TNode>(input.begin(), input.end())};
    }

    return {};
}

void TFilterMultiMapper::ToProto(NProto::TMultiMapper* proto) const
{
    auto* operationProto = proto->mutable_filter();
    operationProto->set_input_column_index(ColumnIndex_);

    TTempBufOutput outputStream;
    Value_.Save(&outputStream);

    operationProto->set_value(outputStream.Data(), outputStream.Filled());
}

/////////////////////////////////////////////////////////////////////////////////////////

TSingleMultiMapper::TSingleMultiMapper(
        const TTable& input,
        std::unique_ptr<IRowMapper> inner)
    : IMultiMapper(input)
    , Inner_(std::move(inner))
{
}

TSingleMultiMapper::TSingleMultiMapper(
        const TTable& input,
        const NProto::TSingleMultiMapper& proto)
    : IMultiMapper(input)
    , Inner_(CreateFromProto(input, proto.inner()))
{
}

TRange<int> TSingleMultiMapper::InputColumns() const
{
    return Inner_->InputColumns();
}

TRange<TDataColumn> TSingleMultiMapper::OutputColumns() const
{
    return Inner_->OutputColumns();
}

TRange<TString> TSingleMultiMapper::DeletedColumns() const
{
    return Inner_->DeletedColumns();
}

void TSingleMultiMapper::ToProto(NProto::TMultiMapper* proto) const
{
    auto* operationProto = proto->mutable_single();
    Inner_->ToProto(operationProto->mutable_inner());
}

std::vector<std::vector<TNode>> TSingleMultiMapper::Run(TCallState* state, TRange<TNode> input) const
{
    return {Inner_->Run(state, input)};
}

/////////////////////////////////////////////////////////////////////////////////////////

TRepeatMultiMapper::TRepeatMultiMapper(
    const TTable& input,
    int rowCount,
    std::unique_ptr<IRowMapper> inner)
    : IMultiMapper(input)
    , Count_(rowCount)
    , Inner_(std::move(inner))
{
}

TRepeatMultiMapper::TRepeatMultiMapper(
    const TTable& input,
    const NProto::TRepeatMultiMapper& proto)
    : IMultiMapper(input)
    , Count_(proto.count())
    , Inner_(CreateFromProto(input, proto.inner()))
{
    YT_VERIFY(Inner_->DeletedColumns().empty());
}

TRange<int> TRepeatMultiMapper::InputColumns() const
{
    return Inner_->InputColumns();
}

TRange<TDataColumn> TRepeatMultiMapper::OutputColumns() const
{
    return Inner_->OutputColumns();
}

void TRepeatMultiMapper::ToProto(NProto::TMultiMapper* proto) const
{
    auto* protoOperation = proto->mutable_repeat();
    protoOperation->set_count(Count_);
    Inner_->ToProto(protoOperation->mutable_inner());
}

std::vector<std::vector<TNode>> TRepeatMultiMapper::Run(TCallState* state, TRange<TNode> input) const
{
    std::vector<std::vector<TNode>> result;
    for (int i = 0; i < Count_; i++) {
        result.push_back(Inner_->Run(state, input));
    }
    return result;
}

/////////////////////////////////////////////////////////////////////////////////////////

TCombineMultiMapper::TCombineMultiMapper(
        const TTable& input,
        std::vector<std::unique_ptr<IRowMapper>> singleOperations,
        std::unique_ptr<IMultiMapper> multiOperation)
    : IMultiMapper(input)
    , SingleOperations_(std::move(singleOperations))
    , MultiOperation_(std::move(multiOperation))
{
    PopulateInputColumns();
}

TCombineMultiMapper::TCombineMultiMapper(
    const TTable& input,
    const NProto::TCombineMultiMapper& proto)
    : IMultiMapper(input)
    , MultiOperation_(CreateFromProto(input, proto.multi_operation()))
{
    for (const auto& singleOperationProto : proto.single_operations()) {
        SingleOperations_.push_back(CreateFromProto(input, singleOperationProto));
        YT_VERIFY(SingleOperations_.back()->OutputColumns().empty());
    }
    PopulateInputColumns();
}

void TCombineMultiMapper::PopulateInputColumns()
{
    std::vector<const IOperation*> allOperations;
    for (const auto& operation : SingleOperations_) {
        YT_VERIFY(operation->OutputColumns().empty());
        YT_VERIFY(operation->DeletedColumns().empty());
        allOperations.push_back(operation.get());
    }
    YT_VERIFY(MultiOperation_->DeletedColumns().empty());
    allOperations.push_back(MultiOperation_.get());
    InputColumns_ = CollectInputColumns(allOperations);
}

std::vector<std::vector<TNode>> TCombineMultiMapper::Run(TCallState* state, TRange<TNode> input) const
{
    for (const auto& operation : SingleOperations_) {
        const auto operationInput = PopulateOperationInput(
            InputColumns_, operation->InputColumns(), input);

        auto operationOutput = operation->Run(state, input);
        YT_VERIFY(operationOutput.empty());
    }

    const auto multiInput = PopulateOperationInput(
        InputColumns_, MultiOperation_->InputColumns(), input);
    return MultiOperation_->Run(state, multiInput);
}

TRange<int> TCombineMultiMapper::InputColumns() const
{
    return InputColumns_;
}

TRange<TDataColumn> TCombineMultiMapper::OutputColumns() const
{
    return MultiOperation_->OutputColumns();
}

void TCombineMultiMapper::ToProto(NProto::TMultiMapper* proto) const
{
    auto* operationProto = proto->mutable_combine();
    for (const auto& operation : SingleOperations_) {
        NProto::TRowMapper innerProto;
        operation->ToProto(&innerProto);
        operationProto->add_single_operations()->Swap(&innerProto);
    }

    MultiOperation_->ToProto(operationProto->mutable_multi_operation());
}

}  // namespace NYT::NTest
