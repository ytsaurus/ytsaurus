
#include <library/cpp/yt/logging/logger.h>
#include <yt/systest/operation.h>
#include <yt/systest/operation/reduce.h>
#include <yt/systest/table.h>

namespace NYT::NTest {

TSumReducer::TSumReducer(const TTable& input, int columnIndex, TDataColumn outputColumn)
    : IReducer(input)
{
    if (outputColumn.Type != NProto::EColumnType::EInt64) {
        THROW_ERROR_EXCEPTION("TSumReducer output column must have type int64");
    }
    InputColumnIndex_[0] = columnIndex;
    OutputColumns_[0] = outputColumn;
}

TSumReducer::TSumReducer(const TTable& input, const NProto::TSumReducer& proto)
    : IReducer(input)
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

std::vector<std::vector<TNode>> TSumReducer::Run(TCallState* /*state*/, TRange<TRange<TNode>> input) const
{
    int64_t result = 0;
    for (const auto& nodes : input) {
        result += nodes[0].AsInt64();
    }

    return {{result}};
}

}  // namespace NYT::NTest
