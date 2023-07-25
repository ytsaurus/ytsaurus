
#include <yt/systest/operation/util.h>
#include <yt/systest/operation/map.h>

namespace NYT::NTest {

TSetSeedRowMapper::TSetSeedRowMapper(const TTable& input, int columnIndex, int thisSeed)
    : IRowMapper(input)
    , InputColumnIndex{columnIndex}
    , ThisSeed_(thisSeed)
{
}

TSetSeedRowMapper::TSetSeedRowMapper(const TTable& input, const NProto::TSetSeedRowMapper& proto)
    : IRowMapper(input)
    , InputColumnIndex{proto.input_column_index()}
    , ThisSeed_(proto.this_seed())
{
}

TRange<int> TSetSeedRowMapper::InputColumns() const
{
    return TRange<int>(InputColumnIndex, 1);
}

TRange<TDataColumn> TSetSeedRowMapper::OutputColumns() const
{
    return TRange<TDataColumn>();
}

std::vector<TNode> TSetSeedRowMapper::Run(TCallState* state, TRange<TNode> input) const
{
    state->RandomEngine = std::mt19937(ThisSeed_ + input[0].AsInt64());
    return {};
}

void TSetSeedRowMapper::ToProto(NProto::TRowMapper* proto) const
{
    auto* operationProto = proto->mutable_set_seed();
    operationProto->set_input_column_index(InputColumnIndex[0]);
    operationProto->set_this_seed(ThisSeed_);
}

/////////////////////////////////////////////////////////////////////////////////////////

TGenerateRandomRowMapper::TGenerateRandomRowMapper(const TTable& input, TDataColumn output)
    : IRowMapper(input)
    , OutputColumns_{output}
{
}

TGenerateRandomRowMapper::TGenerateRandomRowMapper(const TTable& input, const NProto::TGenerateRandomRowMapper& proto)
    : IRowMapper(input)
{
    if (proto.columns().columns_size() != 1) {
        THROW_ERROR_EXCEPTION("TGenerateRandomRowMapper proto is expected to specify one "
            "output column, got %v", proto.columns().columns_size());
    }
    FromProto(&OutputColumns_[0], proto.columns().columns(0));
}

TRange<int> TGenerateRandomRowMapper::InputColumns() const
{
    return TRange<int>();
}

TRange<TDataColumn> TGenerateRandomRowMapper::OutputColumns() const
{
    return TRange<TDataColumn>(OutputColumns_, 1);
}

std::vector<TNode> TGenerateRandomRowMapper::Run(TCallState* state, TRange<TNode> input) const
{
    if (!state->RandomEngine) {
        THROW_ERROR_EXCEPTION("TCallState random engine must be set");
    }
    YT_VERIFY(input.empty());

    return {Generate(state)};
}

void TGenerateRandomRowMapper::ToProto(NProto::TRowMapper* proto) const
{
    auto* operationProto = proto->mutable_generate_random();
    NTest::ToProto(operationProto->mutable_columns()->add_columns(), OutputColumns_[0]);
}

TNode TGenerateRandomRowMapper::Generate(TCallState* state) const
{
    std::uniform_int_distribution<int8_t> distribution8;
    std::uniform_int_distribution<int16_t> distribution16;
    std::uniform_int_distribution<int64_t> distribution64;
    std::uniform_real_distribution<double> distributionDouble(-1e9, 1e9);
    switch (OutputColumns_[0].Type) {
        case NProto::EColumnType::ENone:
            THROW_ERROR_EXCEPTION("Column type must be set for generate random operation");
        case NProto::EColumnType::EInt8:
            return distribution8(*state->RandomEngine);
        case NProto::EColumnType::EInt16:
            return distribution16(*state->RandomEngine);
        case NProto::EColumnType::EInt64: {
            return distribution64(*state->RandomEngine);
        }
        case NProto::EColumnType::EDouble: {
            return distributionDouble(*state->RandomEngine);
        }
        case NProto::EColumnType::ELatinString100: {
            TString result;
            std::uniform_int_distribution<char> distribution('a', 'z');
            for (int i = 0; i < 100; i++) {
                result += distribution(*state->RandomEngine);
            }
            return result;
        }
        case NProto::EColumnType::EBytes64K: {
            TString result;
            const int size = 64 << 10;
            result.reserve(size);
            std::uniform_int_distribution<char> distribution;
            for (int i = 0; i < size; i++) {
                result += distribution(*state->RandomEngine);
            }
            return result;
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

TConcatenateColumnsRowMapper::TConcatenateColumnsRowMapper(const TTable& input, std::vector<std::unique_ptr<IRowMapper>> operations)
    : IRowMapper(input)
    , Operations_(std::move(operations))
{
    std::vector<const IOperation*> operationPtrs;
    for (const auto& operation : Operations_)
    {
        auto operationColumns = operation->OutputColumns();
        std::copy(operationColumns.begin(), operationColumns.end(), std::back_inserter(OutputColumns_));
        operationPtrs.push_back(operation.get());
    }

    InputColumns_ = CollectInputColumns(operationPtrs);
}

TConcatenateColumnsRowMapper::TConcatenateColumnsRowMapper(const TTable& input, const NProto::TConcatenateColumnsRowMapper& proto)
    : IRowMapper(input)
{
    Operations_.reserve(proto.inner_operations_size());
    for (const auto& operationProto : proto.inner_operations())
    {
        auto operation = CreateFromProto(input, operationProto);
        auto operationColumns = operation->OutputColumns();
        std::copy(operationColumns.begin(), operationColumns.end(),
            std::back_inserter(OutputColumns_));

        Operations_.push_back(std::move(operation));
    }
}

TRange<int> TConcatenateColumnsRowMapper::InputColumns() const
{
    return InputColumns_;
}

TRange<TDataColumn> TConcatenateColumnsRowMapper::OutputColumns() const
{
    return OutputColumns_;
}

void TConcatenateColumnsRowMapper::ToProto(NProto::TRowMapper* proto) const
{
    auto* protoOperation = proto->mutable_concatenate_columns();
    for (const auto& operation : Operations_) {
        operation->ToProto(protoOperation->add_inner_operations());
    }
}

std::vector<TNode> TConcatenateColumnsRowMapper::Run(TCallState* state, TRange<TNode> input) const
{
    std::vector<TNode> result;
    for (const auto& operation : Operations_) {
        const auto operationInput = PopulateOperationInput(
                InputColumns_, operation->InputColumns(), input);
        auto innerNodes = operation->Run(state, operationInput);
        std::move(innerNodes.begin(), innerNodes.end(), std::back_inserter(result));
    }
    return result;
}

}  // namespace NYT::NTest
