
#include <yt/systest/operation.h>
#include <yt/systest/operation/map.h>
#include <yt/systest/operation/multi_map.h>
#include <yt/systest/operation/reduce.h>

#include <random>

namespace NYT::NTest {

std::unique_ptr<IRowMapper> CreateFromProto(
    const TTable& input,
    const NProto::TRowMapper& operationProto)
{
    switch (operationProto.operation_case()) {
        case NProto::TRowMapper::kSetSeed: {
            return std::make_unique<TSetSeedRowMapper>(input, operationProto.set_seed());
        }
        case NProto::TRowMapper::kIdentity: {
            return std::make_unique<TIdentityRowMapper>(input, operationProto.identity());
        }
        case NProto::TRowMapper::kGenerateRandom: {
            return std::make_unique<TGenerateRandomRowMapper>(
                input, operationProto.generate_random());
        }
        case NProto::TRowMapper::kConcatenateColumns: {
            return std::make_unique<TConcatenateColumnsRowMapper>(
                input, operationProto.concatenate_columns());
            break;
        }
        case NProto::TRowMapper::kDeleteColumn: {
            return std::make_unique<TDeleteColumnRowMapper>(
                input, operationProto.delete_column());
            break;
        }
        case NProto::TRowMapper::kRenameColumn: {
            return std::make_unique<TRenameColumnRowMapper>(
                input, operationProto.rename_column());
            break;
        }
        case NProto::TRowMapper::OPERATION_NOT_SET:
            break;
    }
    return nullptr;
}

std::unique_ptr<IMultiMapper> CreateFromProto(
    const TTable& input,
    const NProto::TMultiMapper& operationProto)
{
    switch (operationProto.operation_case()) {
        case NProto::TMultiMapper::kFilter: {
            return std::make_unique<TFilterMultiMapper>(
                    input, operationProto.filter());
        }
        case NProto::TMultiMapper::kSingle: {
            return std::make_unique<TSingleMultiMapper>(
                input, operationProto.single());
        }
        case NProto::TMultiMapper::kRepeat: {
            return std::make_unique<TRepeatMultiMapper>(
                input, operationProto.repeat());
        }
        case NProto::TMultiMapper::kCombine: {
            return std::make_unique<TCombineMultiMapper>(
                input, operationProto.combine());
        }
        case NProto::TMultiMapper::OPERATION_NOT_SET:
            break;
    }
    return nullptr;
}

std::unique_ptr<IReducer> CreateFromProto(
    const TTable& input,
    const NProto::TReducer& operationProto)
{
    switch (operationProto.operation_case()) {
        case NProto::TReducer::kMapper: {
            return nullptr;
        }
        case NProto::TReducer::kSequence: {
            return nullptr;
        }
        case NProto::TReducer::kSum: {
            return std::make_unique<TSumReducer>(input, operationProto.sum());
        }
        case NProto::TReducer::kConcatenateColumns: {
            return nullptr;
        }
        case NProto::TReducer::OPERATION_NOT_SET:
            break;
    }
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TOperationInputColumns* proto, TRange<int> inputColumns)
{
    for (int index : inputColumns) {
        proto->add_index(index);
    }
}

void ToProto(NProto::TOperationOutputColumns* proto, TRange<TDataColumn> outputColumns)
{
    for (const TDataColumn& column : outputColumns) {
        ToProto(proto->add_columns(), column);
    }
}

void FromProto(std::vector<int>* inputColumns, const NProto::TOperationInputColumns& proto)
{
    inputColumns->clear();
    inputColumns->reserve(proto.index_size());
    for (int index : proto.index()) {
        inputColumns->push_back(index);
    }
}

void FromProto(std::vector<TDataColumn>* columns, const NProto::TOperationOutputColumns& proto)
{
    columns->clear();
    columns->resize(proto.columns_size());
    for (int i = 0; i < proto.columns_size(); i++) {
        FromProto(&(columns->at(i)), proto.columns(i));
    }
}

IOperation::~IOperation()
{
}

IOperation::IOperation(const TTable& inputTable)
    : Input_(inputTable)
{
}

const TTable& IOperation::InputTable() const
{
    return Input_;
}

IRowMapper::IRowMapper(const TTable& inputTable)
    : IOperation(inputTable)
{
}

IMultiMapper::IMultiMapper(const TTable& inputTable)
    : IOperation(inputTable)
{
}

IReducer::IReducer(const TTable& inputTable)
    : IOperation(inputTable)
{
}

}  // namespace NYT::NTest
