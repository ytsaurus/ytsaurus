
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
        case NProto::TRowMapper::kGenerateRandom: {
            return std::make_unique<TGenerateRandomRowMapper>(
                input, operationProto.generate_random());
        }
        case NProto::TRowMapper::kConcatenateColumns: {
            return std::make_unique<TConcatenateColumnsRowMapper>(
                input, operationProto.concatenate_columns());
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

void FromProto(TMapOperation* operation, const NProto::TTable& tableProto, const NProto::TMultiMapper& operationProto)
{
    FromProto(&operation->table, tableProto);
    operation->Mapper = CreateFromProto(operation->table, operationProto);
}

}  // namespace NYT::NTest
