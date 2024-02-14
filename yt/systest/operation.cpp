
#include <yt/systest/operation.h>
#include <yt/systest/operation/map.h>
#include <yt/systest/operation/multi_map.h>
#include <yt/systest/operation/reduce.h>

#include <random>

namespace NYT::NTest {

static std::vector<int> computeIndices(
    const std::vector<TString>& columns,
    const std::vector<TDataColumn>& tableColumns,
    const std::vector<int>& order)
{
    std::vector<int> result;
    for (const TString& columnName : columns) {
        auto position = std::lower_bound(order.begin(), order.end(), nullptr, [&](int pos, nullptr_t) {
            return tableColumns[pos].Name < columnName;
        });
        if (position == order.end() || tableColumns[*position].Name != columnName) {
            THROW_ERROR_EXCEPTION("Unknown column %v", columnName);
        }
        result.push_back(*position);
    }
    return result;
}

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
        case NProto::TRowMapper::kDecorateWithDeletedColumn: {
            return std::make_unique<TDecorateWithDeletedColumnRowMapper>(
                input, operationProto.decorate_with_deleted_column());
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
        case NProto::TReducer::kSum: {
            return std::make_unique<TSumReducer>(input, operationProto.sum());
        }
        case NProto::TReducer::kSumHash: {
            return std::make_unique<TSumHashReducer>(input, operationProto.sum_hash());
        }
        case NProto::TReducer::kConcatenateColumns: {
            return std::make_unique<TConcatenateColumnsReducer>(input, operationProto.concatenate_columns());
        }
        case NProto::TReducer::OPERATION_NOT_SET:
            break;
    }
    return nullptr;
}

TTable CreateTableFromMapOperation(const IMultiMapper& op)
{
    return TTable{
        std::vector<TDataColumn>{
            op.OutputColumns().begin(), op.OutputColumns().end()
        },
        std::vector<TString>{
            op.DeletedColumns().begin(), op.DeletedColumns().end()
        },
        0  /*SortColumns*/
    };
}

TTable CreateTableFromReduceOperation(
    const TTable& source,
    const TReduceOperation& op, std::vector<int>* indices)
{
    std::vector<int> order;
    const auto& columns = source.DataColumns;
    for (int i = 0; i < std::ssize(columns); ++i) {
        order.push_back(i);
    }

    std::sort(order.begin(), order.end(), [&columns](int lhs, int rhs) {
        return columns[lhs].Name < columns[rhs].Name;
    });

    *indices = computeIndices(op.ReduceBy, columns, order);

    const int sortColumns = source.SortColumns;
    for (int index : *indices) {
        if (index >= sortColumns) {
            THROW_ERROR_EXCEPTION("Table must be sorted by a reduce column, column %v "
                "not in %v sort columns", columns[index].Name, sortColumns);
        }
    }

    TTable result;
    for (int index : *indices) {
        result.DataColumns.push_back(source.DataColumns[index]);
    }
    std::copy(op.Reducer->OutputColumns().begin(), op.Reducer->OutputColumns().end(),
        std::back_inserter(result.DataColumns));
    result.SortColumns = std::ssize(*indices);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSortRunSpec *proto, const TTable& table, const TSortOperation& operation)
{
    ToProto(proto->mutable_table(), table);
    for (const auto& column : operation.SortBy) {
        proto->add_sort_by(column);
    }
}

void FromProto(TTable* table, TSortOperation* operation, const NProto::TSortRunSpec& proto)
{
    FromProto(table, proto.table());
    operation->SortBy.clear();
    for (const auto& column : proto.sort_by()) {
        operation->SortBy.push_back(column);
    }
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
