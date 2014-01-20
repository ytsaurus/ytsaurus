#include "evaluate_controller.h"

#include "private.h"
#include "helpers.h"

#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/writer.h>
#include <ytlib/new_table_client/schema.h>

#include <core/concurrency/fiber.h>

#include <core/misc/protobuf_helpers.h>
#include <core/misc/chunked_memory_pool.h>

namespace NYT {
namespace NQueryClient {

using namespace NVersionedTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEvaluateController::TEvaluateController(
    IEvaluateCallbacks* callbacks,
    const TPlanFragment& fragment,
    IWriterPtr writer)
    : Callbacks_(callbacks)
    , Fragment_(fragment)
    , Writer_(std::move(writer))
    , Logger(QueryClientLogger)
{
    Logger.AddTag(Sprintf(
        "FragmendId: %s",
        ~ToString(Fragment_.Guid())));
}

TEvaluateController::~TEvaluateController()
{ }

TError TEvaluateController::Run()
{
    try {
        LOG_DEBUG("Evaluating plan fragment");

        auto producer = CreateProducer(GetHead());

        LOG_DEBUG("Opening writer");
        Writer_->Open(
            GetHead()->GetNameTable(),
            GetHead()->GetTableSchema(),
            GetHead()->GetKeyColumns());

        std::vector<TRow> rows;
        rows.reserve(1000);
        while (producer.Run(&rows)) {
            for (auto row : rows) {
               for (int i = 0; i < row.GetCount(); ++i) {
                    Writer_->WriteValue(row[i]);
                }
                if (!Writer_->EndRow()) {
                    auto result = WaitFor(Writer_->GetReadyEvent());
                    RETURN_IF_ERROR(result);
                }
            }
            rows.clear();
        }

        LOG_DEBUG("Closing writer");
        auto error = WaitFor(Writer_->Close());
        if (!error.IsOK()) {
            LOG_ERROR(error);
            return error;
        }

        LOG_DEBUG("Finished evaluating plan fragment");
        return TError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to evaluate plan fragment") << ex;
        LOG_ERROR(error);
        return error;
    }
}

TEvaluateController::TProducer TEvaluateController::CreateProducer(const TOperator* op)
{
    switch (op->GetKind()) {
        case EOperatorKind::Scan:
            return TProducer(BIND(&TEvaluateController::ScanRoutine,
                Unretained(this),
                op->As<TScanOperator>()));
        case EOperatorKind::Union:
            return TProducer(BIND(&TEvaluateController::UnionRoutine,
                Unretained(this),
                op->As<TUnionOperator>()));
        case EOperatorKind::Filter:
            return TProducer(BIND(&TEvaluateController::FilterRoutine,
                Unretained(this),
                op->As<TFilterOperator>()));
        case EOperatorKind::Group:
            return TProducer(BIND(&TEvaluateController::GroupRoutine,
                Unretained(this),
                op->As<TGroupOperator>()));
        case EOperatorKind::Project:
            return TProducer(BIND(&TEvaluateController::ProjectRoutine,
                Unretained(this),
                op->As<TProjectOperator>()));
    }
    YUNREACHABLE();
}

void TEvaluateController::ScanRoutine(
    const TScanOperator* op,
    TProducer& self,
    std::vector<TRow>* rows)
{
    YCHECK(op);

    LOG_DEBUG("Creating producer for scan operator (Op: %p)", op);

    auto reader = GetCallbacks()->GetReader(op->DataSplit());
    auto schema = GetTableSchemaFromDataSplit(op->DataSplit());

    LOG_DEBUG("Opening reader");

    {
        auto nameTable = New<TNameTable>();
        auto error = WaitFor(reader->Open(nameTable, schema));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    while (true) {
        bool hasMoreData = reader->Read(rows);
        bool shouldWait = (rows->size() < rows->capacity());
        std::tie(rows) = self.Yield();
        if (!hasMoreData) {
            break;
        }
        if (shouldWait) {
            auto error = WaitFor(reader->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }
    }

    LOG_DEBUG("Done producing for scan operator (Op: %p)", op);
}

void TEvaluateController::UnionRoutine(
    const TUnionOperator* op,
    TProducer& self,
    std::vector<TRow>* rows)
{
    YASSERT(op);
    LOG_DEBUG("Creating producer for union operator (Op: %p)", op);

    // TODO(sandello): Plug a merging reader here.
    for (const auto& sourceOp : op->Sources()) {
        auto source = CreateProducer(sourceOp);
        while (source.Run(rows)) {
            std::tie(rows) = self.Yield();
        }
    }

    LOG_DEBUG("Done producing for union operator (Op: %p)", op);
}

void TEvaluateController::FilterRoutine(
    const TFilterOperator* op,
    TProducer& self,
    std::vector<TRow>* rows)
{
    YASSERT(op);
    LOG_DEBUG("Creating producer for filter operator (Op: %p)", op);

    auto source = CreateProducer(op->GetSource());
    auto sourceTableSchema = op->GetTableSchema();

    while (source.Run(rows)) {
        rows->erase(
            std::remove_if(
                rows->begin(),
                rows->end(),
                [=] (const TRow row) -> bool {
                    auto value = EvaluateExpression(op->GetPredicate(), row, sourceTableSchema);
                    YCHECK(value.Type == EValueType::Integer);
                    return value.Data.Integer == 0;
                }),
            rows->end());
        std::tie(rows) = self.Yield();
    }

    LOG_DEBUG("Done producing for filter operator (Op: %p)", op);
}

template <class T>
size_t THashCombine(size_t seed, const T& value)
{
    std::hash<T> hasher;
    return seed ^ (hasher(value) + 0x9e3779b9 + (seed << 6) + (seed >> 2)); 
    // TODO(lukyan): Fix this function
}

class TGroupHasher
{
public:
    explicit TGroupHasher(int keySize)
        : KeySize_(keySize)
    { }

    size_t operator() (TRow key) const
    {
        size_t result = 0;
        for (int i = 0; i < KeySize_; ++i) {
            YCHECK(key[i].Type == EValueType::Integer);
            result = THashCombine(result, key[i].Data.Integer);
        }
        return result;
    }

private:
    int KeySize_;

};

class TGroupComparer
{
public:
    explicit TGroupComparer(int keySize)
        : KeySize_(keySize)
    { }

    bool operator() (TRow lhs, TRow rhs) const
    {
        for (int i = 0; i < KeySize_; ++i) {
            YCHECK(lhs[i].Type == EValueType::Integer);
            YCHECK(rhs[i].Type == EValueType::Integer);

            if (lhs[i].Data.Integer != rhs[i].Data.Integer) {
                return false;
            }
        }
        return true;
    }

private:
    int KeySize_;

};

void TEvaluateController::GroupRoutine(
    const TGroupOperator* op,
    TProducer& self,
    std::vector<TRow>* rows)
{
    YASSERT(op);
    LOG_DEBUG("Creating producer for group operator (Op: %p)", op);

    auto source = CreateProducer(op->GetSource());
    auto sourceTableSchema = op->GetSource()->GetTableSchema();
    TChunkedMemoryPool memoryPool;

    auto nameTable = op->GetNameTable();
    int keySize = op->GetGroupItemCount();

    std::vector<TRow> groupedRows;
    std::unordered_set<TRow, TGroupHasher, TGroupComparer> keys(
        256,
        TGroupHasher(keySize),
        TGroupComparer(keySize));

    std::vector<TRow> sourceRows;
    sourceRows.reserve(1000);

    auto resultRow = TRow::Allocate(&memoryPool, keySize + op->AggregateItems().size());

    while (source.Run(&sourceRows)) {
        for (auto row : sourceRows) {
            for (int i = 0; i < keySize; ++i) {
                resultRow[i] = EvaluateExpression(op->GetGroupItem(i).Expression, row, sourceTableSchema);
                resultRow[i].Id = nameTable->GetId(op->GetGroupItem(i).Name);
            }

            for (int i = 0; i < op->AggregateItems().size(); ++i) {
                resultRow[keySize + i] = EvaluateExpression(op->AggregateItems()[i].Expression, row, sourceTableSchema);
                resultRow[keySize + i].Id = nameTable->GetId(op->AggregateItems()[i].Name);
            }

            auto keysIt = keys.find(resultRow);
            auto keysJt = keys.end();

            if (keysIt != keysJt) {
                auto aggregateRow = *keysIt;
                for (int i = 0; i < op->AggregateItems().size(); ++i) {
                    YCHECK(aggregateRow[keySize + i].Type == EValueType::Integer);
                    YCHECK(resultRow[keySize + i].Type == EValueType::Integer);

                    auto& aggregateValue = aggregateRow[keySize + i].Data.Integer;
                    auto resultValue = resultRow[keySize + i].Data.Integer;

                    switch (op->AggregateItems()[i].AggregateFunction) {
                        case EAggregateFunctions::Sum:
                            aggregateValue += resultValue;
                            break;
                        case EAggregateFunctions::Min:
                            aggregateValue = std::min(aggregateValue, resultValue);
                            break;
                        case EAggregateFunctions::Max:
                            aggregateValue = std::max(aggregateValue, resultValue);
                            break;
                        default:
                            YUNIMPLEMENTED();
                    }
                }
            } else {
                groupedRows.push_back(resultRow);
                keys.insert(groupedRows.back());
                resultRow = TRow::Allocate(&memoryPool, keySize + op->AggregateItems().size());
            }
        }

        sourceRows.clear();
        sourceRows.reserve(1000);
    }

    rows->swap(groupedRows);
    std::tie(rows) = self.Yield();
    memoryPool.Clear();

    LOG_DEBUG("Done producing for group operator (Op: %p)", op);
}

void TEvaluateController::ProjectRoutine(
    const TProjectOperator* op,
    TProducer& self,
    std::vector<TRow>* rows)
{
    YASSERT(op);
    LOG_DEBUG("Creating producer for project operator (Op: %p)", op);

    auto source = CreateProducer(op->GetSource());
    auto sourceTableSchema = op->GetSource()->GetTableSchema();
    TChunkedMemoryPool memoryPool;

    auto nameTable = op->GetNameTable();
    while (source.Run(rows)) {
        std::transform(
            rows->begin(),
            rows->end(),
            rows->begin(),
            [&] (const TRow row) -> TRow {
                auto result = TRow::Allocate(&memoryPool, op->GetProjectionCount());
                for (int i = 0; i < op->GetProjectionCount(); ++i) {
                    result[i] = EvaluateExpression(op->GetProjection(i).Expression, row, sourceTableSchema);
                    result[i].Id = nameTable->GetId(op->GetProjection(i).Name);
                }
                return result;
            });
        std::tie(rows) = self.Yield();
        memoryPool.Clear();
    }

    LOG_DEBUG("Done producing for project operator (Op: %p)", op);
}

TValue TEvaluateController::EvaluateExpression(
    const TExpression* expr,
    const TRow row,
    const TTableSchema& tableSchema) const
{
    YASSERT(expr);
    switch (expr->GetKind()) {
        case EExpressionKind::IntegerLiteral:
            return MakeIntegerValue<TValue>(
                expr->As<TIntegerLiteralExpression>()->GetValue());
        case EExpressionKind::DoubleLiteral:
            return MakeDoubleValue<TValue>(
                expr->As<TDoubleLiteralExpression>()->GetValue());
        case EExpressionKind::Reference:
        {
            int index = tableSchema.GetColumnIndexOrThrow(expr->As<TReferenceExpression>()->GetName());
            return row[index];
        }
        case EExpressionKind::Function:
            return EvaluateFunctionExpression(
                expr->As<TFunctionExpression>(),
                row,
                tableSchema);
        case EExpressionKind::BinaryOp:
            return EvaluateBinaryOpExpression(
                expr->As<TBinaryOpExpression>(),
                row,
                tableSchema);
    }
    YUNREACHABLE();
}

TValue TEvaluateController::EvaluateFunctionExpression(
    const TFunctionExpression* expr,
    const TRow row,
    const TTableSchema& tableSchema) const
{
    YASSERT(expr);
    YUNIMPLEMENTED();
}

TValue TEvaluateController::EvaluateBinaryOpExpression(
    const TBinaryOpExpression* expr,
    const TRow row,
    const TTableSchema& tableSchema) const
{
    YASSERT(expr);

    auto lhsValue = EvaluateExpression(expr->GetLhs(), row, tableSchema);
    auto rhsValue = EvaluateExpression(expr->GetRhs(), row, tableSchema);
    auto name = expr->GetName();

    switch (expr->GetOpcode()) {
#define XX_RETURN_INTEGER(value) \
        return MakeIntegerValue<TValue>((value))
#define XX_RETURN_DOUBLE(value) \
        return MakeDoubleValue<TValue>((value))

        // Arithmetical operations.
#define XX(opcode, optype) \
        case EBinaryOp::opcode: \
            switch (expr->GetType(tableSchema)) { \
                case EValueType::Integer: \
                    XX_RETURN_INTEGER( \
                        lhsValue.Data.Integer optype rhsValue.Data.Integer); \
                case EValueType::Double: \
                    XX_RETURN_DOUBLE( \
                        lhsValue.Data.Double optype rhsValue.Data.Double); \
                default: \
                    YUNREACHABLE(); \
            } \
            break
        XX(Plus, +);
        XX(Minus, -);
        XX(Multiply, *);
        XX(Divide, /);
#undef XX

        // Integral and logical operations.
#define XX(opcode, optype) \
        case EBinaryOp::opcode: \
            switch (expr->GetType(tableSchema)) { \
                case EValueType::Integer: \
                    XX_RETURN_INTEGER( \
                        lhsValue.Data.Integer optype rhsValue.Data.Integer); \
                default: \
                    YUNREACHABLE(); /* Typechecked. */ \
            } \
            break
        XX(Modulo, %);
        XX(And, &);
        XX(Or, |);
#undef XX

        // Relational operations.
#define XX(opcode, optype) \
        case EBinaryOp::opcode: \
            switch (expr->GetType(tableSchema)) { \
                case EValueType::Integer: \
                    XX_RETURN_INTEGER( \
                        lhsValue.Data.Integer optype rhsValue.Data.Integer \
                        ? 1 : 0); \
                case EValueType::Double: \
                    XX_RETURN_INTEGER( \
                        lhsValue.Data.Double optype rhsValue.Data.Double \
                        ? 1 : 0); \
                default: \
                    YUNREACHABLE(); /* Typechecked. */ \
            } \
            break
        XX(Equal, ==);
        XX(NotEqual, !=);
        XX(Less, <);
        XX(LessOrEqual, <=);
        XX(Greater, >);
        XX(GreaterOrEqual, >=);
#undef XX

#undef XX_RETURN_INTEGER
#undef XX_RETURN_DOUBLE
    }

    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

