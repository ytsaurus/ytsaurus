#include "stdafx.h"

#include "plan_fragment.h"
#include "private.h"
#include "helpers.h"
#include "plan_helpers.h"
#include "lexer.h"
#include "parser.hpp"
#include "callbacks.h"

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/table_client/chunk_meta_extensions.h>

#include <core/misc/protobuf_helpers.h>

#include <core/ytree/convert.h>

#include <ytlib/query_client/plan_fragment.pb.h>

#include <limits>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NVersionedTableClient;

using NYT::ToProto;
using NYT::FromProto;

using NNodeTrackerClient::TNodeDirectory;

static const auto& Logger = QueryClientLogger;
static const int PlanFragmentDepthLimit = 50;

////////////////////////////////////////////////////////////////////////////////

struct TTableSchemaProxy
{
    TTableSchema TableSchema;
    std::set<Stroka>* LiveColumns;

    explicit TTableSchemaProxy(
        const TTableSchema& tableSchema,
        std::set<Stroka>* liveColumns = nullptr)
        : TableSchema(tableSchema)
        , LiveColumns(liveColumns)
    { }

    const TColumnSchema& operator [] (size_t index) const
    {
        return TableSchema.Columns()[index];
    }

    size_t GetColumnIndex(const TStringBuf& name) const
    {
        if (LiveColumns) {
            LiveColumns->emplace(name);
        }

        auto* column = TableSchema.FindColumn(name);
        if (!column) {
            THROW_ERROR_EXCEPTION("Undefined reference %Qv", name);
        }

        return TableSchema.GetColumnIndex(*column);
    }
};

struct TGroupClauseProxy
{
    TTableSchemaProxy SourceSchemaProxy;
    TGroupClause& Op;
    std::map<Stroka, size_t> SubexprNames;

    TGroupClauseProxy(
        const TTableSchemaProxy& sourceSchemaProxy,
        TGroupClause& op)
        : SourceSchemaProxy(sourceSchemaProxy)
        , Op(op)
    { }

};

Stroka InferName(TConstExpressionPtr expr)
{
    bool newTuple = true;
    auto comma = [&] {
        bool isNewTuple = newTuple;
        newTuple = false;
        return Stroka(isNewTuple ? "" : ", ");
    };

    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        return ToString(static_cast<TUnversionedValue>(literalExpr->Value));
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        return referenceExpr->ColumnName;
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        auto str = functionExpr->FunctionName + "(";
        for (const auto& argument : functionExpr->Arguments) {
            str += comma() + InferName(argument);
        }
        return str + ")";
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        auto canOmitParenthesis = [] (TConstExpressionPtr expr) {
            return
                expr->As<TLiteralExpression>() ||
                expr->As<TReferenceExpression>() ||
                expr->As<TFunctionExpression>();
        };
        auto lhsName = InferName(binaryOp->Lhs);
        if (!canOmitParenthesis(binaryOp->Lhs)) {
            lhsName = "(" + lhsName + ")";
        }
        auto rhsName = InferName(binaryOp->Rhs);
        if (!canOmitParenthesis(binaryOp->Rhs)) {
            rhsName = "(" + rhsName + ")";
        }
        return
            lhsName +
            " " + GetBinaryOpcodeLexeme(binaryOp->Opcode) + " " +
            rhsName;
    } else if (auto inOp = expr->As<TInOpExpression>()) {
        auto str = Stroka("(");
        for (const auto& argument : inOp->Arguments) {
            str += comma() + InferName(argument);
        }
        str += ") IN (";
        newTuple = true;
        for (const auto& row: inOp->Values) {
            str += comma() + "(" + ToString(row) + ")";
        }
        return str + ")";
    } else {
        YUNREACHABLE();
    }
}

Stroka InferName(TConstQueryPtr query)
{
    bool newBlock = true;
    auto block = [&] {
        bool isNewBlock = newBlock;
        newBlock = false;
        return Stroka(isNewBlock ? "" : " ");
    };

    bool newTuple = true;
    auto comma = [&] {
        bool isNewTuple = newTuple;
        newTuple = false;
        return Stroka(isNewTuple ? "" : ", ");
    };

    Stroka str;

    str += block() + "SELECT ";
    if (query->ProjectClause) {
        newTuple = true;
        for (const auto& namedItem : query->ProjectClause.Get().Projections) {
            str += comma() + InferName(namedItem.Expression) + " AS " + namedItem.Name;
        }
    } else {
        str += "*";
    }

    if (query->GroupClause) {
        str += block() + "GROUP BY ";
        newTuple = true;
        for (const auto& namedItem : query->GroupClause.Get().GroupItems) {
            str += comma() + InferName(namedItem.Expression) + " AS " + namedItem.Name;
        }
    }

    if (query->Predicate) {
        str += block() + "WHERE " + InferName(query->Predicate);
    }

    return str;
}

Stroka TExpression::GetName() const
{
    return Stroka();
}

EValueType InferBinaryExprType(EBinaryOp opCode, EValueType lhsType, EValueType rhsType, const TStringBuf& source)
{
    if (lhsType != rhsType) {
        THROW_ERROR_EXCEPTION(
            "Type mismatch in expression %Qv",
            source)
            << TErrorAttribute("lhs_type", ToString(lhsType))
            << TErrorAttribute("rhs_type", ToString(rhsType));
    }

    EValueType operandType = lhsType;

    switch (opCode) {
        case EBinaryOp::Plus:
        case EBinaryOp::Minus:
        case EBinaryOp::Multiply:
        case EBinaryOp::Divide:
            if (!IsArithmeticType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires either integral or floating-point operands",
                    source)
                    << TErrorAttribute("operand_type", ToString(operandType));
            }
            return operandType;

        case EBinaryOp::Modulo:
            if (!IsIntegralType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires integral operands",
                    source)
                    << TErrorAttribute("operand_type", ToString(operandType));
            }
            return operandType;

        case EBinaryOp::And:
        case EBinaryOp::Or:
            if (operandType != EValueType::Boolean) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires boolean operands",
                    source)
                    << TErrorAttribute("operand_type", ToString(operandType));
            }
            return EValueType::Boolean;

        case EBinaryOp::Equal:
        case EBinaryOp::NotEqual:
        case EBinaryOp::Less:
        case EBinaryOp::Greater:
        case EBinaryOp::LessOrEqual:
        case EBinaryOp::GreaterOrEqual:
            if (!IsComparableType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires either integral, floating-point or string operands",
                    source)
                    << TErrorAttribute("lhs_type", ToString(operandType));
            }
            return EValueType::Boolean;

        default:
            YUNREACHABLE();
    }
}

EValueType InferFunctionExprType(Stroka functionName, const std::vector<EValueType>& argTypes, const TStringBuf& source)
{
    functionName.to_lower();

    auto validateArgCount = [&] (int argCount) {
        if (argTypes.size() != argCount) {
            THROW_ERROR_EXCEPTION(
                "Expression %Qv expects %v arguments, but %v provided",
                functionName,
                argCount,
                argTypes.size())
                << TErrorAttribute("expression", source);
        }
    };

    auto checkTypeCast = [&] (EValueType dstType) {
        validateArgCount(1);
        auto argType = argTypes[0];

        if (argType != EValueType::Int64 && argType != EValueType::Uint64 && argType != EValueType::Double) {
            THROW_ERROR_EXCEPTION("Conversion %Qv is not supported for this types", source)
                << TErrorAttribute("src_type", ToString(argType))
                << TErrorAttribute("dst_type", ToString(dstType));
        }

        return dstType;
    };

    if (functionName == "if") {
        validateArgCount(3);

        auto conditionType = argTypes[0];
        auto thenType = argTypes[1];
        auto elseType = argTypes[2];

        if (conditionType != EValueType::Boolean) {
            THROW_ERROR_EXCEPTION("Expected condition %Qv to be boolean", source)
                << TErrorAttribute("condition_type", ToString(conditionType));
        }

        if (thenType != elseType) {
            THROW_ERROR_EXCEPTION(
                "Type mismatch in expression %Qv",
                source)
                << TErrorAttribute("then_type", ToString(thenType))
                << TErrorAttribute("else_type", ToString(elseType));
        }

        return thenType;
    } else if (functionName == "is_prefix") {
        validateArgCount(2);

        auto lhsType = argTypes[0];
        auto rhsType = argTypes[1];

        if (lhsType != EValueType::String || rhsType != EValueType::String) {
            THROW_ERROR_EXCEPTION(
                "Expression %Qv supports only string arguments",
                source)
                << TErrorAttribute("lhs_type", ToString(lhsType))
                << TErrorAttribute("rhs_type", ToString(rhsType));
        }

        return EValueType::Boolean;
    } else if (functionName == "lower") {
        validateArgCount(1);
        auto argType = argTypes[0];

        if (argType != EValueType::String) {
            THROW_ERROR_EXCEPTION(
                "Expression %Qv supports only string argument",
                source)
                << TErrorAttribute("arg_type", ToString(argType));
        }

        return EValueType::String;
    } else if (functionName == "is_null") {
        validateArgCount(1);
        return EValueType::Boolean;
    } else if (functionName == "int64") {
        return checkTypeCast(EValueType::Int64);
    } else if (functionName == "uint64") {
        return checkTypeCast(EValueType::Uint64);
    } else if (functionName == "double") {
        return checkTypeCast(EValueType::Double);
    }

    THROW_ERROR_EXCEPTION(
        "Unknown function in expression %Qv",
        source)
        << TErrorAttribute("function_name", functionName);

}

void CheckExpressionDepth(const TConstExpressionPtr& op, int depth = 0)
{
    if (depth > PlanFragmentDepthLimit) {
        THROW_ERROR_EXCEPTION("Plan fragment depth limit exceeded");
    }

    if (op->As<TLiteralExpression>() || op->As<TReferenceExpression>() || op->As<TInOpExpression>()) {
        return;
    } else if (auto functionExpr = op->As<TFunctionExpression>()) {
        for (const auto& argument : functionExpr->Arguments) {
            CheckExpressionDepth(argument, depth + 1);
        }
        return;
    } else if (auto binaryOpExpr = op->As<TBinaryOpExpression>()) {
        CheckExpressionDepth(binaryOpExpr->Lhs, depth + 1);
        CheckExpressionDepth(binaryOpExpr->Rhs, depth + 1);
        return;
    }
    YUNREACHABLE();
};

static std::vector<TConstExpressionPtr> BuildTypedExpression(
    const TTableSchemaProxy& tableSchema,
    const NAst::TExpression* expr,
    TGroupClauseProxy* groupProxy,
    const Stroka& querySourceString)
{
    auto getAggregate = [] (TStringBuf functionName) {
        Stroka name(functionName);
        name.to_lower();

        TNullable<EAggregateFunctions> result;

        if (name == "sum") {
            result.Assign(EAggregateFunctions::Sum);
        } else if (name == "min") {
            result.Assign(EAggregateFunctions::Min);
        } else if (name == "max") {
            result.Assign(EAggregateFunctions::Max);
        } else if (name == "avg") {
            result.Assign(EAggregateFunctions::Average);
        } else if (name == "count") {
            result.Assign(EAggregateFunctions::Count);
        }

        return result;
    };

    auto captureRows = [] (const NAst::TValueTupleList& literalTuples, size_t keySize) {
        TUnversionedOwningRowBuilder rowBuilder;

        std::vector<TOwningRow> result;
        for (const auto & tuple : literalTuples) {
            for (auto literal : tuple) {
                rowBuilder.AddValue(literal);
            }
            result.push_back(rowBuilder.FinishRow());
        }
        std::sort(result.begin(), result.end());

        return result;
    };

    std::function<std::vector<TConstExpressionPtr>(
        const TTableSchemaProxy&,
        const NAst::TExpression*,
        TGroupClauseProxy*)>
        buildTypedExpression = [&] (
            const TTableSchemaProxy& tableSchema,
            const NAst::TExpression* expr,
            TGroupClauseProxy* groupProxy) -> std::vector<TConstExpressionPtr> {

        std::vector<TConstExpressionPtr> result;
        if (auto commaExpr = expr->As<NAst::TCommaExpression>()) {
            auto typedLhsExprs = buildTypedExpression(tableSchema, commaExpr->Lhs.Get(), groupProxy);
            auto typedRhsExprs = buildTypedExpression(tableSchema, commaExpr->Rhs.Get(), groupProxy);

            result.insert(result.end(), typedLhsExprs.begin(), typedLhsExprs.end());
            result.insert(result.end(), typedRhsExprs.begin(), typedRhsExprs.end());
        } else if (auto literalExpr = expr->As<NAst::TLiteralExpression>()) {
            result.push_back(New<TLiteralExpression>(
                literalExpr->SourceLocation,
                EValueType(literalExpr->Value.Type),
                literalExpr->Value));
        } else if (auto referenceExpr = expr->As<NAst::TReferenceExpression>()) {
            size_t index = tableSchema.GetColumnIndex(referenceExpr->ColumnName);
            result.push_back(New<TReferenceExpression>(
                referenceExpr->SourceLocation,
                tableSchema[index].Type,
                referenceExpr->ColumnName));
        } else if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
            auto functionName = functionExpr->FunctionName;
            auto aggregateFunction = getAggregate(functionName);

            if (aggregateFunction) {
                if (!groupProxy) {
                    THROW_ERROR_EXCEPTION(
                        "Misuse of aggregate function %v",
                        aggregateFunction.Get())
                        << TErrorAttribute("source", functionExpr->GetSource(querySourceString));
                }
                
                auto& groupOp = groupProxy->Op;

                auto subexprName = InferName(functionExpr);
                auto emplaced = groupProxy->SubexprNames.emplace(subexprName, groupOp.AggregateItems.size());
                if (emplaced.second) {
                    auto typedOperands = buildTypedExpression(
                        groupProxy->SourceSchemaProxy,
                        functionExpr->Arguments.Get(),
                        nullptr);

                    if (typedOperands.size() != 1) {
                        THROW_ERROR_EXCEPTION(
                            "Aggregate function %Qv must have exactly one argument",
                            aggregateFunction.Get())
                            << TErrorAttribute("source", functionExpr->GetSource(querySourceString));
                    }

                    CheckExpressionDepth(typedOperands.front());

                    groupOp.AggregateItems.emplace_back(
                        typedOperands.front(),
                        aggregateFunction.Get(),
                        subexprName);
                }

                result.push_back(New<TReferenceExpression>(
                    NullSourceLocation,
                    groupOp.AggregateItems[emplaced.first->second].Expression->Type,
                    subexprName));
            } else {
                std::vector<EValueType> types;

                auto typedOperands = buildTypedExpression(tableSchema, functionExpr->Arguments.Get(), groupProxy);

                for (const auto& typedOperand : typedOperands) {
                    types.push_back(typedOperand->Type);
                }

                result.push_back(New<TFunctionExpression>(
                    functionExpr->SourceLocation,
                    InferFunctionExprType(functionName, types, functionExpr->GetSource(querySourceString)),
                    functionName,
                    typedOperands));
            }
        } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
            auto typedLhsExpr = buildTypedExpression(tableSchema, binaryExpr->Lhs.Get(), groupProxy);
            auto typedRhsExpr = buildTypedExpression(tableSchema, binaryExpr->Rhs.Get(), groupProxy);

            auto makeBinaryExpr = [&] (EBinaryOp op, const TConstExpressionPtr& lhs, const TConstExpressionPtr& rhs) {
                return New<TBinaryOpExpression>(
                    binaryExpr->SourceLocation,
                    InferBinaryExprType(
                        op,
                        lhs->Type,
                        rhs->Type,
                        binaryExpr->GetSource(querySourceString)),
                    op,
                    lhs,
                    rhs);
            };

            std::function<TConstExpressionPtr(size_t, size_t, EBinaryOp)> gen = [&] (size_t offset, size_t keySize, EBinaryOp op) -> TConstExpressionPtr {
                if (offset + 1 < keySize) {
                    auto next = gen(offset + 1, keySize, op);
                    auto eq = MakeAndExpression(
                            makeBinaryExpr(EBinaryOp::Equal, typedLhsExpr[offset], typedRhsExpr[offset]),
                            next);
                    if (op == EBinaryOp::Less || op == EBinaryOp::LessOrEqual) {
                        return MakeOrExpression(
                            makeBinaryExpr(EBinaryOp::Less, typedLhsExpr[offset], typedRhsExpr[offset]),
                            eq);
                    } else if (op == EBinaryOp::Greater || op == EBinaryOp::GreaterOrEqual)  {
                        return MakeOrExpression(
                            makeBinaryExpr(EBinaryOp::Greater, typedLhsExpr[offset], typedRhsExpr[offset]),
                            eq);
                    } else {
                        return eq;
                    }                  
                } else {
                    return makeBinaryExpr(op, typedLhsExpr[offset], typedRhsExpr[offset]);
                }
            };

            if (binaryExpr->Opcode == EBinaryOp::Less
                || binaryExpr->Opcode == EBinaryOp::LessOrEqual
                || binaryExpr->Opcode == EBinaryOp::Greater
                || binaryExpr->Opcode == EBinaryOp::GreaterOrEqual
                || binaryExpr->Opcode == EBinaryOp::Equal) {

                if (typedLhsExpr.size() != typedRhsExpr.size()) {
                    THROW_ERROR_EXCEPTION("Expecting tuples of same size")
                        << TErrorAttribute("source", binaryExpr->Rhs->GetSource(querySourceString));
                }

                size_t keySize = typedLhsExpr.size();

                result.push_back(gen(0, keySize, binaryExpr->Opcode));            
            } else {
                if (typedLhsExpr.size() != 1) {
                    THROW_ERROR_EXCEPTION("Expecting scalar expression")
                        << TErrorAttribute("source", binaryExpr->Lhs->GetSource(querySourceString));
                }

                if (typedRhsExpr.size() != 1) {
                    THROW_ERROR_EXCEPTION("Expecting scalar expression")
                        << TErrorAttribute("source", binaryExpr->Rhs->GetSource(querySourceString));
                }

                result.push_back(makeBinaryExpr(binaryExpr->Opcode, typedLhsExpr.front(), typedRhsExpr.front()));
            }
        } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
            auto inExprOperands = buildTypedExpression(tableSchema, inExpr->Expr.Get(), groupProxy);

            size_t keySize = inExprOperands.size();

            auto caturedRows = captureRows(inExpr->Values, keySize);

            result.push_back(New<TInOpExpression>(
                inExpr->SourceLocation,
                inExprOperands,
                caturedRows));
        }

        return result;
    };

    return buildTypedExpression(tableSchema, expr, groupProxy);
};

static TQueryPtr PrepareQuery(
    NAst::TQuery& ast,
    const Stroka& querySourceString,
    i64 inputRowLimit,
    i64 outputRowLimit,
    const TTableSchema& tableSchema)
{
    auto query = New<TQuery>(inputRowLimit, outputRowLimit, TGuid::Create());
    query->TableSchema = tableSchema;

    std::set<Stroka> liveColumns;
    auto tableSchemaProxy = TTableSchemaProxy(query->TableSchema, &liveColumns);

    if (ast.WherePredicate) {

        auto typedPredicate = BuildTypedExpression(
            tableSchemaProxy,
            ast.WherePredicate.Get(),
            nullptr,
            querySourceString);

        if (typedPredicate.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", ast.WherePredicate->GetSource(querySourceString));
        }

        auto predicate = typedPredicate.front();

        CheckExpressionDepth(predicate);

        auto actualType = predicate->Type;
        EValueType expectedType(EValueType::Boolean);
        if (actualType != expectedType) {
            THROW_ERROR_EXCEPTION("WHERE-clause is not a boolean expression")
                << TErrorAttribute("actual_type", actualType)
                << TErrorAttribute("expected_type", expectedType);
        }

        query->Predicate = predicate;
    }

    TNullable<TGroupClauseProxy> groupClauseProxy;

    if (ast.GroupExprs) {
        TTableSchema tableSchema;

        TGroupClause groupClause;

        for (const auto& expr : ast.GroupExprs.Get()) {
            auto typedExprs = BuildTypedExpression(
                tableSchemaProxy,
                expr.first.Get(),
                nullptr,
                querySourceString);

            if (typedExprs.size() != 1) {
                THROW_ERROR_EXCEPTION("Expecting scalar expression")
                    << TErrorAttribute("source", expr.first->GetSource(querySourceString));
            }

            CheckExpressionDepth(typedExprs.front());
            groupClause.GroupItems.emplace_back(typedExprs.front(), expr.second);
            tableSchema.Columns().emplace_back(expr.second, typedExprs.front()->Type);
        }

        ValidateTableSchema(tableSchema);

        query->GroupClause = std::move(groupClause);

        groupClauseProxy.Emplace(tableSchemaProxy, query->GroupClause.Get());
        tableSchemaProxy = TTableSchemaProxy(tableSchema);
    }

    if (ast.SelectExprs) {
        TTableSchema tableSchema;

        TProjectClause projectClause;

        for (const auto& expr : ast.SelectExprs.Get()) {
            auto typedExprs = BuildTypedExpression(
                tableSchemaProxy,
                expr.first.Get(),
                groupClauseProxy.GetPtr(),
                querySourceString);

            if (typedExprs.size() != 1) {
                THROW_ERROR_EXCEPTION("Expecting scalar expression")
                    << TErrorAttribute("source", expr.first->GetSource(querySourceString));
            }

            CheckExpressionDepth(typedExprs.front());

            projectClause.Projections.emplace_back(typedExprs.front(), expr.second);
            tableSchema.Columns().emplace_back(expr.second, typedExprs.front()->Type);
        }

        ValidateTableSchema(tableSchema);

        query->ProjectClause = std::move(projectClause);

        groupClauseProxy.Reset();
        tableSchemaProxy = TTableSchemaProxy(tableSchema);
    }

    // Now we have planOperator and tableSchemaProxy

    // Prune references

    auto& columns = query->TableSchema.Columns();

    if (!tableSchemaProxy.LiveColumns) {
        columns.erase(
            std::remove_if(
                columns.begin(),
                columns.end(),
                [&liveColumns] (const TColumnSchema& columnSchema) {
                    return liveColumns.find(columnSchema.Name) == liveColumns.end();
                }),
            columns.end());
    }

    return query;
}

static void ParseYqlString(
    NAst::TAstHead* astHead,
    TRowBuffer* rowBuffer,
    const Stroka& source,
    NAst::TParser::token::yytokentype strayToken)
{
    NAst::TLexer lexer(source, strayToken);
    NAst::TParser parser(lexer, astHead, rowBuffer, source);

    int result = parser.parse();

    if (result != 0) {
        THROW_ERROR_EXCEPTION("Parse failure")
            << TErrorAttribute("source", source);
    }
}

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    i64 inputRowLimit,
    i64 outputRowLimit,
    TTimestamp timestamp)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>()};
    NAst::TRowBuffer rowBuffer;
    ParseYqlString(&astHead, &rowBuffer, source, NAst::TParser::token::StrayWillParseQuery);

    auto& ast = astHead.As<NAst::TQuery>();
    auto tablePath = ast.FromPath;

    LOG_DEBUG("Getting initial data split for %v", tablePath);
    // XXX(sandello): We have just one table at the moment.
    // Will put TParallelAwaiter here in case of multiple tables.

    auto dataSplitOrError = WaitFor(callbacks->GetInitialSplit(
        tablePath,
        timestamp));
    THROW_ERROR_EXCEPTION_IF_FAILED(
        dataSplitOrError,
        "Failed to get initial data split for table %v",
        tablePath);

    auto initialDataSplit = dataSplitOrError.Value();
    auto tableSchema = GetTableSchemaFromDataSplit(initialDataSplit);

    auto planFragment = New<TPlanFragment>(source);
    planFragment->NodeDirectory = New<TNodeDirectory>();

    auto query = PrepareQuery(ast, source, inputRowLimit, outputRowLimit, tableSchema);

    if (ast.Limit) {
        query->Limit = ast.Limit;
        planFragment->Ordered = true;
    }

    SetTableSchema(&initialDataSplit, query->TableSchema);
    query->KeyColumns = GetKeyColumnsFromDataSplit(initialDataSplit);

    planFragment->Query = query;
    planFragment->DataSplits.push_back(initialDataSplit);

    return planFragment;
}

TPlanFragmentPtr PrepareJobPlanFragment(
    const Stroka& source,
    const TTableSchema& tableSchema)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>()};
    NAst::TRowBuffer rowBuffer;
    ParseYqlString(&astHead, &rowBuffer, source, NAst::TParser::token::StrayWillParseJobQuery);

    auto& ast = astHead.As<NAst::TQuery>();

    if (ast.Limit) {
        THROW_ERROR_EXCEPTION("LIMIT is not supported in map-reduce queries");
    }

    if (ast.GroupExprs) {
        THROW_ERROR_EXCEPTION("GROUP BY is not supported in map-reduce queries");
    }

    auto planFragment = New<TPlanFragment>(source);
    auto unlimited = std::numeric_limits<i64>::max();
    auto query = PrepareQuery(ast, source, unlimited, unlimited, tableSchema);

    planFragment->Query = query;

    return planFragment;
}

TConstExpressionPtr PrepareExpression(
    const Stroka& source,
    const TTableSchema& tableSchema)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TNamedExpression>()};
    NAst::TRowBuffer rowBuffer;
    ParseYqlString(&astHead, &rowBuffer, source, NAst::TParser::token::StrayWillParseExpression);

    auto& expr = astHead.As<NAst::TNamedExpression>();

    std::set<Stroka> liveColumns;
    auto tableSchemaProxy = TTableSchemaProxy(tableSchema, &liveColumns);
    auto typedExprs = BuildTypedExpression(tableSchemaProxy, expr.first.Get(), nullptr, source);

    if (typedExprs.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            << TErrorAttribute("source", expr.first->GetSource(source));
    }

    return typedExprs.front();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExpression* serialized, const TConstExpressionPtr& original)
{
    serialized->set_type(static_cast<int>(original->Type));
    serialized->set_location_begin(original->SourceLocation.first);
    serialized->set_location_end(original->SourceLocation.second);

    if (auto literalExpr = original->As<TLiteralExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::Literal));
        auto* proto = serialized->MutableExtension(NProto::TLiteralExpression::literal_expression);
        auto value = TValue(literalExpr->Value);
        auto data = value.Data;

        switch (value.Type) {
            case EValueType::Int64: {
                proto->set_int64_value(data.Int64);
                break;
            }

            case EValueType::Uint64: {
                proto->set_uint64_value(data.Uint64);
                break;
            }
                
            case EValueType::Double: {
                proto->set_double_value(data.Double);
                break;
            }

            case EValueType::String: {
                proto->set_string_value(data.String, value.Length);
                break;
            }

            case EValueType::Boolean: {
                proto->set_boolean_value(data.Boolean);
                break;
            }

            default:
                YUNREACHABLE();
        }

    } else if (auto referenceExpr = original->As<TReferenceExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::Reference));
        auto* proto = serialized->MutableExtension(NProto::TReferenceExpression::reference_expression);
        proto->set_column_name(referenceExpr->ColumnName);
    } else if (auto functionExpr = original->As<TFunctionExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::Function));
        auto* proto = serialized->MutableExtension(NProto::TFunctionExpression::function_expression);
        proto->set_function_name(functionExpr->FunctionName);
        ToProto(proto->mutable_arguments(), functionExpr->Arguments);
    } else if (auto binaryOpExpr = original->As<TBinaryOpExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::BinaryOp));
        auto* proto = serialized->MutableExtension(NProto::TBinaryOpExpression::binary_op_expression);
        proto->set_opcode(static_cast<int>(binaryOpExpr->Opcode));
        ToProto(proto->mutable_lhs(), binaryOpExpr->Lhs);
        ToProto(proto->mutable_rhs(), binaryOpExpr->Rhs);
    } else if (auto inOpExpr = original->As<TInOpExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::InOp));
        auto* proto = serialized->MutableExtension(NProto::TInOpExpression::in_op_expression);
        ToProto(proto->mutable_arguments(), inOpExpr->Arguments);
        ToProto(proto->mutable_values(), inOpExpr->Values);
    } else {
        YUNREACHABLE();
    }
}

TExpressionPtr FromProto(const NProto::TExpression& serialized)
{
    auto kind = EExpressionKind(serialized.kind());
    auto type = EValueType(serialized.type());
    TSourceLocation sourceLocation(serialized.location_begin(), serialized.location_end());

    switch (kind) {
        case EExpressionKind::Literal: {
            auto typedResult = New<TLiteralExpression>(sourceLocation, type);
            auto data = serialized.GetExtension(NProto::TLiteralExpression::literal_expression);

            switch (type) {
                case EValueType::Int64: {
                    typedResult->Value = MakeUnversionedInt64Value(data.int64_value());
                    break;
                }

                case EValueType::Uint64: {
                    typedResult->Value = MakeUnversionedUint64Value(data.uint64_value());
                    break;
                }

                case EValueType::Double: {
                    typedResult->Value = MakeUnversionedDoubleValue(data.double_value());
                    break;
                }

                case EValueType::String: {
                    typedResult->Value = MakeUnversionedStringValue(data.string_value());
                    break;
                }

                case EValueType::Boolean: {
                    typedResult->Value = MakeUnversionedBooleanValue(data.boolean_value());
                    break;
                }

                default:
                    YUNREACHABLE();
            }

            return typedResult;
        }

        case EExpressionKind::Reference: {
            auto typedResult = New<TReferenceExpression>(sourceLocation, type);
            auto data = serialized.GetExtension(NProto::TReferenceExpression::reference_expression);
            typedResult->ColumnName = data.column_name();
            return typedResult;
        }

        case EExpressionKind::Function: {
            auto typedResult = New<TFunctionExpression>(sourceLocation, type);
            auto data = serialized.GetExtension(NProto::TFunctionExpression::function_expression);
            typedResult->FunctionName = data.function_name();
            typedResult->Arguments.reserve(data.arguments_size());
            for (int i = 0; i < data.arguments_size(); ++i) {
                typedResult->Arguments.push_back(FromProto(data.arguments(i)));
            }
            return typedResult;
        }

        case EExpressionKind::BinaryOp: {
            auto typedResult = New<TBinaryOpExpression>(sourceLocation, type);
            auto data = serialized.GetExtension(NProto::TBinaryOpExpression::binary_op_expression);
            typedResult->Opcode = EBinaryOp(data.opcode());
            typedResult->Lhs = FromProto(data.lhs());
            typedResult->Rhs = FromProto(data.rhs());
            return typedResult;
        } 

        case EExpressionKind::InOp: {
            auto typedResult = New<TInOpExpression>(sourceLocation, type);
            auto data = serialized.GetExtension(NProto::TInOpExpression::in_op_expression);
            typedResult->Arguments.reserve(data.arguments_size());
            for (int i = 0; i < data.arguments_size(); ++i) {
                typedResult->Arguments.push_back(FromProto(data.arguments(i)));
            }

            typedResult->Values = FromProto<TOwningRow>(data.values());

            return typedResult;
        } 
    }

    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNamedItem* serialized, const TNamedItem& original)
{
    ToProto(serialized->mutable_expression(), original.Expression);
    ToProto(serialized->mutable_name(), original.Name);
}

void ToProto(NProto::TAggregateItem* serialized, const TAggregateItem& original)
{
    ToProto(serialized->mutable_expression(), original.Expression);
    serialized->set_aggregate_function(static_cast<int>(original.AggregateFunction));
    ToProto(serialized->mutable_name(), original.Name);
}

void ToProto(NProto::TGroupClause* proto, const TGroupClause& original)
{
    ToProto(proto->mutable_group_items(), original.GroupItems);
    ToProto(proto->mutable_aggregate_items(), original.AggregateItems);
}

void ToProto(NProto::TProjectClause* proto, const TProjectClause& original)
{
    ToProto(proto->mutable_projections(), original.Projections);
}

void ToProto(NProto::TQuery* proto, const TConstQueryPtr& original)
{
    proto->set_input_row_limit(original->GetInputRowLimit());
    proto->set_output_row_limit(original->GetOutputRowLimit());

    ToProto(proto->mutable_id(), original->GetId());

    proto->set_limit(original->Limit);

    ToProto(proto->mutable_table_schema(), original->TableSchema);
    ToProto(proto->mutable_key_columns(), original->KeyColumns);

    if (original->Predicate) {
        ToProto(proto->mutable_predicate(), original->Predicate);
    }

    if (original->GroupClause) {
        ToProto(proto->mutable_group_clause(), original->GroupClause.Get());
    }
    
    if (original->ProjectClause) {
        ToProto(proto->mutable_project_clause(), original->ProjectClause.Get());
    }
}

TNamedItem FromProto(const NProto::TNamedItem& serialized)
{
    return TNamedItem(
        FromProto(serialized.expression()),
        serialized.name());
}

TAggregateItem FromProto(const NProto::TAggregateItem& serialized)
{
    return TAggregateItem(
        FromProto(serialized.expression()),
        EAggregateFunctions(serialized.aggregate_function()),
        serialized.name());
}

TGroupClause FromProto(const NProto::TGroupClause& serialized)
{
    TGroupClause result;
    result.GroupItems.reserve(serialized.group_items_size());
    for (int i = 0; i < serialized.group_items_size(); ++i) {
        result.GroupItems.push_back(FromProto(serialized.group_items(i)));
    }
    result.AggregateItems.reserve(serialized.aggregate_items_size());
    for (int i = 0; i < serialized.aggregate_items_size(); ++i) {
        result.AggregateItems.push_back(FromProto(serialized.aggregate_items(i)));
    }

    return result;
}

TProjectClause FromProto(const NProto::TProjectClause& serialized)
{
    TProjectClause result;

    result.Projections.reserve(serialized.projections_size());
    for (int i = 0; i < serialized.projections_size(); ++i) {
        result.Projections.push_back(FromProto(serialized.projections(i)));
    }

    return result;
}

TQueryPtr FromProto(const NProto::TQuery& serialized)
{
    auto query = New<TQuery>(
        serialized.input_row_limit(),
        serialized.output_row_limit(),
        NYT::FromProto<TGuid>(serialized.id()));

    query->Limit = serialized.limit();

    FromProto(&query->TableSchema, serialized.table_schema());
    FromProto(&query->KeyColumns, serialized.key_columns());

    if (serialized.has_predicate()) {
        query->Predicate = FromProto(serialized.predicate());
    }

    if (serialized.has_group_clause()) {
        query->GroupClause = FromProto(serialized.group_clause());       
    }

    if (serialized.has_project_clause()) {
        query->ProjectClause = FromProto(serialized.project_clause());       
    }

    return query;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TPlanFragment* proto, const TConstPlanFragmentPtr& fragment)
{
    ToProto(proto->mutable_query(), fragment->Query);
    ToProto(proto->mutable_data_split(), fragment->DataSplits);
    proto->set_ordered(fragment->Ordered);
    
    proto->set_source(fragment->GetSource());
}

TPlanFragmentPtr FromProto(const NProto::TPlanFragment& serialized)
{
    auto result = New<TPlanFragment>(
        serialized.source());

    result->NodeDirectory = New<TNodeDirectory>();
    result->Query = FromProto(serialized.query());
    result->Ordered = serialized.ordered();

    result->DataSplits.reserve(serialized.data_split_size());
    for (int i = 0; i < serialized.data_split_size(); ++i) {
        TDataSplit dataSplit;
        FromProto(&dataSplit, serialized.data_split(i));
        result->DataSplits.push_back(dataSplit);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
