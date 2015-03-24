#include "stdafx.h"

#include "function_registry.h"
#include "plan_fragment.h"
#include "private.h"
#include "helpers.h"
#include "plan_helpers.h"
#include "lexer.h"
#include "parser.hpp"
#include "callbacks.h"
#include "functions.h"

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

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

Stroka InferName(TConstExpressionPtr expr)
{
    bool newTuple = true;
    auto comma = [&] {
        bool isNewTuple = newTuple;
        newTuple = false;
        return Stroka(isNewTuple ? "" : ", ");
    };
    auto canOmitParenthesis = [] (TConstExpressionPtr expr) {
        return
            expr->As<TLiteralExpression>() ||
            expr->As<TReferenceExpression>() ||
            expr->As<TFunctionExpression>();
    };

    if (!expr) {
        return Stroka();
    } else if (auto literalExpr = expr->As<TLiteralExpression>()) {
        return ToString(static_cast<TUnversionedValue>(literalExpr->Value));
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        return referenceExpr->ColumnName;
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        auto str = functionExpr->FunctionName + "(";
        for (const auto& argument : functionExpr->Arguments) {
            str += comma() + InferName(argument);
        }
        return str + ")";
    } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
        auto rhsName = InferName(unaryOp->Operand);
        if (!canOmitParenthesis(unaryOp->Operand)) {
            rhsName = "(" + rhsName + ")";
        }
        return Stroka() + GetUnaryOpcodeLexeme(unaryOp->Opcode) + rhsName;
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
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
        for (const auto& namedItem : query->ProjectClause->Projections) {
            str += comma() + InferName(namedItem.Expression) + " AS " + namedItem.Name;
        }
    } else {
        str += "*";
    }

    if (query->GroupClause) {
        str += block() + "GROUP BY ";
        newTuple = true;
        for (const auto& namedItem : query->GroupClause->GroupItems) {
            str += comma() + InferName(namedItem.Expression) + " AS " + namedItem.Name;
        }
    }

    if (query->WhereClause) {
        str += block() + "WHERE " + InferName(query->WhereClause);
    }

    return str;
}

Stroka TExpression::GetName() const
{
    return Stroka();
}

EValueType InferUnaryExprType(EUnaryOp opCode, EValueType operandType, const TStringBuf& source)
{
    switch (opCode) {
        case EUnaryOp::Plus:
        case EUnaryOp::Minus:
            if (!IsArithmeticType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires either integral or floating-point operand",
                    source)
                    << TErrorAttribute("operand_type", ToString(operandType));
            }
            return operandType;

        default:
            YUNREACHABLE();
    }
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

EValueType InferFunctionExprType(
    Stroka functionName,
    const std::vector<EValueType>& argTypes,
    const TStringBuf& source,
    const TFunctionRegistryPtr functionRegistry)
{
    if (!functionRegistry->IsRegistered(functionName)) {
        THROW_ERROR_EXCEPTION(
            "Unknown function in expression %Qv",
            source)
            << TErrorAttribute("function_name", functionName);
    }

    return functionRegistry->GetFunction(functionName).InferResultType(argTypes, source);
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
    } else if (auto unaryOpExpr = op->As<TUnaryOpExpression>()) {
        CheckExpressionDepth(unaryOpExpr->Operand, depth + 1);
        return;
    } else if (auto binaryOpExpr = op->As<TBinaryOpExpression>()) {
        CheckExpressionDepth(binaryOpExpr->Lhs, depth + 1);
        CheckExpressionDepth(binaryOpExpr->Rhs, depth + 1);
        return;
    }
    YUNREACHABLE();
};

DECLARE_REFCOUNTED_STRUCT(ISchemaProxy)

struct ISchemaProxy
    : public TIntrinsicRefCounted
{
    TTableSchema* TableSchema;

    explicit ISchemaProxy(TTableSchema* tableSchema)
        : TableSchema(tableSchema)
    { }

    // NOTE: result must be used before next call
    virtual const TColumnSchema* GetColumnPtr(const TStringBuf& name) = 0;
    virtual const TColumnSchema* GetAggregateColumnPtr(
        EAggregateFunction aggregateFunction,
        const NAst::TExpression* arguments,
        Stroka subexprName,
        Stroka source,
        const TFunctionRegistryPtr functionRegistry)
    {
        THROW_ERROR_EXCEPTION(
            "Misuse of aggregate function %v",
            aggregateFunction);
    }

    virtual void Finish()
    { }

    static const TColumnSchema* AddColumn(TTableSchema* tableSchema, const TColumnSchema& column)
    {
        tableSchema->Columns().push_back(column);
        return &tableSchema->Columns().back();
    }

    static TNullable<EAggregateFunction> GetAggregate(TStringBuf functionName)
    {
        Stroka name(functionName);
        name.to_lower();

        TNullable<EAggregateFunction> result;

        if (name == "sum") {
            result.Assign(EAggregateFunction::Sum);
        } else if (name == "min") {
            result.Assign(EAggregateFunction::Min);
        } else if (name == "max") {
            result.Assign(EAggregateFunction::Max);
        }

        return result;
    };

    static std::vector<TOwningRow> CaptureRows(const NAst::TValueTupleList& literalTuples, size_t keySize)
    {
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

    std::vector<TConstExpressionPtr> BuildTypedExpression(
        const NAst::TExpression* expr,
        const Stroka& source,
        const TFunctionRegistryPtr functionRegistry)
    {
        std::vector<TConstExpressionPtr> result;
        if (auto commaExpr = expr->As<NAst::TCommaExpression>()) {
            auto typedLhsExprs = BuildTypedExpression(commaExpr->Lhs.Get(), source, functionRegistry);
            auto typedRhsExprs = BuildTypedExpression(commaExpr->Rhs.Get(), source, functionRegistry);

            result.insert(result.end(), typedLhsExprs.begin(), typedLhsExprs.end());
            result.insert(result.end(), typedRhsExprs.begin(), typedRhsExprs.end());
        } else if (auto literalExpr = expr->As<NAst::TLiteralExpression>()) {
            result.push_back(New<TLiteralExpression>(
                literalExpr->SourceLocation,
                EValueType(literalExpr->Value.Type),
                literalExpr->Value));
        } else if (auto referenceExpr = expr->As<NAst::TReferenceExpression>()) {
            const auto* column = GetColumnPtr(referenceExpr->ColumnName);

            if (!column) {
                THROW_ERROR_EXCEPTION("Undefined reference %Qv", referenceExpr->ColumnName);
            }

            result.push_back(New<TReferenceExpression>(
                referenceExpr->SourceLocation,
                column->Type,
                referenceExpr->ColumnName));
        } else if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
            auto functionName = functionExpr->FunctionName;
            auto aggregateFunction = GetAggregate(functionName);

            if (aggregateFunction) {
                auto subexprName = InferName(functionExpr);

                try {
                    const auto* aggregateColumn = GetAggregateColumnPtr(
                        aggregateFunction.Get(),
                        functionExpr->Arguments.Get(),
                        subexprName,
                        source,
                        functionRegistry);

                    result.push_back(New<TReferenceExpression>(
                        NullSourceLocation,
                        aggregateColumn->Type,
                        aggregateColumn->Name));

                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Failed creating aggregate")
                        << TErrorAttribute("source", functionExpr->GetSource(source))
                        << ex;
                }

            } else {
                std::vector<EValueType> types;

                auto typedOperands = BuildTypedExpression(functionExpr->Arguments.Get(), source, functionRegistry);

                for (const auto& typedOperand : typedOperands) {
                    types.push_back(typedOperand->Type);
                }

                result.push_back(New<TFunctionExpression>(
                    functionExpr->SourceLocation,
                    InferFunctionExprType(functionName, types, functionExpr->GetSource(source), functionRegistry),
                    functionName,
                    typedOperands));
            }
        } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
            auto typedOperandExpr = BuildTypedExpression(unaryExpr->Operand.Get(), source, functionRegistry);

            for (const auto& operand : typedOperandExpr) {
                if (auto literalExpr = operand->As<TLiteralExpression>()) {
                    if (unaryExpr->Opcode == EUnaryOp::Plus) {
                        result.push_back(literalExpr);
                        continue;
                    } else if (unaryExpr->Opcode == EUnaryOp::Minus) {
                        TUnversionedValue value = literalExpr->Value;
                        switch (value.Type) {
                            case EValueType::Int64:
                                value.Data.Int64 = -value.Data.Int64;
                                break;
                            case EValueType::Uint64:
                                value.Data.Uint64 = -value.Data.Uint64;
                                break;
                            case EValueType::Double:
                                value.Data.Double = -value.Data.Double;
                                break;
                            default:
                                YUNREACHABLE();
                        }
                        result.push_back(New<TLiteralExpression>(
                            literalExpr->SourceLocation,
                            EValueType(value.Type),
                            value));
                        continue;
                    }
                }
                result.push_back(New<TUnaryOpExpression>(
                    unaryExpr->SourceLocation,
                    InferUnaryExprType(
                        unaryExpr->Opcode,
                        operand->Type,
                        unaryExpr->GetSource(source)),
                    unaryExpr->Opcode,
                    operand));
            }
        } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
            auto typedLhsExpr = BuildTypedExpression(binaryExpr->Lhs.Get(), source, functionRegistry);
            auto typedRhsExpr = BuildTypedExpression(binaryExpr->Rhs.Get(), source, functionRegistry);

            auto makeBinaryExpr = [&] (EBinaryOp op, const TConstExpressionPtr& lhs, const TConstExpressionPtr& rhs) {
                return New<TBinaryOpExpression>(
                    binaryExpr->SourceLocation,
                    InferBinaryExprType(
                        op,
                        lhs->Type,
                        rhs->Type,
                        binaryExpr->GetSource(source)),
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
                        << TErrorAttribute("source", binaryExpr->Rhs->GetSource(source));
                }

                size_t keySize = typedLhsExpr.size();

                result.push_back(gen(0, keySize, binaryExpr->Opcode));            
            } else {
                if (typedLhsExpr.size() != 1) {
                    THROW_ERROR_EXCEPTION("Expecting scalar expression")
                        << TErrorAttribute("source", binaryExpr->Lhs->GetSource(source));
                }

                if (typedRhsExpr.size() != 1) {
                    THROW_ERROR_EXCEPTION("Expecting scalar expression")
                        << TErrorAttribute("source", binaryExpr->Rhs->GetSource(source));
                }

                result.push_back(makeBinaryExpr(binaryExpr->Opcode, typedLhsExpr.front(), typedRhsExpr.front()));
            }
        } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
            auto inExprOperands = BuildTypedExpression(inExpr->Expr.Get(), source, functionRegistry);

            size_t keySize = inExprOperands.size();

            auto caturedRows = CaptureRows(inExpr->Values, keySize);

            result.push_back(New<TInOpExpression>(
                inExpr->SourceLocation,
                inExprOperands,
                caturedRows));
        }

        return result;
    }

};

DEFINE_REFCOUNTED_TYPE(ISchemaProxy)

struct TSimpleSchemaProxy
    : public ISchemaProxy
{
    TNullable<TTableSchema> SourceTableSchema;

    explicit TSimpleSchemaProxy(
        TTableSchema* tableSchema)
        : ISchemaProxy(tableSchema)
    { }
    
    TSimpleSchemaProxy(
        TTableSchema* tableSchema,
        const TTableSchema& sourceTableSchema,
        size_t keyColumnCount = 0)
        : ISchemaProxy(tableSchema)
        , SourceTableSchema(sourceTableSchema)
    {
        const auto& columns = sourceTableSchema.Columns();
        size_t count = std::min(sourceTableSchema.HasComputedColumns() ? keyColumnCount : 0, columns.size());
        for (size_t i = 0; i < count; ++i) {
            AddColumn(TableSchema, columns[i]);
        }
    }

    virtual void Finish()
    {
        if (SourceTableSchema) {
            for (const auto& column : SourceTableSchema->Columns()) {
                if (!TableSchema->FindColumn(column.Name)) {
                    AddColumn(TableSchema, column);
                }
            }
        }
    }

    virtual const TColumnSchema* GetColumnPtr(const TStringBuf& name) override
    {
        const auto* column = TableSchema->FindColumn(name);

        !column
            && SourceTableSchema
            && (column = SourceTableSchema->FindColumn(name))
            && (column = AddColumn(TableSchema, *column));       

        return column;
    }
};

struct TJoinSchemaProxy
    : public ISchemaProxy
{
    ISchemaProxyPtr Self;
    ISchemaProxyPtr Foreign;

    TJoinSchemaProxy(
        TTableSchema* tableSchema,
        ISchemaProxyPtr self,
        ISchemaProxyPtr foreign)
        : ISchemaProxy(tableSchema)
        , Self(self)
        , Foreign(foreign)
    { }

    virtual void Finish()
    {
        Self->Finish();
        Foreign->Finish();

        for (const auto& column : Self->TableSchema->Columns()) {
            if (!TableSchema->FindColumn(column.Name)) {
                AddColumn(TableSchema, column);
            }
        }

        for (const auto& column : Foreign->TableSchema->Columns()) {
            if (!TableSchema->FindColumn(column.Name)) {
                AddColumn(TableSchema, column);
            }
        }
    }

    virtual const TColumnSchema* GetColumnPtr(const TStringBuf& name) override
    {
        const TColumnSchema* column = TableSchema->FindColumn(name);

        if (!column) {
            if (column = Self->GetColumnPtr(name)) {
                if (Foreign->GetColumnPtr(name)) {
                    THROW_ERROR_EXCEPTION("Column %Qv collision", name);
                } else {
                    column = AddColumn(TableSchema, *column);
                }
            } else if (column = Foreign->GetColumnPtr(name)) {
                column = AddColumn(TableSchema, *column);
            }
        }

        return column;
    }

};

struct TGroupSchemaProxy
    : public ISchemaProxy
{
    ISchemaProxyPtr Base;
    TAggregateItemList* AggregateItems;

    TGroupSchemaProxy(
        TTableSchema* tableSchema,
        ISchemaProxyPtr base,
        TAggregateItemList* aggregateItems)
        : ISchemaProxy(tableSchema)
        , Base(base)
        , AggregateItems(aggregateItems)
    { }

    virtual const TColumnSchema* GetColumnPtr(const TStringBuf& name) override
    {
        return TableSchema->FindColumn(name);
    }

    virtual const TColumnSchema* GetAggregateColumnPtr(
        EAggregateFunction aggregateFunction,
        const NAst::TExpression* arguments,
        Stroka subexprName,
        Stroka source,
        const TFunctionRegistryPtr functionRegistry)
    {
        const TColumnSchema* aggregateColumn = TableSchema->FindColumn(subexprName);

        if (!aggregateColumn) {
            auto typedOperands = Base->BuildTypedExpression(
                arguments,
                source,
                functionRegistry);

            if (typedOperands.size() != 1) {
                THROW_ERROR_EXCEPTION(
                    "Aggregate function %Qv must have exactly one argument",
                    aggregateFunction);
            }

            CheckExpressionDepth(typedOperands.front());

            AggregateItems->emplace_back(
                typedOperands.front(),
                aggregateFunction,
                subexprName);

            aggregateColumn = AddColumn(TableSchema, TColumnSchema(subexprName, typedOperands.front()->Type));
        }

        return aggregateColumn;
    }
};

TConstExpressionPtr BuildWhereClause(
    NAst::TExpressionPtr& expressionAst,
    const Stroka& source,
    const ISchemaProxyPtr& schemaProxy,
    const TFunctionRegistryPtr functionRegistry)
{
    auto typedPredicate = schemaProxy->BuildTypedExpression(
        expressionAst.Get(),
        source,
        functionRegistry);

    if (typedPredicate.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            << TErrorAttribute("source", expressionAst->GetSource(source));
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

    return predicate;
}

TConstGroupClausePtr BuildGroupClause(
    NAst::TNullableNamedExprs& expressionsAst,
    const Stroka& source,
    ISchemaProxyPtr& schemaProxy,
    const TFunctionRegistryPtr functionRegistry)
{
    auto groupClause = New<TGroupClause>();
    TTableSchema& tableSchema = groupClause->GroupedTableSchema;

    for (const auto& expr : expressionsAst.Get()) {
        auto typedExprs = schemaProxy->BuildTypedExpression(
            expr.first.Get(),
            source,
            functionRegistry);

        if (typedExprs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", expr.first->GetSource(source));
        }

        CheckExpressionDepth(typedExprs.front());
        groupClause->GroupItems.emplace_back(typedExprs.front(), expr.second);
        tableSchema.Columns().emplace_back(expr.second, typedExprs.front()->Type);
    }

    ValidateTableSchema(tableSchema);
    schemaProxy = New<TGroupSchemaProxy>(&tableSchema, std::move(schemaProxy), &groupClause->AggregateItems);

    return groupClause;
}

TConstProjectClausePtr BuildProjectClause(
    NAst::TNullableNamedExprs& expressionsAst,
    const Stroka& source,
    ISchemaProxyPtr& schemaProxy,
    const TFunctionRegistryPtr functionRegistry)
{
    auto projectClause = New<TProjectClause>();

    for (const auto& expr : expressionsAst.Get()) {
        auto typedExprs = schemaProxy->BuildTypedExpression(
            expr.first.Get(),
            source,
            functionRegistry);

        if (typedExprs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", expr.first->GetSource(source));
        }

        CheckExpressionDepth(typedExprs.front());

        projectClause->AddProjection(typedExprs.front(), expr.second);

    }

    ValidateTableSchema(projectClause->ProjectTableSchema);
    schemaProxy = New<TSimpleSchemaProxy>(&projectClause->ProjectTableSchema);

    return projectClause;
}

TQueryPtr PrepareQuery(
    NAst::TQuery& ast,
    const Stroka& source,
    i64 inputRowLimit,
    i64 outputRowLimit,
    const TTableSchema& tableSchema,
    const TFunctionRegistryPtr functionRegistry)
{
    auto query = New<TQuery>(inputRowLimit, outputRowLimit, TGuid::Create());
    ISchemaProxyPtr schemaProxy = New<TSimpleSchemaProxy>(&query->TableSchema, tableSchema);

    if (ast.WherePredicate) {
        query->WhereClause = BuildWhereClause(ast.WherePredicate, source, schemaProxy, functionRegistry);
    }

    if (ast.GroupExprs) {
        query->GroupClause = BuildGroupClause(ast.GroupExprs, source, schemaProxy, functionRegistry);
    }

    if (ast.SelectExprs) {
        query->ProjectClause = BuildProjectClause(ast.SelectExprs, source, schemaProxy, functionRegistry);
    }

    schemaProxy->Finish();

    return query;
}

void ParseYqlString(
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
    const TFunctionRegistryPtr functionRegistry,
    i64 inputRowLimit,
    i64 outputRowLimit,
    TTimestamp timestamp)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>()};
    NAst::TRowBuffer rowBuffer;
    ParseYqlString(&astHead, &rowBuffer, source, NAst::TParser::token::StrayWillParseQuery);

    auto& ast = astHead.As<NAst::TQuery>();
    
    TDataSplit selfDataSplit;
    TDataSplit foreignDataSplit;

    auto query = New<TQuery>(inputRowLimit, outputRowLimit, TGuid::Create());
    ISchemaProxyPtr schemaProxy;
    
    if (auto simpleSource = ast.Source->As<NAst::TSimpleSource>()) {
        LOG_DEBUG("Getting initial data split for %v", simpleSource->Path);

        selfDataSplit = WaitFor(callbacks->GetInitialSplit(simpleSource->Path, timestamp)).ValueOrThrow();
        auto tableSchema = GetTableSchemaFromDataSplit(selfDataSplit);
        auto keyColumns = GetKeyColumnsFromDataSplit(selfDataSplit);

        query->KeyColumns = keyColumns;
        schemaProxy = New<TSimpleSchemaProxy>(&query->TableSchema, tableSchema, keyColumns.size());
    } else if (auto joinSource = ast.Source->As<NAst::TJoinSource>()) {
        LOG_DEBUG("Getting initial data split for %v and %v", joinSource->LeftPath, joinSource->RightPath);

        std::vector<TFuture<TDataSplit>> splitFutures({
            callbacks->GetInitialSplit(joinSource->LeftPath, timestamp),
            callbacks->GetInitialSplit(joinSource->RightPath, timestamp)
        });

        auto splits = WaitFor(Combine<TDataSplit>(splitFutures)).ValueOrThrow();

        selfDataSplit = splits[0];
        auto selfTableSchema = GetTableSchemaFromDataSplit(selfDataSplit);
        auto selfKeyColumns = GetKeyColumnsFromDataSplit(selfDataSplit);

        foreignDataSplit = splits[1];
        auto foreignTableSchema = GetTableSchemaFromDataSplit(foreignDataSplit);
        auto foreignKeyColumns = GetKeyColumnsFromDataSplit(foreignDataSplit);

        auto joinClause = New<TJoinClause>();
        
        const auto& joinFields = joinSource->Fields;
        joinClause->JoinColumns = joinFields;

        auto selfSourceProxy = New<TSimpleSchemaProxy>(&query->TableSchema, selfTableSchema, selfKeyColumns.size());
        auto foreignSourceProxy = New<TSimpleSchemaProxy>(&joinClause->ForeignTableSchema, foreignTableSchema, foreignKeyColumns.size());
        // Merge columns.
        for (const auto& name : joinFields) {
            const auto* selfColumn = selfSourceProxy->GetColumnPtr(name);
            const auto* foreignColumn = foreignSourceProxy->GetColumnPtr(name);

            if (!selfColumn || !foreignColumn) {
                THROW_ERROR_EXCEPTION("Column %Qv not found", name);
            }

            if (selfColumn->Type != foreignColumn->Type) {
                THROW_ERROR_EXCEPTION("Column type %Qv mismatch", name)
                    << TErrorAttribute("self_type", selfColumn->Type)
                    << TErrorAttribute("foreign_type", foreignColumn->Type);
            }

            joinClause->JoinedTableSchema.Columns().push_back(*selfColumn);
        }

        schemaProxy = New<TJoinSchemaProxy>(
            &joinClause->JoinedTableSchema, 
            selfSourceProxy,
            foreignSourceProxy);
        
        query->KeyColumns = selfKeyColumns;
        joinClause->ForeignKeyColumns = foreignKeyColumns;
        query->JoinClause = std::move(joinClause);
    } else {
        YUNREACHABLE();
    }
    
    if (ast.WherePredicate) {
        query->WhereClause = BuildWhereClause(ast.WherePredicate, source, schemaProxy, functionRegistry);
    }

    if (ast.GroupExprs) {
        query->GroupClause = BuildGroupClause(ast.GroupExprs, source, schemaProxy, functionRegistry);
    }

    if (ast.SelectExprs) {
        query->ProjectClause = BuildProjectClause(ast.SelectExprs, source, schemaProxy, functionRegistry);
    }

    schemaProxy->Finish();

    auto planFragment = New<TPlanFragment>(source);
    planFragment->NodeDirectory = New<TNodeDirectory>();
    
    if (ast.Limit) {
        query->Limit = ast.Limit;
        planFragment->Ordered = true;
    }

    planFragment->Query = query;

    planFragment->Timestamp = timestamp;

    planFragment->DataSources.push_back({
        GetObjectIdFromDataSplit(selfDataSplit),
        GetBothBoundsFromDataSplit(selfDataSplit)});

    if (auto joinClause = query->JoinClause.Get()) {
        planFragment->ForeignDataSource = {
            GetObjectIdFromDataSplit(foreignDataSplit),
            GetBothBoundsFromDataSplit(foreignDataSplit)};
    }
    
    return planFragment;
}

TPlanFragmentPtr PrepareJobPlanFragment(
    const Stroka& source,
    const TTableSchema& tableSchema,
    const TFunctionRegistryPtr functionRegistry)
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

    auto query = PrepareQuery(ast, source, unlimited, unlimited, tableSchema, functionRegistry);

    planFragment->Query = query;

    return planFragment;
}

TConstExpressionPtr PrepareExpression(
    const Stroka& source,
    TTableSchema tableSchema,
    const TFunctionRegistryPtr functionRegistry)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TNamedExpression>()};
    NAst::TRowBuffer rowBuffer;
    ParseYqlString(&astHead, &rowBuffer, source, NAst::TParser::token::StrayWillParseExpression);

    auto& expr = astHead.As<NAst::TNamedExpression>();

    auto schemaProxy = New<TSimpleSchemaProxy>(&tableSchema);

    auto typedExprs = schemaProxy->BuildTypedExpression(expr.first.Get(), source, functionRegistry);

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
    } else if (auto unaryOpExpr = original->As<TUnaryOpExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::UnaryOp));
        auto* proto = serialized->MutableExtension(NProto::TUnaryOpExpression::unary_op_expression);
        proto->set_opcode(static_cast<int>(unaryOpExpr->Opcode));
        ToProto(proto->mutable_operand(), unaryOpExpr->Operand);
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

        case EExpressionKind::UnaryOp: {
            auto typedResult = New<TUnaryOpExpression>(sourceLocation, type);
            auto data = serialized.GetExtension(NProto::TUnaryOpExpression::unary_op_expression);
            typedResult->Opcode = EUnaryOp(data.opcode());
            typedResult->Operand = FromProto(data.operand());
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

void ToProto(NProto::TGroupClause* proto, const TConstGroupClausePtr& original)
{
    ToProto(proto->mutable_group_items(), original->GroupItems);
    ToProto(proto->mutable_aggregate_items(), original->AggregateItems);
}

void ToProto(NProto::TProjectClause* proto, const TConstProjectClausePtr& original)
{
    ToProto(proto->mutable_projections(), original->Projections);
}

void ToProto(NProto::TJoinClause* proto, const TConstJoinClausePtr& original)
{
    ToProto(proto->mutable_join_columns(), original->JoinColumns);
    ToProto(proto->mutable_joined_table_schema(), original->JoinedTableSchema);
    ToProto(proto->mutable_foreign_table_schema(), original->ForeignTableSchema);
    ToProto(proto->mutable_foreign_key_columns(), original->ForeignKeyColumns);
}

void ToProto(NProto::TQuery* proto, const TConstQueryPtr& original)
{
    proto->set_input_row_limit(original->InputRowLimit);
    proto->set_output_row_limit(original->OutputRowLimit);

    ToProto(proto->mutable_id(), original->Id);

    proto->set_limit(original->Limit);
    ToProto(proto->mutable_table_schema(), original->TableSchema);
    ToProto(proto->mutable_key_columns(), original->KeyColumns);

    if (original->JoinClause) {
        ToProto(proto->mutable_join_clause(), original->JoinClause);
    }

    if (original->WhereClause) {
        ToProto(proto->mutable_predicate(), original->WhereClause);
    }

    if (original->GroupClause) {
        ToProto(proto->mutable_group_clause(), original->GroupClause);
    }
    
    if (original->ProjectClause) {
        ToProto(proto->mutable_project_clause(), original->ProjectClause);
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
        EAggregateFunction(serialized.aggregate_function()),
        serialized.name());
}

TGroupClausePtr FromProto(const NProto::TGroupClause& serialized)
{
    auto result = New<TGroupClause>();
    result->GroupItems.reserve(serialized.group_items_size());
    for (int i = 0; i < serialized.group_items_size(); ++i) {
        result->AddGroupItem(FromProto(serialized.group_items(i)));
    }
    result->AggregateItems.reserve(serialized.aggregate_items_size());
    for (int i = 0; i < serialized.aggregate_items_size(); ++i) {
        result->AddAggregateItem(FromProto(serialized.aggregate_items(i)));
    }

    return result;
}

TProjectClausePtr FromProto(const NProto::TProjectClause& serialized)
{
    auto result = New<TProjectClause>();

    result->Projections.reserve(serialized.projections_size());
    for (int i = 0; i < serialized.projections_size(); ++i) {
        result->AddProjection(FromProto(serialized.projections(i)));
    }

    return result;
}

TJoinClausePtr FromProto(const NProto::TJoinClause& serialized)
{
    auto result = New<TJoinClause>();

    result->JoinColumns.reserve(serialized.join_columns_size());
    for (int i = 0; i < serialized.join_columns_size(); ++i) {
        result->JoinColumns.push_back(serialized.join_columns(i));
    }

    FromProto(&result->JoinedTableSchema, serialized.joined_table_schema());
    FromProto(&result->ForeignTableSchema, serialized.foreign_table_schema());
    FromProto(&result->ForeignKeyColumns, serialized.foreign_key_columns());

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

    if (serialized.has_join_clause()) {
        query->JoinClause = FromProto(serialized.join_clause());
    }

    if (serialized.has_predicate()) {
        query->WhereClause = FromProto(serialized.predicate());
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

void ToProto(NProto::TDataSource* proto, const TDataSource& dataSource)
{
    ToProto(proto->mutable_id(), dataSource.Id);
    ToProto(proto->mutable_lower_bound(), dataSource.Range.first);
    ToProto(proto->mutable_upper_bound(), dataSource.Range.second);
}

TDataSource FromProto(const NProto::TDataSource& serialized)
{
    TDataSource dataSource;

    FromProto(&dataSource.Id, serialized.id());
    FromProto(&dataSource.Range.first, serialized.lower_bound());
    FromProto(&dataSource.Range.second, serialized.upper_bound());

    return dataSource;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TPlanFragment* proto, const TConstPlanFragmentPtr& fragment)
{
    ToProto(proto->mutable_query(), fragment->Query);
    ToProto(proto->mutable_data_sources(), fragment->DataSources);
    ToProto(proto->mutable_foreign_data_source(), fragment->ForeignDataSource);
    proto->set_ordered(fragment->Ordered);
    proto->set_verbose_logging(fragment->VerboseLogging);
    
    proto->set_source(fragment->Source);
    proto->set_timestamp(fragment->Timestamp);
}

TPlanFragmentPtr FromProto(const NProto::TPlanFragment& serialized)
{
    auto result = New<TPlanFragment>(
        serialized.source());

    result->NodeDirectory = New<TNodeDirectory>();
    result->Query = FromProto(serialized.query());
    result->Ordered = serialized.ordered();
    result->VerboseLogging = serialized.verbose_logging();
    result->Timestamp = serialized.timestamp();

    result->DataSources.reserve(serialized.data_sources_size());
    for (int i = 0; i < serialized.data_sources_size(); ++i) {
        result->DataSources.push_back(FromProto(serialized.data_sources(i)));
    }

    result->ForeignDataSource = FromProto(serialized.foreign_data_source());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
