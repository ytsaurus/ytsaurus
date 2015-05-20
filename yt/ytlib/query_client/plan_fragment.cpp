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

#include <ytlib/tablet_client/wire_protocol.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <core/ytree/convert.h>

#include <core/misc/protobuf_helpers.h>

#include <ytlib/query_client/plan_fragment.pb.h>

#include <limits>
#include <unordered_set>
#include <cmath>

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
        return Stroka() + GetUnaryOpcodeLexeme(unaryOp->Opcode) + " " + rhsName;
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
        Stroka str;
        for (const auto& argument : inOp->Arguments) {
            str += comma() + InferName(argument);
        }
        if (inOp->Arguments.size() > 1) {
            str = "(" + str + ")";
        }
        str += " IN (";
        newTuple = true;
        for (const auto& row : inOp->Values) {
            str += comma() + ToString(row);
        }
        return str + ")";
    } else {
        YUNREACHABLE();
    }
}

Stroka InferName(TConstQueryPtr query)
{
    auto namedItemFormatter = [] (const TNamedItem& item) {
        return InferName(item.Expression) + " AS " + item.Name;
    };

    std::vector<Stroka> clauses;
    Stroka str;

    if (query->ProjectClause) {
        str = JoinToString(query->ProjectClause->Projections, namedItemFormatter);
    } else {
        str = "*";
    }
    clauses.emplace_back("SELECT " + str);

    if (query->WhereClause) {
        str = InferName(query->WhereClause);
        clauses.push_back(Stroka("WHERE ") + str);
    }
    if (query->GroupClause) {
        str = JoinToString(query->GroupClause->GroupItems, namedItemFormatter);
        clauses.push_back(Stroka("GROUP BY ") + str);
    }
    if (query->OrderClause) {
        str = JoinToString(query->OrderClause->OrderColumns);
        clauses.push_back(Stroka("ORDER BY ") + str);
    }
    if (query->Limit < std::numeric_limits<i64>::max()) {
        str = ToString(query->Limit);
        clauses.push_back(Stroka("LIMIT ") + str);
    }

    return JoinToString(clauses, Stroka(" "));
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
        case EUnaryOp::Not:
            if (operandType != EValueType::Boolean) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires boolean operand",
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
    IFunctionRegistry* functionRegistry)
{
    if (auto function = functionRegistry->FindFunction(functionName)) {
        return function->InferResultType(argTypes, source);
    } else {
        THROW_ERROR_EXCEPTION(
            "Unknown function in expression %Qv",
            source)
            << TErrorAttribute("function_name", functionName);
    }
}

void CheckExpressionDepth(TConstExpressionPtr op, int depth = 0)
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

DECLARE_REFCOUNTED_CLASS(TSchemaProxy)

class TSchemaProxy
    : public TIntrinsicRefCounted
{
public:
    explicit TSchemaProxy(TTableSchema* tableSchema)
        : TableSchema_(tableSchema)
    { }

    // NOTE: result must be used before next call
    virtual const TColumnSchema* GetColumnPtr(const TStringBuf& name) = 0;

    // NOTE: result must be used before next call
    virtual const TColumnSchema* GetAggregateColumnPtr(
        const Stroka& aggregateFunction,
        const NAst::TExpression* arguments,
        Stroka subexprName,
        Stroka source,
        IFunctionRegistry* functionRegistry)
    {
        THROW_ERROR_EXCEPTION(
            "Misuse of aggregate function %v",
            aggregateFunction);
    }

    virtual void Finish()
    { }

    std::vector<TConstExpressionPtr> BuildTypedExpression(
        const NAst::TExpression* expr,
        const Stroka& source,
        IFunctionRegistry* functionRegistry)
    {
        auto expressions = DoBuildTypedExpression(expr, source, functionRegistry);

        for (auto& expr : expressions) {
            expr = PropagateNotExpression(std::move(expr));
        }

        return expressions;
    }

    DEFINE_BYVAL_RO_PROPERTY(TTableSchema*, TableSchema);

protected:
    std::vector<TConstExpressionPtr> DoBuildTypedExpression(
        const NAst::TExpression* expr,
        const Stroka& source,
        IFunctionRegistry* functionRegistry)
    {
        std::vector<TConstExpressionPtr> result;
        if (auto commaExpr = expr->As<NAst::TCommaExpression>()) {
            auto typedLhsExprs = DoBuildTypedExpression(commaExpr->Lhs.Get(), source, functionRegistry);
            auto typedRhsExprs = DoBuildTypedExpression(commaExpr->Rhs.Get(), source, functionRegistry);

            result.insert(result.end(), typedLhsExprs.begin(), typedLhsExprs.end());
            result.insert(result.end(), typedRhsExprs.begin(), typedRhsExprs.end());
        } else if (auto literalExpr = expr->As<NAst::TLiteralExpression>()) {
            const auto& literalValue = literalExpr->Value;
            switch (literalValue.Tag()) {
                case NAst::TLiteralValue::TagOf<i64>():
                    result.push_back(New<TLiteralExpression>(
                        EValueType::Int64,
                        GetValue(literalValue)));
                    break;
                case NAst::TLiteralValue::TagOf<ui64>():
                    result.push_back(New<TLiteralExpression>(
                        EValueType::Uint64,
                        GetValue(literalValue)));
                    break;
                case NAst::TLiteralValue::TagOf<double>():
                    result.push_back(New<TLiteralExpression>(
                        EValueType::Double,
                        GetValue(literalValue)));
                    break;
                case NAst::TLiteralValue::TagOf<bool>():
                    result.push_back(New<TLiteralExpression>(
                        EValueType::Boolean,
                        GetValue(literalValue)));
                    break;
                case NAst::TLiteralValue::TagOf<Stroka>():
                    result.push_back(New<TLiteralExpression>(
                        EValueType::String,
                        GetValue(literalValue)));
                    break;
            }
        } else if (auto referenceExpr = expr->As<NAst::TReferenceExpression>()) {
            const auto* column = GetColumnPtr(referenceExpr->ColumnName);
            if (!column) {
                THROW_ERROR_EXCEPTION("Undefined reference %Qv", referenceExpr->ColumnName);
            }

            result.push_back(New<TReferenceExpression>(
                column->Type,
                referenceExpr->ColumnName));
        } else if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
            auto functionName = functionExpr->FunctionName;
            auto aggregateFunction = GetAggregate(functionName, functionRegistry);

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
                        aggregateColumn->Type,
                        aggregateColumn->Name));

                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error creating aggregate")
                        << TErrorAttribute("source", functionExpr->GetSource(source))
                        << ex;
                }

            } else {
                auto typedOperands = DoBuildTypedExpression(functionExpr->Arguments.Get(), source, functionRegistry);

                std::vector<EValueType> types;
                for (const auto& typedOperand : typedOperands) {
                    types.push_back(typedOperand->Type);
                }

                result.push_back(New<TFunctionExpression>(
                    InferFunctionExprType(functionName, types, functionExpr->GetSource(source), functionRegistry),
                    functionName,
                    typedOperands));
            }
        } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
            auto typedOperandExpr = DoBuildTypedExpression(unaryExpr->Operand.Get(), source, functionRegistry);

            for (const auto& operand : typedOperandExpr) {
                if (auto foldedExpr = FoldConstants(unaryExpr, operand)) {
                    result.push_back(foldedExpr);
                } else {
                    result.push_back(New<TUnaryOpExpression>(
                        InferUnaryExprType(
                            unaryExpr->Opcode,
                            operand->Type,
                            unaryExpr->GetSource(source)),
                        unaryExpr->Opcode,
                        operand));
                }
            }
        } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
            auto typedLhsExpr = DoBuildTypedExpression(binaryExpr->Lhs.Get(), source, functionRegistry);
            auto typedRhsExpr = DoBuildTypedExpression(binaryExpr->Rhs.Get(), source, functionRegistry);

            auto makeBinaryExpr = [&] (EBinaryOp op, TConstExpressionPtr lhs, TConstExpressionPtr rhs) -> TConstExpressionPtr {
                auto type = InferBinaryExprType(op, lhs->Type, rhs->Type, binaryExpr->GetSource(source));
                if (auto foldedExpr = FoldConstants(binaryExpr, lhs, rhs)) {
                    return foldedExpr;
                } else {
                    return New<TBinaryOpExpression>(type, op, lhs, rhs);
                }
            };

            std::function<TConstExpressionPtr(int, int, EBinaryOp)> gen = [&] (int offset, int keySize, EBinaryOp op) -> TConstExpressionPtr {
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
                    THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
                        typedLhsExpr.size(),
                        typedRhsExpr.size())
                        << TErrorAttribute("source", binaryExpr->Rhs->GetSource(source));
                }

                int keySize = typedLhsExpr.size();
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
            auto inExprOperands = DoBuildTypedExpression(inExpr->Expr.Get(), source, functionRegistry);

            std::unordered_set<Stroka> references;
            std::vector<EValueType> argTypes;
            for (const auto& arg : inExprOperands) {
                argTypes.push_back(arg->Type);
                if (auto reference = arg->As<TReferenceExpression>()) {
                    if (references.find(reference->ColumnName) != references.end()) {
                        THROW_ERROR_EXCEPTION("IN operator has multiple references to column %Qv", reference->ColumnName)
                            << TErrorAttribute("source", source);
                    } else {
                        references.insert(reference->ColumnName);
                    }
                }
            }

            auto capturedRows = TupleListsToRows(inExpr->Values, argTypes, inExpr->GetSource(source));
            result.push_back(New<TInOpExpression>(
                std::move(inExprOperands),
                std::move(capturedRows)));
        }

        return result;
    }

    TConstExpressionPtr PropagateNotExpression(TConstExpressionPtr expr)
    {
        if (expr->As<TReferenceExpression>() ||
            expr->As<TLiteralExpression>())
        {
            return expr;
        } else if (auto inExpr = expr->As<TInOpExpression>()) {
            std::vector<TConstExpressionPtr> propagatedArgumenst;
            for (auto argument : inExpr->Arguments) {
                propagatedArgumenst.push_back(PropagateNotExpression(argument));
            }
            return New<TInOpExpression>(
                std::move(propagatedArgumenst),
                inExpr->Values);
        } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
            std::vector<TConstExpressionPtr> propagatedArgumenst;
            for (auto argument : functionExpr->Arguments) {
                propagatedArgumenst.push_back(PropagateNotExpression(argument));
            }
            return New<TFunctionExpression>(
                functionExpr->Type,
                functionExpr->FunctionName,
                std::move(propagatedArgumenst));
        } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
            return New<TBinaryOpExpression>(
                binaryOp->Type,
                binaryOp->Opcode,
                PropagateNotExpression(binaryOp->Lhs),
                PropagateNotExpression(binaryOp->Rhs));
        } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
            auto& operand = unaryOp->Operand;
            if (unaryOp->Opcode == EUnaryOp::Not) {
                if (auto operandUnaryOp = operand->As<TUnaryOpExpression>()) {
                    if (operandUnaryOp->Opcode == EUnaryOp::Not) {
                        return PropagateNotExpression(operandUnaryOp->Operand);
                    }
                } else if (auto operandBinaryOp = operand->As<TBinaryOpExpression>()) {
                    if (operandBinaryOp->Opcode == EBinaryOp::And) {
                        return PropagateNotExpression(MakeOrExpression(
                            New<TUnaryOpExpression>(
                                operandBinaryOp->Lhs->Type,
                                EUnaryOp::Not,
                                operandBinaryOp->Lhs),
                            New<TUnaryOpExpression>(
                                operandBinaryOp->Rhs->Type,
                                EUnaryOp::Not,
                                operandBinaryOp->Rhs)));
                    } else if (operandBinaryOp->Opcode == EBinaryOp::Or) {
                        return PropagateNotExpression(MakeAndExpression(
                            New<TUnaryOpExpression>(
                                operandBinaryOp->Lhs->Type,
                                EUnaryOp::Not,
                                operandBinaryOp->Lhs),
                            New<TUnaryOpExpression>(
                                operandBinaryOp->Rhs->Type,
                                EUnaryOp::Not,
                                operandBinaryOp->Rhs)));
                    } else if (IsRelationalBinaryOp(operandBinaryOp->Opcode)) {
                        return PropagateNotExpression(New<TBinaryOpExpression>(
                            operandBinaryOp->Type,
                            GetInversedBinaryOpcode(operandBinaryOp->Opcode),
                            operandBinaryOp->Lhs,
                            operandBinaryOp->Rhs));
                    }
                } else if (auto literal = operand->As<TLiteralExpression>()) {
                    TUnversionedValue value = literal->Value;
                    value.Data.Boolean = !value.Data.Boolean;
                    return New<TLiteralExpression>(
                        literal->Type,
                        value);
                }
            }
            return New<TUnaryOpExpression>(
                unaryOp->Type,
                unaryOp->Opcode,
                PropagateNotExpression(operand));
        }
        YUNREACHABLE();
    }

protected:
    static const TColumnSchema* AddColumn(TTableSchema* tableSchema, const TColumnSchema& column)
    {
        tableSchema->Columns().push_back(column);
        return &tableSchema->Columns().back();
    }

    static TNullable<Stroka> GetAggregate(
        const TStringBuf functionName,
        IFunctionRegistryPtr functionRegistry)
    {
        Stroka name(functionName);
        name.to_lower();

        TNullable<Stroka> result;

        if (functionRegistry->FindAggregateFunction(name))
        {
            result.Assign(name);
        }

        return result;
    };

    static EValueType GetType(const NAst::TLiteralValue& literalValue)
    {
        switch (literalValue.Tag()) {
            case NAst::TLiteralValue::TagOf<i64>():
                return EValueType::Int64;
            case NAst::TLiteralValue::TagOf<ui64>():
                return EValueType::Uint64;
            case NAst::TLiteralValue::TagOf<double>():
                return EValueType::Double;
            case NAst::TLiteralValue::TagOf<bool>():
                return EValueType::Boolean;
            case NAst::TLiteralValue::TagOf<Stroka>():
                return EValueType::String;
            default:
                YUNREACHABLE();
        }
    }

    static TValue GetValue(const NAst::TLiteralValue& literalValue)
    {
        switch (literalValue.Tag()) {
            case NAst::TLiteralValue::TagOf<i64>():
                return MakeUnversionedInt64Value(literalValue.As<i64>());
            case NAst::TLiteralValue::TagOf<ui64>():
                return MakeUnversionedUint64Value(literalValue.As<ui64>());
            case NAst::TLiteralValue::TagOf<double>():
                return MakeUnversionedDoubleValue(literalValue.As<double>());
            case NAst::TLiteralValue::TagOf<bool>():
                return MakeUnversionedBooleanValue(literalValue.As<bool>());
            case NAst::TLiteralValue::TagOf<Stroka>():
                return MakeUnversionedStringValue(
                    literalValue.As<Stroka>().c_str(),
                    literalValue.As<Stroka>().length());
            default:
                YUNREACHABLE();
        }
    }

    static TSharedRange<TRow> TupleListsToRows(
        const NAst::TLiteralValueTupleList& literalTuples,
        const std::vector<EValueType>& argTypes,
        const TStringBuf& source)
    {
        auto rowBuffer = New<TRowBuffer>();
        TUnversionedRowBuilder rowBuilder;
        std::vector<TRow> rows;
        for (const auto & tuple : literalTuples) {
            if (tuple.size() != argTypes.size()) {
                THROW_ERROR_EXCEPTION("IN operator arguments size mismatch")
                    << TErrorAttribute("source", source);
            }

            for (int i = 0; i < tuple.size(); ++i) {
                if (GetType(tuple[i]) != argTypes[i]) {
                    THROW_ERROR_EXCEPTION("IN operator types mismatch")
                        << TErrorAttribute("source", source)
                        << TErrorAttribute("expected", argTypes[i])
                        << TErrorAttribute("actual", GetType(tuple[i]));
                }

                rowBuilder.AddValue(GetValue(tuple[i]));
            }
            rows.push_back(rowBuffer->Capture(rowBuilder.GetRow()));
            rowBuilder.Reset();
        }

        std::sort(rows.begin(), rows.end());
        return MakeSharedRange(std::move(rows), std::move(rowBuffer));
    }

    TConstExpressionPtr FoldConstants(
        const NAst::TUnaryOpExpression* unaryExpr,
        TConstExpressionPtr operand)
    {
        auto foldConstants = [] (EUnaryOp opcode, TConstExpressionPtr operand) -> TNullable<TUnversionedValue> {
            if (auto literalExpr = operand->As<TLiteralExpression>()) {
                if (opcode == EUnaryOp::Plus) {
                    return static_cast<TUnversionedValue>(literalExpr->Value);
                } else if (opcode == EUnaryOp::Minus) {
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
                    return value;
                }
            }
            return TNullable<TUnversionedValue>();
        };

        if (auto value = foldConstants(unaryExpr->Opcode, operand)) {
            return New<TLiteralExpression>(EValueType(value->Type), *value);
        }

        return TConstExpressionPtr();
    }

    TConstExpressionPtr FoldConstants(
        const NAst::TBinaryOpExpression* binaryExpr,
        TConstExpressionPtr lhsExpr,
        TConstExpressionPtr rhsExpr)
    {
        auto foldConstants = [] (
            EBinaryOp opcode,
            TConstExpressionPtr lhsExpr,
            TConstExpressionPtr rhsExpr)
            -> TNullable<TUnversionedValue>
        {
            auto lhsLiteral = lhsExpr->As<TLiteralExpression>();
            auto rhsLiteral = rhsExpr->As<TLiteralExpression>();
            if (lhsLiteral && rhsLiteral) {
                auto lhs = static_cast<TUnversionedValue>(lhsLiteral->Value);
                auto rhs = static_cast<TUnversionedValue>(rhsLiteral->Value);
                YCHECK(lhs.Type == rhs.Type);

                switch (opcode) {
                    case EBinaryOp::Plus:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                lhs.Data.Int64 += rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                lhs.Data.Uint64 += rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Double:
                                lhs.Data.Double += rhs.Data.Double;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Minus:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                lhs.Data.Int64 -= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                lhs.Data.Uint64 -= rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Double:
                                lhs.Data.Double -= rhs.Data.Double;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Multiply:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                lhs.Data.Int64 *= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                lhs.Data.Uint64 *= rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Double:
                                lhs.Data.Double *= rhs.Data.Double;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Divide:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                if (rhs.Data.Int64 == 0) {
                                    THROW_ERROR_EXCEPTION("Division by zero");
                                }
                                lhs.Data.Int64 /= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                if (rhs.Data.Uint64 == 0) {
                                    THROW_ERROR_EXCEPTION("Division by zero");
                                }
                                lhs.Data.Uint64 /= rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Double:
                                if (std::abs(rhs.Data.Double) <= std::numeric_limits<double>::epsilon()) {
                                    THROW_ERROR_EXCEPTION("Division by zero");
                                }
                                lhs.Data.Double /= rhs.Data.Double;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Modulo:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                if (rhs.Data.Int64 == 0) {
                                    THROW_ERROR_EXCEPTION("Division by zero");
                                }
                                lhs.Data.Int64 %= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                if (rhs.Data.Uint64 == 0) {
                                    THROW_ERROR_EXCEPTION("Division by zero");
                                }
                                lhs.Data.Uint64 %= rhs.Data.Uint64;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::And:
                        switch (lhs.Type) {
                            case EValueType::Boolean:
                                lhs.Data.Boolean = lhs.Data.Boolean && rhs.Data.Boolean;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Or:
                        switch (lhs.Type) {
                            case EValueType::Boolean:
                                lhs.Data.Boolean = lhs.Data.Boolean || rhs.Data.Boolean;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Equal:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) == 0);
                        break;
                    case EBinaryOp::NotEqual:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) != 0);
                        break;
                    case EBinaryOp::Less:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) < 0);
                        break;
                    case EBinaryOp::Greater:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) > 0);
                        break;
                    case EBinaryOp::LessOrEqual:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) <= 0);
                        break;
                    case EBinaryOp::GreaterOrEqual:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) >= 0);
                        break;
                    default:
                        break;
                }
            }
            return TNullable<TUnversionedValue>();
        };

        if (auto value = foldConstants(binaryExpr->Opcode, lhsExpr, rhsExpr)) {
            return New<TLiteralExpression>(EValueType(value->Type), *value);
        }

        if (binaryExpr->Opcode == EBinaryOp::Divide) {
            auto lhsBinaryExpr = lhsExpr->As<TBinaryOpExpression>();
            auto rhsLiteralExpr = rhsExpr->As<TLiteralExpression>();
            if (lhsBinaryExpr && rhsLiteralExpr && lhsBinaryExpr->Opcode == EBinaryOp::Divide) {
                auto lhsLiteralExpr = lhsBinaryExpr->Rhs->As<TLiteralExpression>();
                if (lhsLiteralExpr) {
                    TUnversionedValue lhs = lhsLiteralExpr->Value;
                    TUnversionedValue rhs = rhsLiteralExpr->Value;
                    YCHECK(lhs.Type == rhs.Type);

                    auto overflow = [] (ui64 a, ui64 b, bool isSigned) {
                        auto rsh = [] (ui64 base, int amount) {return base >> amount;};
                        auto lower = [] (ui64 base) {return base & 0xffffffff;};
                        ui64 a1 = lower(a);
                        ui64 a2 = rsh(a, 32);
                        ui64 b1 = lower(b);
                        ui64 b2 = rsh(b, 32);
                        int shift = isSigned ? 31 : 32;
                        return (a2 * b2 != 0 ||
                            rsh(a1 * b2, shift) != 0 ||
                            rsh(a2 * b1, shift) != 0 ||
                            rsh(lower(a1 * b2) + lower(a2 * b1) + rsh(a1 * b1, 32), shift) != 0);
                    };

                    auto makeBinaryExpr = [&] (TUnversionedValue divisor) {
                        return New<TBinaryOpExpression>(
                            divisor.Type,
                            EBinaryOp::Divide,
                            lhsBinaryExpr->Lhs,
                            New<TLiteralExpression>(
                                divisor.Type,
                                divisor));
                    };

                    switch (lhs.Type) {
                        case EValueType::Int64:
                            if (!overflow(lhs.Data.Int64, rhs.Data.Int64, true)) {
                                lhs.Data.Int64 *= rhs.Data.Int64;
                                return makeBinaryExpr(lhs);
                            }
                            break;
                        case EValueType::Uint64:
                            if (!overflow(lhs.Data.Uint64, rhs.Data.Uint64, false)) {
                                lhs.Data.Uint64 *= rhs.Data.Uint64;
                                return makeBinaryExpr(lhs);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        return TConstExpressionPtr();
    }
};

DEFINE_REFCOUNTED_TYPE(TSchemaProxy)

class TSimpleSchemaProxy
    : public TSchemaProxy
{
public:
    explicit TSimpleSchemaProxy(TTableSchema* tableSchema)
        : TSchemaProxy(tableSchema)
    { }
    
    TSimpleSchemaProxy(
        TTableSchema* tableSchema,
        const TTableSchema& sourceTableSchema,
        int keyColumnCount = 0)
        : TSchemaProxy(tableSchema)
        , SourceTableSchema_(sourceTableSchema)
    {
        const auto& columns = sourceTableSchema.Columns();
        int count = std::min(
            sourceTableSchema.HasComputedColumns() ? keyColumnCount : 0,
            static_cast<int>(columns.size()));
        for (int i = 0; i < count; ++i) {
            AddColumn(GetTableSchema(), columns[i]);
        }
    }

    virtual const TColumnSchema* GetColumnPtr(const TStringBuf& name) override
    {
        const auto* column = GetTableSchema()->FindColumn(name);

        !column
            && SourceTableSchema_
            && (column = SourceTableSchema_->FindColumn(name))
            && (column = AddColumn(GetTableSchema(), *column));       

        return column;
    }

    virtual void Finish() override
    {
        if (SourceTableSchema_) {
            for (const auto& column : SourceTableSchema_->Columns()) {
                if (!GetTableSchema()->FindColumn(column.Name)) {
                    AddColumn(GetTableSchema(), column);
                }
            }
        }
    }

private:
    const TNullable<TTableSchema> SourceTableSchema_;

};

class TJoinSchemaProxy
    : public TSchemaProxy
{
public:
    TJoinSchemaProxy(
        TTableSchema* tableSchema,
        TSchemaProxyPtr self,
        TSchemaProxyPtr foreign)
        : TSchemaProxy(tableSchema)
        , Self_(self)
        , Foreign_(foreign)
    { }

    virtual const TColumnSchema* GetColumnPtr(const TStringBuf& name) override
    {
        auto tableSchema = GetTableSchema();
        const TColumnSchema* column = tableSchema->FindColumn(name);

        if (!column) {
            if (column = Self_->GetColumnPtr(name)) {
                if (Foreign_->GetColumnPtr(name)) {
                    THROW_ERROR_EXCEPTION("Column %Qv collision", name);
                } else {
                    column = AddColumn(tableSchema, *column);
                }
            } else if (column = Foreign_->GetColumnPtr(name)) {
                column = AddColumn(tableSchema, *column);
            }
        }

        return column;
    }

    virtual void Finish() override
    {
        Self_->Finish();
        Foreign_->Finish();

        auto tableSchema = GetTableSchema();

        for (const auto& column : Self_->GetTableSchema()->Columns()) {
            if (!tableSchema->FindColumn(column.Name)) {
                AddColumn(tableSchema, column);
            }
        }

        for (const auto& column : Foreign_->GetTableSchema()->Columns()) {
            if (!tableSchema->FindColumn(column.Name)) {
                AddColumn(tableSchema, column);
            }
        }
    }

private:
    TSchemaProxyPtr Self_;
    TSchemaProxyPtr Foreign_;

};

class TGroupSchemaProxy
    : public TSchemaProxy
{
public:
    TGroupSchemaProxy(
        TTableSchema* tableSchema,
        TSchemaProxyPtr base,
        TAggregateItemList* aggregateItems)
        : TSchemaProxy(tableSchema)
        , Base_(base)
        , AggregateItems_(aggregateItems)
    { }

    virtual const TColumnSchema* GetColumnPtr(const TStringBuf& name) override
    {
        return GetTableSchema()->FindColumn(name);
    }

    virtual const TColumnSchema* GetAggregateColumnPtr(
        const Stroka& aggregateFunction,
        const NAst::TExpression* arguments,
        Stroka subexprName,
        Stroka source,
        IFunctionRegistry* functionRegistry) override
    {
        const TColumnSchema* aggregateColumn = GetTableSchema()->FindColumn(subexprName);

        if (!aggregateColumn) {
            auto typedOperands = Base_->BuildTypedExpression(
                arguments,
                source,
                functionRegistry);

            if (typedOperands.size() != 1) {
                THROW_ERROR_EXCEPTION(
                    "Aggregate function %Qv must have exactly one argument",
                    aggregateFunction);
            }

            CheckExpressionDepth(typedOperands.front());

            AggregateItems_->emplace_back(
                typedOperands.front(),
                aggregateFunction,
                subexprName);

            aggregateColumn = AddColumn(GetTableSchema(), TColumnSchema(subexprName, typedOperands.front()->Type));
        }

        return aggregateColumn;
    }

private:
    TSchemaProxyPtr Base_;
    TAggregateItemList* AggregateItems_;

};

TConstExpressionPtr BuildWhereClause(
    NAst::TExpressionPtr& expressionAst,
    const Stroka& source,
    const TSchemaProxyPtr& schemaProxy,
    IFunctionRegistry* functionRegistry)
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
    NAst::TNullableNamedExpressionList& expressionsAst,
    const Stroka& source,
    TSchemaProxyPtr& schemaProxy,
    IFunctionRegistry* functionRegistry)
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
    NAst::TNullableNamedExpressionList& expressionsAst,
    const Stroka& source,
    TSchemaProxyPtr& schemaProxy,
    IFunctionRegistry* functionRegistry)
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

void PrepareQuery(
    const TQueryPtr& query,
    NAst::TQuery& ast,
    const Stroka& source,
    TSchemaProxyPtr& schemaProxy,
    IFunctionRegistry* functionRegistry)
{
    if (ast.WherePredicate) {
        query->WhereClause = BuildWhereClause(ast.WherePredicate, source, schemaProxy, functionRegistry);
    }

    if (ast.GroupExprs) {
        query->GroupClause = BuildGroupClause(ast.GroupExprs, source, schemaProxy, functionRegistry);
    }

    if (ast.OrderFields) {
        auto orderClause = New<TOrderClause>();
        orderClause->OrderColumns = ast.OrderFields.Get();
        query->OrderClause = std::move(orderClause);
    }

    if (ast.SelectExprs) {
        query->ProjectClause = BuildProjectClause(ast.SelectExprs, source, schemaProxy, functionRegistry);
    }

    schemaProxy->Finish();
}

void ParseYqlString(
    NAst::TAstHead* astHead,
    const Stroka& source,
    NAst::TParser::token::yytokentype strayToken)
{
    NAst::TLexer lexer(source, strayToken);
    NAst::TParser parser(lexer, astHead, source);

    int result = parser.parse();

    if (result != 0) {
        THROW_ERROR_EXCEPTION("Parse failure")
            << TErrorAttribute("source", source);
    }
}

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    IFunctionRegistry* functionRegistry,
    i64 inputRowLimit,
    i64 outputRowLimit,
    TTimestamp timestamp)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseQuery);

    auto& ast = astHead.As<NAst::TQuery>();
    
    TDataSplit selfDataSplit;
    TDataSplit foreignDataSplit;

    auto query = New<TQuery>(inputRowLimit, outputRowLimit, TGuid::Create());
    TSchemaProxyPtr schemaProxy;
    
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

    PrepareQuery(query, ast, source, schemaProxy, functionRegistry);

    auto planFragment = New<TPlanFragment>(source);

    if (ast.Limit) {
        query->Limit = ast.Limit;
        if (!query->OrderClause) {
            planFragment->Ordered = true;
        }
    } else if (query->OrderClause) {
        THROW_ERROR_EXCEPTION("ORDER BY used without LIMIT");
    }

    planFragment->Query = query;
    planFragment->Timestamp = timestamp;

    auto range = GetBothBoundsFromDataSplit(selfDataSplit);
    
    TRowRange rowRange(
        planFragment->KeyRangesRowBuffer->Capture(range.first.Get()),
        planFragment->KeyRangesRowBuffer->Capture(range.second.Get()));

    planFragment->DataSources.push_back({
        GetObjectIdFromDataSplit(selfDataSplit),
        rowRange});

    if (query->JoinClause) {
        planFragment->ForeignDataId = GetObjectIdFromDataSplit(foreignDataSplit);
    }
    
    return planFragment;
}

TQueryPtr PrepareJobQuery(
    const Stroka& source,
    const TTableSchema& tableSchema,
    IFunctionRegistry* functionRegistry)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseJobQuery);

    auto& ast = astHead.As<NAst::TQuery>();

    if (ast.Limit) {
        THROW_ERROR_EXCEPTION("LIMIT is not supported in map-reduce queries");
    }

    if (ast.GroupExprs) {
        THROW_ERROR_EXCEPTION("GROUP BY is not supported in map-reduce queries");
    }

    auto planFragment = New<TPlanFragment>(source);
    auto unlimited = std::numeric_limits<i64>::max();

    auto query = New<TQuery>(unlimited, unlimited, TGuid::Create());
    TSchemaProxyPtr schemaProxy = New<TSimpleSchemaProxy>(&query->TableSchema, tableSchema);

    PrepareQuery(query, ast, source, schemaProxy, functionRegistry);

    return query;
}

TConstExpressionPtr PrepareExpression(
    const Stroka& source,
    TTableSchema tableSchema,
    IFunctionRegistry* functionRegistry)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TExpressionPtr>()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseExpression);

    auto& expr = astHead.As<NAst::TExpressionPtr>();

    auto schemaProxy = New<TSimpleSchemaProxy>(&tableSchema);

    auto typedExprs = schemaProxy->BuildTypedExpression(expr.Get(), source, functionRegistry);

    if (typedExprs.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            << TErrorAttribute("source", expr->GetSource(source));
    }

    return typedExprs.front();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExpression* serialized, TConstExpressionPtr original)
{
    serialized->set_type(static_cast<int>(original->Type));

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

        NTabletClient::TWireProtocolWriter writer;
        writer.WriteUnversionedRowset(inOpExpr->Values);
        ToProto(proto->mutable_values(), ToString(MergeRefs(writer.Flush())));
    } else {
        YUNREACHABLE();
    }
}

TExpressionPtr FromProto(const NProto::TExpression& serialized)
{
    auto kind = EExpressionKind(serialized.kind());
    auto type = EValueType(serialized.type());

    switch (kind) {
        case EExpressionKind::Literal: {
            auto typedResult = New<TLiteralExpression>(type);
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
            auto typedResult = New<TReferenceExpression>(type);
            auto data = serialized.GetExtension(NProto::TReferenceExpression::reference_expression);
            typedResult->ColumnName = data.column_name();
            return typedResult;
        }

        case EExpressionKind::Function: {
            auto typedResult = New<TFunctionExpression>(type);
            auto data = serialized.GetExtension(NProto::TFunctionExpression::function_expression);
            typedResult->FunctionName = data.function_name();
            typedResult->Arguments.reserve(data.arguments_size());
            for (int i = 0; i < data.arguments_size(); ++i) {
                typedResult->Arguments.push_back(FromProto(data.arguments(i)));
            }
            return typedResult;
        }

        case EExpressionKind::UnaryOp: {
            auto typedResult = New<TUnaryOpExpression>(type);
            auto data = serialized.GetExtension(NProto::TUnaryOpExpression::unary_op_expression);
            typedResult->Opcode = EUnaryOp(data.opcode());
            typedResult->Operand = FromProto(data.operand());
            return typedResult;
        }

        case EExpressionKind::BinaryOp: {
            auto typedResult = New<TBinaryOpExpression>(type);
            auto data = serialized.GetExtension(NProto::TBinaryOpExpression::binary_op_expression);
            typedResult->Opcode = EBinaryOp(data.opcode());
            typedResult->Lhs = FromProto(data.lhs());
            typedResult->Rhs = FromProto(data.rhs());
            return typedResult;
        }

        case EExpressionKind::InOp: {
            auto typedResult = New<TInOpExpression>(type);
            auto data = serialized.GetExtension(NProto::TInOpExpression::in_op_expression);
            typedResult->Arguments.reserve(data.arguments_size());
            for (int i = 0; i < data.arguments_size(); ++i) {
                typedResult->Arguments.push_back(FromProto(data.arguments(i)));
            }

            NTabletClient::TWireProtocolReader reader(TSharedRef::FromString(data.values()));
            typedResult->Values = reader.ReadUnversionedRowset();

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
    serialized->set_aggregate_function_name(original.AggregateFunction);
    ToProto(serialized->mutable_name(), original.Name);
}

void ToProto(NProto::TJoinClause* proto, TConstJoinClausePtr original)
{
    ToProto(proto->mutable_join_columns(), original->JoinColumns);
    ToProto(proto->mutable_joined_table_schema(), original->JoinedTableSchema);
    ToProto(proto->mutable_foreign_table_schema(), original->ForeignTableSchema);
    ToProto(proto->mutable_foreign_key_columns(), original->ForeignKeyColumns);
}

void ToProto(NProto::TGroupClause* proto, TConstGroupClausePtr original)
{
    ToProto(proto->mutable_group_items(), original->GroupItems);
    ToProto(proto->mutable_aggregate_items(), original->AggregateItems);
}

void ToProto(NProto::TProjectClause* proto, TConstProjectClausePtr original)
{
    ToProto(proto->mutable_projections(), original->Projections);
}

void ToProto(NProto::TOrderClause* proto, TConstOrderClausePtr original)
{
    ToProto(proto->mutable_order_columns(), original->OrderColumns);
}

void ToProto(NProto::TQuery* proto, TConstQueryPtr original)
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

    if (original->OrderClause) {
        ToProto(proto->mutable_order_clause(), original->OrderClause);
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
    Stroka aggregateFunction;
    if (serialized.has_aggregate_function_name()) {
        aggregateFunction = serialized.aggregate_function_name();
    } else {
        switch (EAggregateFunction(serialized.aggregate_function())) {
            case EAggregateFunction::Min:
                aggregateFunction = "min";
                break;
            case EAggregateFunction::Max:
                aggregateFunction = "max";
                break;
            case EAggregateFunction::Sum:
                aggregateFunction = "sum";
                break;
        }
    }

    return TAggregateItem(
        FromProto(serialized.expression()),
        aggregateFunction,
        serialized.name());
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

TOrderClausePtr FromProto(const NProto::TOrderClause& serialized)
{
    auto result = New<TOrderClause>();

    result->OrderColumns.reserve(serialized.order_columns_size());
    for (int i = 0; i < serialized.order_columns_size(); ++i) {
        result->OrderColumns.push_back(serialized.order_columns(i));
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

    if (serialized.has_join_clause()) {
        query->JoinClause = FromProto(serialized.join_clause());
    }

    if (serialized.has_predicate()) {
        query->WhereClause = FromProto(serialized.predicate());
    }

    if (serialized.has_group_clause()) {
        query->GroupClause = FromProto(serialized.group_clause());       
    }

    if (serialized.has_order_clause()) {
        query->OrderClause = FromProto(serialized.order_clause());       
    }

    if (serialized.has_project_clause()) {
        query->ProjectClause = FromProto(serialized.project_clause());       
    }

    return query;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TPlanFragment* proto, TConstPlanFragmentPtr fragment)
{
    ToProto(proto->mutable_query(), fragment->Query);

    NTabletClient::TWireProtocolWriter writer;
    for (const auto& dataSource : fragment->DataSources) {
        ToProto(proto->add_data_id(), dataSource.Id);
        const auto& range = dataSource.Range;
        writer.WriteUnversionedRow(range.first);
        writer.WriteUnversionedRow(range.second);
    }

    ToProto(proto->mutable_data_bounds(), ToString(MergeRefs(writer.Flush())));

    ToProto(proto->mutable_foreign_data_id(), fragment->ForeignDataId);
    proto->set_ordered(fragment->Ordered);
    proto->set_verbose_logging(fragment->VerboseLogging);
    
    proto->set_source(fragment->Source);
    proto->set_timestamp(fragment->Timestamp);
}

TPlanFragmentPtr FromProto(const NProto::TPlanFragment& serialized)
{
    auto result = New<TPlanFragment>(serialized.source());

    result->Query = FromProto(serialized.query());
    result->Ordered = serialized.ordered();
    result->VerboseLogging = serialized.verbose_logging();
    result->Timestamp = serialized.timestamp();

    NTabletClient::TWireProtocolReader reader(TSharedRef::FromString(serialized.data_bounds()));

    const auto& rowBuffer = result->KeyRangesRowBuffer;
    for (int i = 0; i < serialized.data_id_size(); ++i) {
        TDataSource dataSource;
        FromProto(&dataSource.Id, serialized.data_id(i));

        auto lowerBound = rowBuffer->Capture(reader.ReadUnversionedRow());
        auto upperBound = rowBuffer->Capture(reader.ReadUnversionedRow());

        dataSource.Range = TRowRange(lowerBound, upperBound);
        result->DataSources.push_back(dataSource);
    }

    FromProto(&result->ForeignDataId, serialized.foreign_data_id());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
