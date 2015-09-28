#include "stdafx.h"

#include "query_preparer.h"
#include "helpers.h"
#include "plan_helpers.h"
#include "lexer.h"
#include "parser.hpp"
#include "callbacks.h"
#include "functions.h"
#include "function_registry.h"

#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <unordered_set>

namespace NYT {
namespace NQueryClient {
using namespace NConcurrency;
using namespace NTableClient;

static const auto& Logger = QueryClientLogger;
static const int PlanFragmentDepthLimit = 50;

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

        case EUnaryOp::BitNot:
            if (!IsIntegralType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires integral operand",
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

EValueType InferBinaryExprType(
    EBinaryOp opCode,
    EValueType lhsType,
    EValueType rhsType,
    const TStringBuf& source,
    const TStringBuf& lhsSource,
    const TStringBuf& rhsSource)
{
    if (lhsType != rhsType) {
        THROW_ERROR_EXCEPTION(
            "Type mismatch in expression %Qv",
            source)
            << TErrorAttribute("lhs_source", lhsSource)
            << TErrorAttribute("rhs_source", rhsSource)
            << TErrorAttribute("lhs_type", lhsType)
            << TErrorAttribute("rhs_type", rhsType);
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
                    << TErrorAttribute("lhs_source", lhsSource)
                    << TErrorAttribute("rhs_source", rhsSource)
                    << TErrorAttribute("operand_type", operandType);
            }
            return operandType;

        case EBinaryOp::Modulo:
        case EBinaryOp::LeftShift:
        case EBinaryOp::RightShift:
        case EBinaryOp::BitOr:
        case EBinaryOp::BitAnd:
            if (!IsIntegralType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires integral operands",
                    source)
                    << TErrorAttribute("lhs_source", lhsSource)
                    << TErrorAttribute("rhs_source", rhsSource)
                    << TErrorAttribute("operand_type", operandType);
            }
            return operandType;

        case EBinaryOp::And:
        case EBinaryOp::Or:
            if (operandType != EValueType::Boolean) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires boolean operands",
                    source)
                    << TErrorAttribute("lhs_source", lhsSource)
                    << TErrorAttribute("rhs_source", rhsSource)
                    << TErrorAttribute("operand_type", operandType);
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
                    << TErrorAttribute("lhs_source", lhsSource)
                    << TErrorAttribute("rhs_source", rhsSource)
                    << TErrorAttribute("lhs_type", operandType);
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

DECLARE_REFCOUNTED_CLASS(TSchemaProxy)

class TSchemaProxy
    : public TIntrinsicRefCounted
{
public:
    explicit TSchemaProxy(TTableSchema* tableSchema)
        : TableSchema_(tableSchema)
    {
        YCHECK(tableSchema);
        const auto& columns = TableSchema_->Columns();
        for (size_t index = 0; index < columns.size(); ++index) {
            Lookup_.insert(MakePair(MakePair(Stroka(columns[index].Name), Stroka()), index));
        }
    }

    // NOTE: result must be used before next call
    const TColumnSchema* GetColumnPtr(const TStringBuf& name, const TStringBuf& tableName)
    {
        auto& resultColumns = TableSchema_->Columns();
        auto it = Lookup_.find(MakePair(Stroka(name), Stroka(tableName)));
        if (it != Lookup_.end()) {
            return &resultColumns[it->second];
        } else if (auto original = AddColumnPtr(name, tableName)) {
            auto index = resultColumns.size();
            Lookup_.insert(MakePair(MakePair(Stroka(name), Stroka(tableName)), index));
            resultColumns.push_back(*original);
            resultColumns.back().Name = NAst::FormatColumn(name, tableName);
            return &resultColumns.back();
        }
        return nullptr;
    }

    const yhash_map<TPair<Stroka, Stroka>, size_t>& GetLookup() const
    {
        return Lookup_;
    }

    const TColumnSchema* GetAggregateColumnPtr(
        const Stroka& aggregateFunction,
        const NAst::TExpression* arguments,
        Stroka subexprName,
        Stroka source,
        IFunctionRegistry* functionRegistry,
        const NAst::TAliasMap& aliasMap)
    {
        auto& resultColumns = TableSchema_->Columns();
        auto it = Lookup_.find(MakePair(Stroka(subexprName), Stroka()));
        if (it != Lookup_.end()) {
            return &resultColumns[it->second];
        }

        auto original = AddAggregateColumnPtr(
            aggregateFunction,
            arguments,
            subexprName,
            source,
            functionRegistry,
            aliasMap);

        auto index = resultColumns.size();
        Lookup_.insert(MakePair(MakePair(Stroka(subexprName), Stroka()), index));
        resultColumns.push_back(original);
        return &resultColumns.back();
    }

    virtual void Finish()
    { }

    struct TNamedExpression
    {
        TConstExpressionPtr Expr;
        TNullable<Stroka> Name;

        TNamedExpression(const TConstExpressionPtr& expr, const TNullable<Stroka>& name = TNullable<Stroka>())
            : Expr(expr)
            , Name(name)
        { }
    };

    TConstExpressionPtr BuildTypedExpression(
        const NAst::TExpression* expr,
        const Stroka& source,
        IFunctionRegistry* functionRegistry,
        const NAst::TAliasMap& aliasMap)
    {
        std::set<Stroka> usedAliases;
        return PropagateNotExpression(
            DoBuildTypedExpression(expr, source, functionRegistry, aliasMap, usedAliases));
    }

    DEFINE_BYVAL_RO_PROPERTY(TTableSchema*, TableSchema);



protected:
    TConstExpressionPtr DoBuildTypedExpression(
        const NAst::TExpression* expr,
        const Stroka& source,
        IFunctionRegistry* functionRegistry,
        const NAst::TAliasMap& aliasMap,
        std::set<Stroka>& usedAliases)
    {
        if (auto literalExpr = expr->As<NAst::TLiteralExpression>()) {
            const auto& literalValue = literalExpr->Value;

            return New<TLiteralExpression>(
                GetType(literalValue),
                GetValue(literalValue));
        } else if (auto referenceExpr = expr->As<NAst::TReferenceExpression>()) {
            const auto* column = GetColumnPtr(referenceExpr->ColumnName, referenceExpr->TableName);
            if (!column) {
                if (referenceExpr->TableName.empty()) {
                    auto columnName = referenceExpr->ColumnName;
                    auto found = aliasMap.find(columnName);

                    if (found != aliasMap.end()) {
                        if (usedAliases.count(columnName)) {
                            THROW_ERROR_EXCEPTION("Recursive usage of alias %Qv", columnName);
                        }

                        usedAliases.insert(columnName);
                        auto aliasExpr = DoBuildTypedExpression(
                            found->second.Get(),
                            source,
                            functionRegistry,
                            aliasMap,
                            usedAliases);

                        usedAliases.erase(columnName);
                        return aliasExpr;
                    }
                }

                THROW_ERROR_EXCEPTION("Undefined reference %Qv",
                    NAst::FormatColumn(referenceExpr->ColumnName, referenceExpr->TableName));
            }

            return New<TReferenceExpression>(column->Type, column->Name);
        } else if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
            auto functionName = functionExpr->FunctionName;
            auto aggregateFunction = GetAggregate(functionName, functionRegistry);

            if (aggregateFunction) {
                auto subexprName = InferName(functionExpr);

                try {
                    if (functionExpr->Arguments.size() != 1) {
                        THROW_ERROR_EXCEPTION(
                            "Aggregate function %Qv must have exactly one argument",
                            aggregateFunction);
                    }

                    const auto* aggregateColumn = GetAggregateColumnPtr(
                        aggregateFunction.Get(),
                        functionExpr->Arguments.front().Get(),
                        subexprName,
                        source,
                        functionRegistry,
                        aliasMap);

                    return New<TReferenceExpression>(
                        aggregateColumn->Type,
                        aggregateColumn->Name);

                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error creating aggregate")
                        << TErrorAttribute("source", functionExpr->GetSource(source))
                        << ex;
                }
            } else {
                std::vector<EValueType> types;
                std::vector<TConstExpressionPtr> typedOperands;
                for (const auto& argument : functionExpr->Arguments) {
                    auto typedArgument = DoBuildTypedExpression(
                        argument.Get(),
                        source,
                        functionRegistry,
                        aliasMap,
                        usedAliases);
                    types.push_back(typedArgument->Type);
                    typedOperands.push_back(typedArgument);
                }

                return New<TFunctionExpression>(
                    InferFunctionExprType(functionName, types, functionExpr->GetSource(source), functionRegistry),
                    functionName,
                    typedOperands);
            }
        } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
            if (unaryExpr->Operand.size() != 1) {
                THROW_ERROR_EXCEPTION(
                    "Unary operator %Qv must have exactly one argument",
                    unaryExpr->Opcode);
            }

            auto typedOperand = DoBuildTypedExpression(
                unaryExpr->Operand.front().Get(),
                source,
                functionRegistry,
                aliasMap,
                usedAliases);

            if (auto foldedExpr = FoldConstants(unaryExpr, typedOperand)) {
                return foldedExpr;
            } else {
                return New<TUnaryOpExpression>(
                    InferUnaryExprType(
                        unaryExpr->Opcode,
                        typedOperand->Type,
                        unaryExpr->GetSource(source)),
                    unaryExpr->Opcode,
                    typedOperand);
            }
        } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
            auto makeBinaryExpr = [&] (EBinaryOp op, TConstExpressionPtr lhs, TConstExpressionPtr rhs) -> TConstExpressionPtr {
                auto type = InferBinaryExprType(
                    op,
                    lhs->Type,
                    rhs->Type,
                    binaryExpr->GetSource(source),
                    InferName(lhs),
                    InferName(rhs));
                if (auto foldedExpr = FoldConstants(binaryExpr, lhs, rhs)) {
                    return foldedExpr;
                } else {
                    return New<TBinaryOpExpression>(type, op, lhs, rhs);
                }
            };

            std::function<TConstExpressionPtr(int, int, EBinaryOp)> gen = [&] (int offset, int keySize, EBinaryOp op) -> TConstExpressionPtr {
                auto typedLhs = DoBuildTypedExpression(
                    binaryExpr->Lhs[offset].Get(),
                    source,
                    functionRegistry,
                    aliasMap,
                    usedAliases);
                auto typedRhs = DoBuildTypedExpression(
                    binaryExpr->Rhs[offset].Get(),
                    source,
                    functionRegistry,
                    aliasMap,
                    usedAliases);

                if (offset + 1 < keySize) {
                    auto next = gen(offset + 1, keySize, op);
                    auto eq = MakeAndExpression(
                            makeBinaryExpr(EBinaryOp::Equal, typedLhs, typedRhs),
                            next);
                    if (op == EBinaryOp::Less || op == EBinaryOp::LessOrEqual) {
                        return MakeOrExpression(
                            makeBinaryExpr(EBinaryOp::Less, typedLhs, typedRhs),
                            eq);
                    } else if (op == EBinaryOp::Greater || op == EBinaryOp::GreaterOrEqual)  {
                        return MakeOrExpression(
                            makeBinaryExpr(EBinaryOp::Greater, typedLhs, typedRhs),
                            eq);
                    } else {
                        return eq;
                    }
                } else {
                    return makeBinaryExpr(op, typedLhs, typedRhs);
                }
            };

            if (binaryExpr->Opcode == EBinaryOp::Less
                || binaryExpr->Opcode == EBinaryOp::LessOrEqual
                || binaryExpr->Opcode == EBinaryOp::Greater
                || binaryExpr->Opcode == EBinaryOp::GreaterOrEqual
                || binaryExpr->Opcode == EBinaryOp::Equal) {

                if (binaryExpr->Lhs.size() != binaryExpr->Rhs.size()) {
                    THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
                        binaryExpr->Lhs.size(),
                        binaryExpr->Rhs.size())
                        << TErrorAttribute("source", binaryExpr->GetSource(source));
                }

                int keySize = binaryExpr->Lhs.size();
                return gen(0, keySize, binaryExpr->Opcode);
            } else {
                if (binaryExpr->Lhs.size() != 1) {
                    THROW_ERROR_EXCEPTION("Expecting scalar expression")
                        << TErrorAttribute("source", InferName(binaryExpr->Lhs));
                }

                if (binaryExpr->Rhs.size() != 1) {
                    THROW_ERROR_EXCEPTION("Expecting scalar expression")
                        << TErrorAttribute("source", InferName(binaryExpr->Rhs));
                }

                auto typedLhs = DoBuildTypedExpression(
                    binaryExpr->Lhs.front().Get(),
                    source,
                    functionRegistry,
                    aliasMap,
                    usedAliases);
                auto typedRhs = DoBuildTypedExpression(
                    binaryExpr->Rhs.front().Get(),
                    source,
                    functionRegistry,
                    aliasMap,
                    usedAliases);

                return makeBinaryExpr(binaryExpr->Opcode, typedLhs, typedRhs);
            }
        } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
            std::vector<TConstExpressionPtr> typedArguments;
            std::unordered_set<Stroka> references;
            std::vector<EValueType> argTypes;

            for (const auto& argument : inExpr->Expr) {
                auto typedArgument = DoBuildTypedExpression(
                    argument.Get(),
                    source,
                    functionRegistry,
                    aliasMap,
                    usedAliases);

                typedArguments.push_back(typedArgument);
                argTypes.push_back(typedArgument->Type);
                if (auto reference = typedArgument->As<TReferenceExpression>()) {
                    if (references.find(reference->ColumnName) != references.end()) {
                        THROW_ERROR_EXCEPTION("IN operator has multiple references to column %Qv", reference->ColumnName)
                            << TErrorAttribute("source", source);
                    } else {
                        references.insert(reference->ColumnName);
                    }
                }
            }

            auto capturedRows = LiteralTupleListToRows(inExpr->Values, argTypes, inExpr->GetSource(source));
            return New<TInOpExpression>(std::move(typedArguments), std::move(capturedRows));
        }

        YUNREACHABLE();
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

private:
    yhash_map<TPair<Stroka, Stroka>, size_t> Lookup_;

protected:
    virtual const TColumnSchema* AddColumnPtr(const TStringBuf& name, const TStringBuf& tableName)
    {
        return nullptr;
    }

    // NOTE: result must be used before next call
    virtual TColumnSchema AddAggregateColumnPtr(
        const Stroka& aggregateFunction,
        const NAst::TExpression* arguments,
        Stroka subexprName,
        Stroka source,
        IFunctionRegistry* functionRegistry,
        const NAst::TAliasMap& aliasMap)
    {
        THROW_ERROR_EXCEPTION(
            "Misuse of aggregate function %v",
            aggregateFunction);
    }

    static TNullable<Stroka> GetAggregate(
        const TStringBuf& functionName,
        IFunctionRegistryPtr functionRegistry)
    {
        Stroka name(functionName);
        name.to_lower();

        TNullable<Stroka> result;

        if (functionRegistry->FindAggregateFunction(name)) {
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

    static TSharedRange<TRow> LiteralTupleListToRows(
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
                auto valueType = GetType(tuple[i]);
                if (valueType != argTypes[i]) {
                    THROW_ERROR_EXCEPTION("IN operator types mismatch")
                        << TErrorAttribute("source", source)
                        << TErrorAttribute("actual_type", valueType)
                        << TErrorAttribute("expected_type", argTypes[i]);
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
                } else if (opcode == EUnaryOp::BitNot) {
                    TUnversionedValue value = literalExpr->Value;
                    switch (value.Type) {
                        case EValueType::Int64:
                            value.Data.Int64 = ~value.Data.Int64;
                            break;
                        case EValueType::Uint64:
                            value.Data.Uint64 = ~value.Data.Uint64;
                            break;
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
                    case EBinaryOp::LeftShift:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                lhs.Data.Int64 <<= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                lhs.Data.Uint64 <<= rhs.Data.Uint64;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::RightShift:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                lhs.Data.Int64 >>= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                lhs.Data.Uint64 >>= rhs.Data.Uint64;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::BitOr:
                        switch (lhs.Type) {
                            case EValueType::Uint64:
                                lhs.Data.Uint64 = lhs.Data.Uint64 | rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Int64:
                                lhs.Data.Int64 = lhs.Data.Int64 | rhs.Data.Int64;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::BitAnd:
                        switch (lhs.Type) {
                            case EValueType::Uint64:
                                lhs.Data.Uint64 = lhs.Data.Uint64 & rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Int64:
                                lhs.Data.Int64 = lhs.Data.Int64 & rhs.Data.Int64;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::And:
                        switch (lhs.Type) {
                            case EValueType::Uint64:
                                lhs.Data.Uint64 = lhs.Data.Uint64 & rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Int64:
                                lhs.Data.Int64 = lhs.Data.Int64 & rhs.Data.Int64;
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

class TScanSchemaProxy
    : public TSchemaProxy
{
public:
    TScanSchemaProxy(
        TTableSchema* tableSchema,
        TTableSchema* refinedTableSchema,
        const TTableSchema& sourceTableSchema,
        int keyColumnCount = 0,
        const TStringBuf& tableName = TStringBuf())
        : TSchemaProxy(tableSchema)
        , SourceTableSchema_(sourceTableSchema)
        , RefinedTableSchema_(refinedTableSchema)
        , TableName_(tableName)
    {
        const auto& columns = sourceTableSchema.Columns();
        int count = std::min(keyColumnCount, static_cast<int>(columns.size()));
        for (int i = 0; i < count; ++i) {
            GetColumnPtr(columns[i].Name, TableName_);
        }
    }

    virtual const TColumnSchema* AddColumnPtr(const TStringBuf& name, const TStringBuf& tableName) override
    {
        if (tableName != TableName_) {
            return nullptr;
        }

        auto column = SourceTableSchema_.FindColumn(name);
        if (column) {
            RefinedTableSchema_->Columns().push_back(*column);
        }
        return column;
    }

    virtual void Finish() override
    {
        for (const auto& column : SourceTableSchema_.Columns()) {
            GetColumnPtr(column.Name, TableName_);
        }
    }

private:
    const TTableSchema SourceTableSchema_;
    TTableSchema* RefinedTableSchema_;
    TStringBuf TableName_;

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

    virtual const TColumnSchema* AddColumnPtr(const TStringBuf& name, const TStringBuf& tableName) override
    {
        const TColumnSchema* column = nullptr;

        if ((column = Self_->GetColumnPtr(name, tableName))) {
            if (Foreign_->GetColumnPtr(name, tableName)) {
                THROW_ERROR_EXCEPTION("Column %Qv occurs both in main and joined tables",
                    NAst::FormatColumn(name, tableName));
            }
        } else {
            column = Foreign_->GetColumnPtr(name, tableName);
        }

        return column;
    }

    virtual void Finish() override
    {
        Self_->Finish();
        Foreign_->Finish();

        for (const auto& column : Self_->GetLookup()) {
            GetColumnPtr(column.first.first, column.first.second);
        }

        for (const auto& column : Foreign_->GetLookup()) {
            GetColumnPtr(column.first.first, column.first.second);
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

    virtual const TColumnSchema* AddColumnPtr(const TStringBuf& name, const TStringBuf& tableName) override
    {
        return nullptr;
    }

    virtual TColumnSchema AddAggregateColumnPtr(
        const Stroka& aggregateFunction,
        const NAst::TExpression* argument,
        Stroka subexprName,
        Stroka source,
        IFunctionRegistry* functionRegistry,
        const NAst::TAliasMap& aliasMap) override
    {
        auto typedOperand = Base_->BuildTypedExpression(
            argument,
            source,
            functionRegistry,
            aliasMap);

        auto descriptor = functionRegistry
            ->GetAggregateFunction(aggregateFunction);
        auto stateType = descriptor
            ->GetStateType(typedOperand->Type);
        auto resultType = descriptor
            ->InferResultType(typedOperand->Type, source);

        CheckExpressionDepth(typedOperand);

        AggregateItems_->emplace_back(
            typedOperand,
            aggregateFunction,
            subexprName,
            stateType,
            resultType);

        return TColumnSchema(subexprName, resultType);
    }

private:
    TSchemaProxyPtr Base_;
    TAggregateItemList* AggregateItems_;

};

TConstExpressionPtr BuildWhereClause(
    const NAst::TExpressionList& expressionAst,
    const Stroka& source,
    const TSchemaProxyPtr& schemaProxy,
    IFunctionRegistry* functionRegistry,
    const NAst::TAliasMap& aliasMap)
{
    if (expressionAst.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            << TErrorAttribute("source", InferName(expressionAst));
    }

    auto typedPredicate = schemaProxy->BuildTypedExpression(
        expressionAst.front().Get(),
        source,
        functionRegistry,
        aliasMap);

    CheckExpressionDepth(typedPredicate);

    auto actualType = typedPredicate->Type;
    EValueType expectedType(EValueType::Boolean);
    if (actualType != expectedType) {
        THROW_ERROR_EXCEPTION("WHERE-clause is not a boolean expression")
            << TErrorAttribute("source", InferName(expressionAst))
            << TErrorAttribute("actual_type", actualType)
            << TErrorAttribute("expected_type", expectedType);
    }

    return typedPredicate;
}

TConstGroupClausePtr BuildGroupClause(
    const NAst::TExpressionList& expressionsAst,
    const Stroka& source,
    TSchemaProxyPtr& schemaProxy,
    IFunctionRegistry* functionRegistry,
    const NAst::TAliasMap& aliasMap)
{
    auto groupClause = New<TGroupClause>();
    groupClause->IsMerge = false;
    groupClause->IsFinal = true;

    for (const auto& expressionAst : expressionsAst) {
        auto typedExpr = schemaProxy->BuildTypedExpression(
            expressionAst.Get(),
            source,
            functionRegistry,
            aliasMap);

        CheckExpressionDepth(typedExpr);
        groupClause->AddGroupItem(typedExpr, InferName(expressionAst.Get()));
    }

    TTableSchema& tableSchema = groupClause->GroupedTableSchema;
    ValidateTableSchema(tableSchema);
    schemaProxy = New<TGroupSchemaProxy>(&tableSchema, std::move(schemaProxy), &groupClause->AggregateItems);

    return groupClause;
}

TConstExpressionPtr BuildHavingClause(
    const NAst::TExpressionList& expressionsAst,
    const Stroka& source,
    const TSchemaProxyPtr& schemaProxy,
    IFunctionRegistry* functionRegistry,
    const NAst::TAliasMap& aliasMap)
{
    if (expressionsAst.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            << TErrorAttribute("source", InferName(expressionsAst));
    }

    auto typedPredicate = schemaProxy->BuildTypedExpression(
        expressionsAst.front().Get(),
        source,
        functionRegistry,
        aliasMap);

    CheckExpressionDepth(typedPredicate);

    auto actualType = typedPredicate->Type;
    EValueType expectedType(EValueType::Boolean);
    if (actualType != expectedType) {
        THROW_ERROR_EXCEPTION("HAVING-clause is not a boolean expression")
            << TErrorAttribute("actual_type", actualType)
            << TErrorAttribute("expected_type", expectedType);
    }

    return typedPredicate;
}

TConstProjectClausePtr BuildProjectClause(
    const NAst::TExpressionList& expressionsAst,
    const Stroka& source,
    TSchemaProxyPtr& schemaProxy,
    IFunctionRegistry* functionRegistry,
    const NAst::TAliasMap& aliasMap)
{
    auto projectClause = New<TProjectClause>();

    for (const auto& expressionAst : expressionsAst) {
        auto typedExpr = schemaProxy->BuildTypedExpression(
            expressionAst.Get(),
            source,
            functionRegistry,
            aliasMap);

        CheckExpressionDepth(typedExpr);
        projectClause->AddProjection(typedExpr, InferName(expressionAst.Get()));
    }

    ValidateTableSchema(projectClause->ProjectTableSchema);
    schemaProxy = New<TSchemaProxy>(&projectClause->ProjectTableSchema);

    return projectClause;
}

void PrepareQuery(
    const TQueryPtr& query,
    const NAst::TQuery& ast,
    const Stroka& source,
    TSchemaProxyPtr& schemaProxy,
    IFunctionRegistry* functionRegistry,
    const NAst::TAliasMap& aliasMap)
{
    if (ast.WherePredicate) {
        query->WhereClause = BuildWhereClause(
            ast.WherePredicate.Get(),
            source, schemaProxy,
            functionRegistry,
            aliasMap);
    }

    if (ast.GroupExprs) {
        query->GroupClause = BuildGroupClause(
            ast.GroupExprs.Get(),
            source,
            schemaProxy,
            functionRegistry,
            aliasMap);
    }

    if (ast.HavingPredicate) {
        if (!query->GroupClause) {
            THROW_ERROR_EXCEPTION("Expected GROUP BY before HAVING");
        }
        query->HavingClause = BuildHavingClause(
            ast.HavingPredicate.Get(),
            source,
            schemaProxy,
            functionRegistry,
            aliasMap);
    }

    if (ast.OrderFields) {
        auto orderClause = New<TOrderClause>();
        orderClause->IsDescending = ast.IsDescendingOrder;
        for (const auto& reference : ast.OrderFields.Get()) {
            const auto* column = schemaProxy->GetColumnPtr(reference->ColumnName, reference->TableName);
            if (!column) {
                THROW_ERROR_EXCEPTION("Undefined reference %Qv",
                    NAst::FormatColumn(reference->ColumnName, reference->TableName));
            }

            orderClause->OrderColumns.push_back(column->Name);
        }

        query->OrderClause = std::move(orderClause);
    }

    if (ast.SelectExprs) {
        query->ProjectClause = BuildProjectClause(
            ast.SelectExprs.Get(),
            source,
            schemaProxy,
            functionRegistry,
            aliasMap);
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
    IFunctionRegistryPtr functionRegistry,
    i64 inputRowLimit,
    i64 outputRowLimit,
    TTimestamp timestamp)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>(), NAst::TAliasMap()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseQuery);

    auto& ast = astHead.first.As<NAst::TQuery>();

    TDataSplit selfDataSplit;

    auto query = New<TQuery>(inputRowLimit, outputRowLimit, TGuid::Create());
    TSchemaProxyPtr schemaProxy;

    auto table = ast.Table;
    LOG_DEBUG("Getting initial data split for %v", table.Path);

    selfDataSplit = WaitFor(callbacks->GetInitialSplit(table.Path, timestamp))
        .ValueOrThrow();
    auto tableSchema = GetTableSchemaFromDataSplit(selfDataSplit);
    auto keyColumns = GetKeyColumnsFromDataSplit(selfDataSplit);

    std::vector<Stroka> refinedColumns;

    query->KeyColumnsCount = keyColumns.size();
    schemaProxy = New<TScanSchemaProxy>(
        &query->RenamedTableSchema,
        &query->TableSchema,
        tableSchema,
        keyColumns.size(),
        table.Alias);

    for (const auto& join : ast.Joins) {
        auto foreignDataSplit = WaitFor(callbacks->GetInitialSplit(join.Table.Path, timestamp))
            .ValueOrThrow();

        auto foreignTableSchema = GetTableSchemaFromDataSplit(foreignDataSplit);
        auto foreignKeyColumns = GetKeyColumnsFromDataSplit(foreignDataSplit);

        auto joinClause = New<TJoinClause>();

        joinClause->ForeignKeyColumnsCount = foreignKeyColumns.size();
        joinClause->ForeignDataId = GetObjectIdFromDataSplit(foreignDataSplit);

        auto foreignSourceProxy = New<TScanSchemaProxy>(
            &joinClause->RenamedTableSchema,
            &joinClause->ForeignTableSchema,
            foreignTableSchema,
            foreignKeyColumns.size(),
            join.Table.Alias);

        // Merge columns.
        for (const auto& reference : join.Fields) {
            const auto* selfColumn = schemaProxy->GetColumnPtr(
                reference->ColumnName,
                reference->TableName);
            const auto* foreignColumn = foreignSourceProxy->GetColumnPtr(
                reference->ColumnName,
                reference->TableName);

            if (!selfColumn || !foreignColumn) {
                THROW_ERROR_EXCEPTION("Column %Qv not found",
                    NAst::FormatColumn(reference->ColumnName, reference->TableName));
            }

            if (selfColumn->Type != foreignColumn->Type) {
                THROW_ERROR_EXCEPTION("Column type %Qv mismatch",
                    NAst::FormatColumn(reference->ColumnName, reference->TableName))
                    << TErrorAttribute("self_type", selfColumn->Type)
                    << TErrorAttribute("foreign_type", foreignColumn->Type);
            }

            joinClause->Equations.emplace_back(
                New<TReferenceExpression>(selfColumn->Type, selfColumn->Name),
                New<TReferenceExpression>(foreignColumn->Type, foreignColumn->Name));

            joinClause->JoinedTableSchema.Columns().push_back(*selfColumn);
        }

        std::vector<TConstExpressionPtr> leftEquations;
        std::vector<TConstExpressionPtr> rightEquations;

        for (const auto& argument : join.Left) {
            leftEquations.push_back(schemaProxy->BuildTypedExpression(
                argument.Get(),
                source,
                functionRegistry.Get(),
                astHead.second));
        }

        for (const auto& argument : join.Right) {
            rightEquations.push_back(foreignSourceProxy->BuildTypedExpression(
                argument.Get(),
                source,
                functionRegistry.Get(),
                astHead.second));
        }

        if (leftEquations.size() != rightEquations.size()) {
            THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
                leftEquations.size(),
                rightEquations.size())
                << TErrorAttribute("lhs_source", InferName(join.Left))
                << TErrorAttribute("rhs_source", InferName(join.Right));
        }

        for (size_t index = 0; index < leftEquations.size(); ++index) {
            if (leftEquations[index]->Type != rightEquations[index]->Type) {
                THROW_ERROR_EXCEPTION("Types mismatch in join equation \"%v = %v\"",
                    InferName(leftEquations[index]),
                    InferName(rightEquations[index]))
                    << TErrorAttribute("self_type", leftEquations[index]->Type)
                    << TErrorAttribute("foreign_type", rightEquations[index]->Type);
            }

            joinClause->Equations.emplace_back(leftEquations[index], rightEquations[index]);
        }

        schemaProxy = New<TJoinSchemaProxy>(
            &joinClause->JoinedTableSchema,
            schemaProxy,
            foreignSourceProxy);

        query->JoinClauses.push_back(std::move(joinClause));
    }

    PrepareQuery(query, ast, source, schemaProxy, functionRegistry.Get(), astHead.second);

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

    return planFragment;
}

TParsedQueryInfo PrepareJobQueryAst(const Stroka& source)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>(), NAst::TAliasMap()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseJobQuery);

    auto& ast = astHead.first.As<NAst::TQuery>();

    if (ast.Limit) {
        THROW_ERROR_EXCEPTION("LIMIT is not supported in map-reduce queries");
    }

    if (ast.GroupExprs) {
        THROW_ERROR_EXCEPTION("GROUP BY is not supported in map-reduce queries");
    }

    return std::make_pair(ast, astHead.second);
}

void GetExternalFunctions(
    const NAst::TNullableExpressionList& exprs,
    IFunctionRegistryPtr builtinRegistry,
    std::vector<Stroka>* externalFunctions);

void GetExternalFunctions(
    const NAst::TExpressionPtr& expr,
    IFunctionRegistryPtr builtinRegistry,
    std::vector<Stroka>* externalFunctions)
{
    if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
        const auto& name = functionExpr->FunctionName;
        if (!builtinRegistry->FindFunction(name) &&
            !builtinRegistry->FindAggregateFunction(name))
        {
            externalFunctions->push_back(name);
        }
        GetExternalFunctions(functionExpr->Arguments, builtinRegistry, externalFunctions);
    } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
        GetExternalFunctions(unaryExpr->Operand, builtinRegistry, externalFunctions);
    } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
        GetExternalFunctions(binaryExpr->Lhs, builtinRegistry, externalFunctions);
        GetExternalFunctions(binaryExpr->Rhs, builtinRegistry, externalFunctions);
    } else if (expr->As<NAst::TInExpression>()) {
    } else if (expr->As<NAst::TLiteralExpression>()) {
    } else if (expr->As<NAst::TReferenceExpression>()) {
    } else {
        YUNREACHABLE();
    }
}

void GetExternalFunctions(
    const NAst::TNullableExpressionList& exprs,
    IFunctionRegistryPtr builtinRegistry,
    std::vector<Stroka>* externalFunctions)
{
    if (!exprs) {
        return;
    }

    for (const auto& expr : exprs.Get()) {
        GetExternalFunctions(expr, builtinRegistry, externalFunctions);
    }
}

std::vector<Stroka> GetExternalFunctions(
    const TParsedQueryInfo& parsedQueryInfo,
    IFunctionRegistryPtr builtinRegistry)
{
    std::vector<Stroka> externalFunctions;

    GetExternalFunctions(parsedQueryInfo.first.WherePredicate, builtinRegistry, &externalFunctions);
    GetExternalFunctions(parsedQueryInfo.first.HavingPredicate, builtinRegistry, &externalFunctions);
    GetExternalFunctions(parsedQueryInfo.first.SelectExprs, builtinRegistry, &externalFunctions);
    GetExternalFunctions(parsedQueryInfo.first.GroupExprs, builtinRegistry, &externalFunctions);

    for (const auto& aliasedExpression : parsedQueryInfo.second) {
        GetExternalFunctions(aliasedExpression.second.Get(), builtinRegistry, &externalFunctions);
    }

    std::sort(externalFunctions.begin(), externalFunctions.end());
    externalFunctions.erase(
        std::unique(externalFunctions.begin(), externalFunctions.end()),
        externalFunctions.end());

    return externalFunctions;
}

TQueryPtr PrepareJobQuery(
    const Stroka& source,
    const TParsedQueryInfo& parsedQueryInfo,
    const TTableSchema& tableSchema,
    IFunctionRegistryPtr functionRegistry)
{
    auto planFragment = New<TPlanFragment>(source);
    auto unlimited = std::numeric_limits<i64>::max();

    auto query = New<TQuery>(unlimited, unlimited, TGuid::Create());
    TSchemaProxyPtr schemaProxy = New<TScanSchemaProxy>(
        &query->RenamedTableSchema,
        &query->TableSchema,
        tableSchema);

    PrepareQuery(
        query,
        parsedQueryInfo.first,
        source,
        schemaProxy,
        functionRegistry.Get(),
        parsedQueryInfo.second);

    return query;
}

TConstExpressionPtr PrepareExpression(
    const Stroka& source,
    TTableSchema tableSchema,
    IFunctionRegistryPtr functionRegistry)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TExpressionPtr>(), NAst::TAliasMap()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseExpression);

    auto& expr = astHead.first.As<NAst::TExpressionPtr>();

    auto schemaProxy = New<TSchemaProxy>(&tableSchema);

    return schemaProxy->BuildTypedExpression(
        expr.Get(),
        source,
        functionRegistry.Get(),
        astHead.second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
