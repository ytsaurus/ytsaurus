#include "query_preparer.h"
#include "private.h"
#include "callbacks.h"
#include "functions.h"
#include "helpers.h"
#include "lexer.h"
#include "parser.hpp"
#include "query_helpers.h"

#include <yt/ytlib/chunk_client/chunk_spec.pb.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/misc/collection_helpers.h>

#include <unordered_set>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NTableClient;

static const int PlanFragmentDepthLimit = 50;

struct TQueryPreparerBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

namespace {

typedef std::pair<NAst::TQuery, NAst::TAliasMap> TParsedQueryInfo;

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<TString>* functions);

void ExtractFunctionNames(
    const NAst::TExpressionPtr& expr,
    std::vector<TString>* functions)
{
    if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
        functions->push_back(to_lower(functionExpr->FunctionName));
        ExtractFunctionNames(functionExpr->Arguments, functions);
    } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
        ExtractFunctionNames(unaryExpr->Operand, functions);
    } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
        ExtractFunctionNames(binaryExpr->Lhs, functions);
        ExtractFunctionNames(binaryExpr->Rhs, functions);
    } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
        ExtractFunctionNames(inExpr->Expr, functions);
    } else if (expr->As<NAst::TLiteralExpression>()) {
    } else if (expr->As<NAst::TReferenceExpression>()) {
    } else {
        Y_UNREACHABLE();
    }
}

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<TString>* functions)
{
    if (!exprs) {
        return;
    }

    for (const auto& expr : exprs.Get()) {
        ExtractFunctionNames(expr, functions);
    }
}

std::vector<TString> ExtractFunctionNames(
    const TParsedQueryInfo& parsedQueryInfo)
{
    std::vector<TString> functions;

    ExtractFunctionNames(parsedQueryInfo.first.WherePredicate, &functions);
    ExtractFunctionNames(parsedQueryInfo.first.HavingPredicate, &functions);
    ExtractFunctionNames(parsedQueryInfo.first.SelectExprs, &functions);

    if (auto groupExprs = parsedQueryInfo.first.GroupExprs.GetPtr()) {
        for (const auto& expr : groupExprs->first) {
            ExtractFunctionNames(expr, &functions);
        }
    }

    for (const auto& join : parsedQueryInfo.first.Joins) {
        ExtractFunctionNames(join.Left, &functions);
        ExtractFunctionNames(join.Right, &functions);
    }

    for (const auto& orderExpression : parsedQueryInfo.first.OrderExpressions) {
        for (const auto& expr : orderExpression.first) {
            ExtractFunctionNames(expr, &functions);
        }
    }

    for (const auto& aliasedExpression : parsedQueryInfo.second) {
        ExtractFunctionNames(aliasedExpression.second.Get(), &functions);
    }

    std::sort(functions.begin(), functions.end());
    functions.erase(
        std::unique(functions.begin(), functions.end()),
        functions.end());

    return functions;
}

////////////////////////////////////////////////////////////////////////////////

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
    Y_UNREACHABLE();
};

TValue CastValueWithCheck(TValue value, EValueType targetType)
{
    if (value.Type == targetType || value.Type == EValueType::Null) {
        return value;
    }

    if (value.Type == EValueType::Int64) {
        if (targetType == EValueType::Uint64) {
            if (value.Data.Int64 < 0) {
                THROW_ERROR_EXCEPTION("Failed to cast %v to uint64: value is negative", value.Data.Int64);
            }
        } else if (targetType == EValueType::Double) {
            auto int64Value = value.Data.Int64;
            if (i64(double(int64Value)) != int64Value) {
                THROW_ERROR_EXCEPTION("Failed to cast %v to double: inaccurate conversion", int64Value);
            }
            value.Data.Double = int64Value;
        } else {
            Y_UNREACHABLE();
        }
    } else if (value.Type == EValueType::Uint64) {
        if (targetType == EValueType::Int64) {
            if (value.Data.Uint64 > std::numeric_limits<i64>::max()) {
                THROW_ERROR_EXCEPTION(
                    "Failed to cast %vu to int64: value is greater than maximum", value.Data.Uint64);
            }
        } else if (targetType == EValueType::Double) {
            auto uint64Value = value.Data.Uint64;
            if (ui64(double(uint64Value)) != uint64Value) {
                THROW_ERROR_EXCEPTION("Failed to cast %vu to double: inaccurate conversion", uint64Value);
            }
            value.Data.Double = uint64Value;
        } else {
            Y_UNREACHABLE();
        }
    } else if (value.Type == EValueType::Double) {
        auto doubleValue = value.Data.Double;
        if (targetType == EValueType::Uint64) {
            if (double(ui64(doubleValue)) != doubleValue) {
                THROW_ERROR_EXCEPTION("Failed to cast %v to uint64: inaccurate conversion", doubleValue);
            }
            value.Data.Uint64 = doubleValue;
        } else if (targetType == EValueType::Int64) {
            if (double(i64(doubleValue)) != doubleValue) {
                THROW_ERROR_EXCEPTION("Failed to cast %v to int64: inaccurate conversion", doubleValue);
            }
            value.Data.Int64 = doubleValue;
        } else {
            Y_UNREACHABLE();
        }
    } else {
        Y_UNREACHABLE();
    }

    value.Type = targetType;
    return value;
}

EValueType GetType(const NAst::TLiteralValue& literalValue)
{
    switch (literalValue.Tag()) {
        case NAst::TLiteralValue::TagOf<NAst::TNullLiteralValue>():
            return EValueType::Null;
        case NAst::TLiteralValue::TagOf<i64>():
            return EValueType::Int64;
        case NAst::TLiteralValue::TagOf<ui64>():
            return EValueType::Uint64;
        case NAst::TLiteralValue::TagOf<double>():
            return EValueType::Double;
        case NAst::TLiteralValue::TagOf<bool>():
            return EValueType::Boolean;
        case NAst::TLiteralValue::TagOf<TString>():
            return EValueType::String;
        default:
            Y_UNREACHABLE();
    }
}

TTypeSet GetTypes(const NAst::TLiteralValue& literalValue)
{
    switch (literalValue.Tag()) {
        case NAst::TLiteralValue::TagOf<NAst::TNullLiteralValue>():
            return TTypeSet({
                EValueType::Null,
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::String,
                EValueType::Any});
        case NAst::TLiteralValue::TagOf<i64>():
            return TTypeSet({
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double});
        case NAst::TLiteralValue::TagOf<ui64>():
            return TTypeSet({
                EValueType::Uint64,
                EValueType::Double});
        case NAst::TLiteralValue::TagOf<double>():
            return TTypeSet({EValueType::Double});
        case NAst::TLiteralValue::TagOf<bool>():
            return TTypeSet({EValueType::Boolean});
        case NAst::TLiteralValue::TagOf<TString>():
            return TTypeSet({EValueType::String});
        default:
            Y_UNREACHABLE();
    }
}

TValue GetValue(const NAst::TLiteralValue& literalValue)
{
    switch (literalValue.Tag()) {
        case NAst::TLiteralValue::TagOf<NAst::TNullLiteralValue>():
            return MakeUnversionedSentinelValue(EValueType::Null);
        case NAst::TLiteralValue::TagOf<i64>():
            return MakeUnversionedInt64Value(literalValue.As<i64>());
        case NAst::TLiteralValue::TagOf<ui64>():
            return MakeUnversionedUint64Value(literalValue.As<ui64>());
        case NAst::TLiteralValue::TagOf<double>():
            return MakeUnversionedDoubleValue(literalValue.As<double>());
        case NAst::TLiteralValue::TagOf<bool>():
            return MakeUnversionedBooleanValue(literalValue.As<bool>());
        case NAst::TLiteralValue::TagOf<TString>():
            return MakeUnversionedStringValue(
                literalValue.As<TString>().c_str(),
                literalValue.As<TString>().length());
        default:
            Y_UNREACHABLE();
    }
}

TSharedRange<TRow> LiteralTupleListToRows(
    const NAst::TLiteralValueTupleList& literalTuples,
    const std::vector<EValueType>& argTypes,
    const TStringBuf& source)
{
    auto rowBuffer = New<TRowBuffer>(TQueryPreparerBufferTag());
    TUnversionedRowBuilder rowBuilder;
    std::vector<TRow> rows;
    for (const auto& tuple : literalTuples) {
        if (tuple.size() != argTypes.size()) {
            THROW_ERROR_EXCEPTION("IN operator arguments size mismatch")
                << TErrorAttribute("source", source);
        }
        for (int i = 0; i < tuple.size(); ++i) {
            auto valueType = GetType(tuple[i]);
            auto value = GetValue(tuple[i]);

            if (valueType == EValueType::Null) {
                value = MakeUnversionedSentinelValue(EValueType::Null);
            } else if (valueType != argTypes[i]) {
                if (IsArithmeticType(valueType) && IsArithmeticType(argTypes[i])) {
                    value = CastValueWithCheck(value, argTypes[i]);
                } else {
                    THROW_ERROR_EXCEPTION("IN operator types mismatch")
                    << TErrorAttribute("source", source)
                    << TErrorAttribute("actual_type", valueType)
                    << TErrorAttribute("expected_type", argTypes[i]);
                }
            }
            rowBuilder.AddValue(value);
        }
        rows.push_back(rowBuffer->Capture(rowBuilder.GetRow()));
        rowBuilder.Reset();
    }

    std::sort(rows.begin(), rows.end());
    return MakeSharedRange(std::move(rows), std::move(rowBuffer));
}

TNullable<TUnversionedValue> FoldConstants(
    EUnaryOp opcode,
    TConstExpressionPtr operand)
{
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
                    Y_UNREACHABLE();
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
                default:
                    Y_UNREACHABLE();
            }
            return value;
        }
    }
    return Null;
}

TNullable<TUnversionedValue> FoldConstants(
    EBinaryOp opcode,
    TConstExpressionPtr lhsExpr,
    TConstExpressionPtr rhsExpr)
{
    auto lhsLiteral = lhsExpr->As<TLiteralExpression>();
    auto rhsLiteral = rhsExpr->As<TLiteralExpression>();
    if (lhsLiteral && rhsLiteral) {
        auto lhs = static_cast<TUnversionedValue>(lhsLiteral->Value);
        auto rhs = static_cast<TUnversionedValue>(rhsLiteral->Value);

        auto checkType = [&] () {
            if (lhs.Type != rhs.Type) {
                if (IsArithmeticType(lhs.Type) && IsArithmeticType(rhs.Type)) {
                    auto targetType = std::max(lhs.Type, rhs.Type);
                    lhs = CastValueWithCheck(lhs, targetType);
                    rhs = CastValueWithCheck(rhs, targetType);
                } else {
                    ThrowTypeMismatchError(lhs.Type, rhs.Type, "", InferName(lhsExpr), InferName(rhsExpr));
                }
            }
        };

        auto checkTypeIfNotNull = [&] () {
            if (lhs.Type != EValueType::Null && rhs.Type != EValueType::Null) {
                checkType();
            }
        };

        #define CHECK_TYPE() \
            if (lhs.Type == EValueType::Null) { \
                return MakeUnversionedSentinelValue(EValueType::Null); \
            } \
            if (rhs.Type == EValueType::Null) { \
                return MakeUnversionedSentinelValue(EValueType::Null); \
            } \
            checkType();

        auto evaluateLogicalOp = [&] (bool parameter) {
            YCHECK(lhs.Type == EValueType::Null || lhs.Type == EValueType::Boolean);
            YCHECK(rhs.Type == EValueType::Null || rhs.Type == EValueType::Boolean);

            if (lhs.Type == EValueType::Null) {
                if (rhs.Type != EValueType::Null && rhs.Data.Boolean == parameter) {
                    return rhs;
                } else {
                    return lhs;
                }
            } else if (lhs.Data.Boolean == parameter) {
                return lhs;
            } else {
                return rhs;
            }
        };

        switch (opcode) {
            case EBinaryOp::Plus:
                CHECK_TYPE();
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
                CHECK_TYPE();
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
                CHECK_TYPE();
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
                CHECK_TYPE();
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
                        lhs.Data.Double /= rhs.Data.Double;
                        return lhs;
                    default:
                        break;
                }
                break;
            case EBinaryOp::Modulo:
                CHECK_TYPE();
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
                CHECK_TYPE();
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
                CHECK_TYPE();
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
                CHECK_TYPE();
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
                CHECK_TYPE();
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
                return evaluateLogicalOp(false);
                break;
            case EBinaryOp::Or:
                return evaluateLogicalOp(true);
                break;
            case EBinaryOp::Equal:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) == 0);
                break;
            case EBinaryOp::NotEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) != 0);
                break;
            case EBinaryOp::Less:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) < 0);
                break;
            case EBinaryOp::Greater:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) > 0);
                break;
            case EBinaryOp::LessOrEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) <= 0);
                break;
            case EBinaryOp::GreaterOrEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) >= 0);
                break;
            default:
                break;
        }
    }
    return Null;
}

template <class TResult, class TDerived>
struct TBaseVisitor
{
    TDerived* Derived()
    {
        return static_cast<TDerived*>(this);
    }

    TResult Visit(TConstExpressionPtr expr)
    {
        if (auto referenceExpr = expr->As<TReferenceExpression>()) {
            return Derived()->OnReference(referenceExpr);
        } else if (auto literalExpr = expr->As<TLiteralExpression>()) {
            return Derived()->OnLiteral(literalExpr);
        } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
            return Derived()->OnUnary(unaryOp);
        } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
            return Derived()->OnBinary(binaryOp);
        } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
            return Derived()->OnFunction(functionExpr);
        } else if (auto inExpr = expr->As<TInOpExpression>()) {
            return Derived()->OnIn(inExpr);
        }
        Y_UNREACHABLE();
    }
};

template <class TDerived>
struct TVisitor
    : public TBaseVisitor<void, TDerived>
{
    using TBase = TBaseVisitor<TConstExpressionPtr, TDerived>;
    using TBase::Derived;
    using TBase::Visit;

    void OnLiteral(const TLiteralExpression* literalExpr)
    { }

    void OnReference(const TReferenceExpression* referenceExpr)
    { }

    void OnUnary(const TUnaryOpExpression* unaryExpr)
    {
        Derived()->Visit(unaryExpr->Operand);
    }

    void OnBinary(const TBinaryOpExpression* binaryExpr)
    {
        Derived()->Visit(binaryExpr->Lhs);
        Derived()->Visit(binaryExpr->Rhs);
    }

    void OnFunction(const TFunctionExpression* functionExpr)
    {
        for (auto argument : functionExpr->Arguments) {
            Derived()->Visit(argument);
        }
    }

    void OnIn(const TInOpExpression* inExpr)
    {
        for (auto argument : inExpr->Arguments) {
            Derived()->Visit(argument);
        }
    }

};

template <class TDerived>
struct TRewriter
    : public TBaseVisitor<TConstExpressionPtr, TDerived>
{
    using TBase = TBaseVisitor<TConstExpressionPtr, TDerived>;
    using TBase::Derived;
    using TBase::Visit;

    TConstExpressionPtr OnLiteral(const TLiteralExpression* literalExpr)
    {
        return literalExpr;
    }

    TConstExpressionPtr OnReference(const TReferenceExpression* referenceExpr)
    {
        return referenceExpr;
    }

    TConstExpressionPtr OnUnary(const TUnaryOpExpression* unaryExpr)
    {
        auto newOperand = Visit(unaryExpr->Operand);

        if (newOperand == unaryExpr->Operand) {
            return unaryExpr;
        }

        return New<TUnaryOpExpression>(
            unaryExpr->Type,
            unaryExpr->Opcode,
            newOperand);
    }

    TConstExpressionPtr OnBinary(const TBinaryOpExpression* binaryExpr)
    {
        auto newLhs = Visit(binaryExpr->Lhs);
        auto newRhs = Visit(binaryExpr->Rhs);

        if (newLhs == binaryExpr->Lhs && newRhs == binaryExpr->Rhs) {
            return binaryExpr;
        }

        return New<TBinaryOpExpression>(
            binaryExpr->Type,
            binaryExpr->Opcode,
            newLhs,
            newRhs);
    }

    TConstExpressionPtr OnFunction(const TFunctionExpression* functionExpr)
    {
        std::vector<TConstExpressionPtr> newArguments;
        bool allEqual = true;
        for (auto argument : functionExpr->Arguments) {
            auto newArgument = Visit(argument);
            allEqual = allEqual && newArgument == argument;
            newArguments.push_back(newArgument);
        }

        if (allEqual) {
            return functionExpr;
        }

        return New<TFunctionExpression>(
            functionExpr->Type,
            functionExpr->FunctionName,
            std::move(newArguments));
    }

    TConstExpressionPtr OnIn(const TInOpExpression* inExpr)
    {
        std::vector<TConstExpressionPtr> newArguments;
        bool allEqual = true;
        for (auto argument : inExpr->Arguments) {
            auto newArgument = Visit(argument);
            allEqual = allEqual && newArgument == argument;
            newArguments.push_back(newArgument);
        }

        if (allEqual) {
            return inExpr;
        }

        return New<TInOpExpression>(
            std::move(newArguments),
            inExpr->Values);
    }

};

struct TNotExpressionPropagator
    : TRewriter<TNotExpressionPropagator>
{
    using TBase = TRewriter<TNotExpressionPropagator>;

    TConstExpressionPtr OnUnary(const TUnaryOpExpression* unaryExpr)
    {
        auto& operand = unaryExpr->Operand;
        if (unaryExpr->Opcode == EUnaryOp::Not) {
            if (auto operandUnaryOp = operand->As<TUnaryOpExpression>()) {
                if (operandUnaryOp->Opcode == EUnaryOp::Not) {
                    return Visit(operandUnaryOp->Operand);
                }
            } else if (auto operandBinaryOp = operand->As<TBinaryOpExpression>()) {
                if (operandBinaryOp->Opcode == EBinaryOp::And) {
                    return Visit(New<TBinaryOpExpression>(
                        EValueType::Boolean,
                        EBinaryOp::Or,
                        New<TUnaryOpExpression>(
                            operandBinaryOp->Lhs->Type,
                            EUnaryOp::Not,
                            operandBinaryOp->Lhs),
                        New<TUnaryOpExpression>(
                            operandBinaryOp->Rhs->Type,
                            EUnaryOp::Not,
                            operandBinaryOp->Rhs)));
                } else if (operandBinaryOp->Opcode == EBinaryOp::Or) {
                    return Visit(New<TBinaryOpExpression>(
                        EValueType::Boolean,
                        EBinaryOp::And,
                        New<TUnaryOpExpression>(
                            operandBinaryOp->Lhs->Type,
                            EUnaryOp::Not,
                            operandBinaryOp->Lhs),
                        New<TUnaryOpExpression>(
                            operandBinaryOp->Rhs->Type,
                            EUnaryOp::Not,
                            operandBinaryOp->Rhs)));
                } else if (IsRelationalBinaryOp(operandBinaryOp->Opcode)) {
                    return Visit(New<TBinaryOpExpression>(
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

        return TBase::OnUnary(unaryExpr);
    }
};

struct TTypedExpressionBuilder;

typedef std::function<TConstExpressionPtr(EValueType)> TExpressionGenerator;

struct TUntypedExpression
{
    TTypeSet FeasibleTypes;
    TExpressionGenerator Generator;
    bool IsConstant;
};

DECLARE_REFCOUNTED_CLASS(ISchemaProxy)

class ISchemaProxy
    : public TIntrinsicRefCounted
{
public:
    virtual TNullable<TBaseColumn> GetColumnPtr(
        const TString& name,
        const TString& tableName) = 0;

    virtual TUntypedExpression GetAggregateColumnPtr(
        const TString& name,
        const TAggregateTypeInferrer* aggregateFunction,
        const NAst::TExpression* arguments,
        TString subexprName,
        const TTypedExpressionBuilder& builder) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemaProxy)

bool Unify(TTypeSet* genericAssignments, const TTypeSet& types)
{
    auto intersection = *genericAssignments & types;

    if (intersection.IsEmpty()) {
        return false;
    } else {
        *genericAssignments = intersection;
        return true;
    }
}

TTypeSet InferFunctionTypes(
    const TFunctionTypeInferrer* inferrer,
    const std::vector<TTypeSet>& effectiveTypes,
    std::vector<TTypeSet>* genericAssignments,
    const TStringBuf& functionName,
    const TStringBuf& source)
{
    std::vector<TTypeSet> typeConstraints;
    std::vector<size_t> formalArguments;
    TNullable<std::pair<size_t, bool>> repeatedType;
    size_t formalResultType = inferrer->GetNormalizedConstraints(
        &typeConstraints,
        &formalArguments,
        &repeatedType);

    *genericAssignments = typeConstraints;

    auto argIndex = 1;
    auto arg = effectiveTypes.begin();
    auto formalArg = formalArguments.begin();
    for (;
        formalArg != formalArguments.end() && arg != effectiveTypes.end();
        arg++, formalArg++, argIndex++)
    {
        auto& constraints = (*genericAssignments)[*formalArg];
        if (!Unify(&constraints, *arg)) {
            THROW_ERROR_EXCEPTION(
                "Wrong type for argument %v to function %Qv: expected %Qv, got %Qv",
                argIndex,
                functionName,
                constraints,
                *arg)
                << TErrorAttribute("expression", source);
        }
    }

    bool hasNoRepeatedArgument = !repeatedType.HasValue();

    if (formalArg != formalArguments.end() ||
        (arg != effectiveTypes.end() && hasNoRepeatedArgument))
    {
        THROW_ERROR_EXCEPTION(
            "Wrong number of arguments to function %Qv: expected %v, got %v",
            functionName,
            formalArguments.size(),
            effectiveTypes.size())
            << TErrorAttribute("expression", source);
    }

    for (; arg != effectiveTypes.end(); arg++) {
        size_t constraintIndex = repeatedType->first;
        if (repeatedType->second) {
            constraintIndex = genericAssignments->size();
            genericAssignments->push_back((*genericAssignments)[repeatedType->first]);
        }
        auto& constraints = (*genericAssignments)[constraintIndex];
        if (!Unify(&constraints, *arg)) {
            THROW_ERROR_EXCEPTION(
                "Wrong type for repeated argument to function %Qv: expected %Qv, got %Qv",
                functionName,
                constraints,
                *arg)
                << TErrorAttribute("expression", source);
        }
    }

    return (*genericAssignments)[formalResultType];
}

std::vector<EValueType> RefineFunctionTypes(
    const TFunctionTypeInferrer* inferrer,
    EValueType resultType,
    size_t argumentCount,
    std::vector<TTypeSet>* genericAssignments)
{
    std::vector<TTypeSet> typeConstraints;
    std::vector<size_t> formalArguments;
    TNullable<std::pair<size_t, bool>> repeatedType;
    size_t formalResultType = inferrer->GetNormalizedConstraints(
        &typeConstraints,
        &formalArguments,
        &repeatedType);

    (*genericAssignments)[formalResultType] = TTypeSet({resultType});

    std::vector<EValueType> genericAssignmentsMin;
    for (auto& constraint : *genericAssignments) {
        YCHECK(!constraint.IsEmpty());
        genericAssignmentsMin.push_back(constraint.GetFront());
    }

    std::vector<EValueType> effectiveTypes;
    auto argIndex = 0;
    auto formalArg = formalArguments.begin();
    for (;
        formalArg != formalArguments.end() && argIndex < argumentCount;
        ++formalArg, ++argIndex)
    {
        effectiveTypes.push_back(genericAssignmentsMin[*formalArg]);
    }

    for (; argIndex < argumentCount; ++argIndex) {
        size_t constraintIndex = repeatedType->first;
        if (repeatedType->second) {
            constraintIndex = genericAssignments->size() - (argumentCount - argIndex);
        }

        effectiveTypes.push_back(genericAssignmentsMin[constraintIndex]);
    }

    return effectiveTypes;
}

// 1. Init generic assignments with constraints
//    Intersect generic assignments with argument types and save them
//    Infer feasible result types
// 2. Apply result types and restrict generic assignments and argument types

struct TOperatorTyper
{
    TTypeSet Constraint;
    TNullable<EValueType> ResultType;
};

TEnumIndexedVector<TOperatorTyper, EBinaryOp> BuildBinaryOperatorTypers()
{
    TEnumIndexedVector<TOperatorTyper, EBinaryOp> result;

    for (auto op : {
        EBinaryOp::Plus,
        EBinaryOp::Minus,
        EBinaryOp::Multiply,
        EBinaryOp::Divide})
    {
        result[op] = {
            TTypeSet({EValueType::Null, EValueType::Int64, EValueType::Uint64, EValueType::Double}),
            Null
        };
    }

    for (auto op : {
        EBinaryOp::Modulo,
        EBinaryOp::LeftShift,
        EBinaryOp::RightShift,
        EBinaryOp::BitOr,
        EBinaryOp::BitAnd})
    {
        result[op] = {
            TTypeSet({EValueType::Null, EValueType::Int64, EValueType::Uint64}),
            Null
        };
    }

    for (auto op : {
        EBinaryOp::And,
        EBinaryOp::Or})
    {
        result[op] = {
            TTypeSet({EValueType::Null, EValueType::Boolean}),
            EValueType::Boolean
        };
    }

    for (auto op : {
        EBinaryOp::Equal,
        EBinaryOp::NotEqual,
        EBinaryOp::Less,
        EBinaryOp::Greater,
        EBinaryOp::LessOrEqual,
        EBinaryOp::GreaterOrEqual})
    {
        result[op] = {
            TTypeSet({
                EValueType::Null,
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::String}),
            EValueType::Boolean
        };
    }

    return result;
}

const TEnumIndexedVector<TOperatorTyper, EBinaryOp>& GetBinaryOperatorTypers()
{
    static auto result = BuildBinaryOperatorTypers();
    return result;
}

TEnumIndexedVector<TOperatorTyper, EUnaryOp> BuildUnaryOperatorTypers()
{
    TEnumIndexedVector<TOperatorTyper, EUnaryOp> result;

    for (auto op : {
        EUnaryOp::Plus,
        EUnaryOp::Minus})
    {
        result[op] = {
            TTypeSet({EValueType::Null, EValueType::Int64, EValueType::Uint64, EValueType::Double}),
            Null
        };
    }

    result[EUnaryOp::BitNot] = {
        TTypeSet({EValueType::Null, EValueType::Int64, EValueType::Uint64}),
        Null
    };

    result[EUnaryOp::Not] = {
        TTypeSet({EValueType::Null, EValueType::Boolean}),
        Null
    };

    return result;
}

const TEnumIndexedVector<TOperatorTyper, EUnaryOp>& GetUnaryOperatorTypers()
{
    static auto result = BuildUnaryOperatorTypers();
    return result;
}

TTypeSet InferBinaryExprTypes(
    EBinaryOp opCode,
    const TTypeSet& lhsTypes,
    const TTypeSet& rhsTypes,
    TTypeSet* genericAssignments,
    const TStringBuf& lhsSource,
    const TStringBuf& rhsSource)
{
    const auto& binaryOperators = GetBinaryOperatorTypers();

    *genericAssignments = binaryOperators[opCode].Constraint;

    if (!Unify(genericAssignments, lhsTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv: expected %Qv, got %Qv",
            opCode,
            *genericAssignments,
            lhsTypes)
            << TErrorAttribute("lhs_source", lhsSource)
            << TErrorAttribute("rhs_source", rhsSource);
    }

    if (!Unify(genericAssignments, rhsTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv: expected %Qv, got %Qv",
            opCode,
            *genericAssignments,
            rhsTypes)
            << TErrorAttribute("lhs_source", lhsSource)
            << TErrorAttribute("rhs_source", rhsSource);
    }

    TTypeSet resultTypes;
    if (binaryOperators[opCode].ResultType) {
        resultTypes = TTypeSet({*binaryOperators[opCode].ResultType});
    } else {
        resultTypes = *genericAssignments;
    }

    return resultTypes;
}

std::pair<EValueType, EValueType> RefineBinaryExprTypes(
    EBinaryOp opCode,
    EValueType resultType,
    TTypeSet* genericAssignments)
{
    const auto& binaryOperators = GetBinaryOperatorTypers();

    EValueType argType;
    if (binaryOperators[opCode].ResultType) {
        YCHECK(!genericAssignments->IsEmpty());
        argType = genericAssignments->GetFront();
    } else {
        YCHECK(genericAssignments->Get(resultType));
        argType = resultType;
    }

    return std::make_pair(argType, argType);
}

TTypeSet InferUnaryExprTypes(
    EUnaryOp opCode,
    const TTypeSet& argTypes,
    TTypeSet* genericAssignments,
    const TStringBuf& opSource)
{
    const auto& unaryOperators = GetUnaryOperatorTypers();

    *genericAssignments = unaryOperators[opCode].Constraint;

    if (!Unify(genericAssignments, argTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv: expected %Qv, got %Qv",
            opCode,
            *genericAssignments,
            argTypes)
            << TErrorAttribute("op_source", opSource);
    }

    TTypeSet resultTypes;
    if (unaryOperators[opCode].ResultType) {
        resultTypes = TTypeSet({*unaryOperators[opCode].ResultType});
    } else {
        resultTypes = *genericAssignments;
    }

    return resultTypes;
}

EValueType RefineUnaryExprTypes(
    EUnaryOp opCode,
    EValueType resultType,
    TTypeSet* genericAssignments)
{
    const auto& unaryOperators = GetUnaryOperatorTypers();

    EValueType argType;
    if (unaryOperators[opCode].ResultType) {
        YCHECK(!genericAssignments->IsEmpty());
        argType = genericAssignments->GetFront();
    } else {
        YCHECK(genericAssignments->Get(resultType));
        argType = resultType;
    }

    return argType;
}

struct TTypedExpressionBuilder
{
    const TString& Source;
    const TConstTypeInferrerMapPtr& Functions;
    const NAst::TAliasMap& AliasMap;

    TUntypedExpression DoBuildUntypedExpression(
        const NAst::TExpression* expr,
        ISchemaProxyPtr schema,
        std::set<TString>& usedAliases) const
    {
        if (auto literalExpr = expr->As<NAst::TLiteralExpression>()) {
            const auto& literalValue = literalExpr->Value;

            auto resultTypes = GetTypes(literalValue);
            TExpressionGenerator generator = [literalValue] (EValueType type) {
                return New<TLiteralExpression>(
                    type,
                    CastValueWithCheck(GetValue(literalValue), type));
            };
            return TUntypedExpression{resultTypes, std::move(generator), true};
        } else if (auto referenceExpr = expr->As<NAst::TReferenceExpression>()) {
            auto column = schema->GetColumnPtr(referenceExpr->ColumnName, referenceExpr->TableName);
            if (!column) {
                if (referenceExpr->TableName.empty()) {
                    auto columnName = referenceExpr->ColumnName;
                    auto found = AliasMap.find(columnName);

                    if (found != AliasMap.end()) {
                        if (usedAliases.count(columnName)) {
                            THROW_ERROR_EXCEPTION("Recursive usage of alias %Qv", columnName);
                        }

                        usedAliases.insert(columnName);
                        auto aliasExpr = DoBuildUntypedExpression(
                            found->second.Get(),
                            schema,
                            usedAliases);

                        usedAliases.erase(columnName);
                        return aliasExpr;
                    }
                }

                THROW_ERROR_EXCEPTION("Undefined reference %Qv",
                    NAst::FormatColumn(referenceExpr->ColumnName, referenceExpr->TableName));
            }

            TTypeSet resultTypes({column->Type});
            TExpressionGenerator generator = [name = column->Name] (EValueType type) {
                return New<TReferenceExpression>(type, name);
            };
            return TUntypedExpression{resultTypes, std::move(generator), false};
        } else if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
            auto functionName = functionExpr->FunctionName;
            functionName.to_lower();

            const auto& descriptor = Functions->GetFunction(functionName);

            if (const auto* aggregateFunction = descriptor->As<TAggregateTypeInferrer>()) {
                auto subexprName = InferName(functionExpr);

                try {
                    if (functionExpr->Arguments.size() != 1) {
                        THROW_ERROR_EXCEPTION(
                            "Aggregate function %Qv must have exactly one argument",
                            functionName);
                    }

                    auto aggregateColumn = schema->GetAggregateColumnPtr(
                        functionName,
                        aggregateFunction,
                        functionExpr->Arguments.front().Get(),
                        subexprName,
                        *this);

                    return aggregateColumn;
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error creating aggregate")
                        << TErrorAttribute("source", functionExpr->GetSource(Source))
                        << ex;
                }
            } else if (const auto* regularFunction = descriptor->As<TFunctionTypeInferrer>()) {
                std::vector<TTypeSet> argTypes;
                std::vector<TExpressionGenerator> operandTypers;
                for (const auto& argument : functionExpr->Arguments) {
                    auto untypedArgument = DoBuildUntypedExpression(
                        argument.Get(),
                        schema,
                        usedAliases);
                    argTypes.push_back(untypedArgument.FeasibleTypes);
                    operandTypers.push_back(untypedArgument.Generator);
                }

                std::vector<TTypeSet> genericAssignments;
                auto resultTypes = InferFunctionTypes(
                    regularFunction,
                    argTypes,
                    &genericAssignments,
                    functionName,
                    functionExpr->GetSource(Source));

                TExpressionGenerator generator = [
                    functionName,
                    regularFunction,
                    operandTypers,
                    genericAssignments] (EValueType type) mutable
                {
                    auto effectiveTypes = RefineFunctionTypes(
                        regularFunction,
                        type,
                        operandTypers.size(),
                        &genericAssignments);

                    std::vector<TConstExpressionPtr> typedOperands;
                    for (size_t index = 0; index < effectiveTypes.size(); ++index) {
                        typedOperands.push_back(operandTypers[index](effectiveTypes[index]));
                    }

                    return New<TFunctionExpression>(type, functionName, typedOperands);
                };

                return TUntypedExpression{resultTypes, std::move(generator), false};
            }
        } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
            if (unaryExpr->Operand.size() != 1) {
                THROW_ERROR_EXCEPTION(
                    "Unary operator %Qv must have exactly one argument",
                    unaryExpr->Opcode);
            }

            auto untypedOperand = DoBuildUntypedExpression(
                unaryExpr->Operand.front().Get(),
                schema,
                usedAliases);

            TTypeSet genericAssignments;
            auto resultTypes = InferUnaryExprTypes(
                unaryExpr->Opcode,
                untypedOperand.FeasibleTypes,
                &genericAssignments,
                unaryExpr->Operand.front()->GetSource(Source));

            if (untypedOperand.IsConstant) {
                auto value = untypedOperand.Generator(untypedOperand.FeasibleTypes.GetFront());
                if (auto foldedExpr = FoldConstants(unaryExpr->Opcode, value)) {
                    TExpressionGenerator generator = [foldedExpr] (EValueType type) {
                        return New<TLiteralExpression>(
                            type,
                            CastValueWithCheck(*foldedExpr, type));
                    };
                    return TUntypedExpression{resultTypes, std::move(generator), true};
                }
            }

            TExpressionGenerator generator = [
                op = unaryExpr->Opcode,
                untypedOperand,
                genericAssignments
            ] (EValueType type) mutable {
                auto argType = RefineUnaryExprTypes(op, type, &genericAssignments);
                return New<TUnaryOpExpression>(type, op, untypedOperand.Generator(argType));
            };
            return TUntypedExpression{resultTypes, std::move(generator), false};

        } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
            auto makeBinaryExpr = [&] (
                EBinaryOp op,
                TUntypedExpression lhs,
                TUntypedExpression rhs,
                TNullable<size_t> offset) -> TUntypedExpression
            {
                TTypeSet genericAssignments;
                auto resultTypes = InferBinaryExprTypes(
                    op,
                    lhs.FeasibleTypes,
                    rhs.FeasibleTypes,
                    &genericAssignments,
                    offset ? binaryExpr->Lhs[*offset]->GetSource(Source) : "",
                    offset ? binaryExpr->Rhs[*offset]->GetSource(Source) : "");

                if (lhs.IsConstant && rhs.IsConstant) {
                    auto lhsValue = lhs.Generator(lhs.FeasibleTypes.GetFront());
                    auto rhsValue = rhs.Generator(rhs.FeasibleTypes.GetFront());
                    if (auto foldedExpr = FoldConstants(op, lhsValue, rhsValue)) {
                        TExpressionGenerator generator = [foldedExpr] (EValueType type) {
                            return New<TLiteralExpression>(
                                type,
                                CastValueWithCheck(*foldedExpr, type));
                        };
                        return TUntypedExpression{resultTypes, std::move(generator), true};
                    }
                }

                TExpressionGenerator generator = [op, lhs, rhs, genericAssignments] (EValueType type) mutable {
                    auto argTypes = RefineBinaryExprTypes(
                        op,
                        type,
                        &genericAssignments);

                    return New<TBinaryOpExpression>(
                        type,
                        op,
                        lhs.Generator(argTypes.first),
                        rhs.Generator(argTypes.second));
                };
                return TUntypedExpression{resultTypes, std::move(generator), false};

            };

            std::function<TUntypedExpression(int, int, EBinaryOp)> gen = [&] (int offset, int keySize, EBinaryOp op)
             -> TUntypedExpression
             {
                auto untypedLhs = DoBuildUntypedExpression(
                    binaryExpr->Lhs[offset].Get(),
                    schema,
                    usedAliases);
                auto untypedRhs = DoBuildUntypedExpression(
                    binaryExpr->Rhs[offset].Get(),
                    schema,
                    usedAliases);

                if (offset + 1 < keySize) {
                    auto next = gen(offset + 1, keySize, op);
                    auto eq = makeBinaryExpr(
                        EBinaryOp::And,
                        makeBinaryExpr(
                            EBinaryOp::Equal,
                            untypedLhs,
                            untypedRhs,
                            offset),
                        std::move(next),
                        Null);
                    if (op == EBinaryOp::Less || op == EBinaryOp::LessOrEqual) {
                        return makeBinaryExpr(
                                EBinaryOp::Or,
                                makeBinaryExpr(EBinaryOp::Less, std::move(untypedLhs), std::move(untypedRhs), offset),
                                std::move(eq),
                                Null);
                    } else if (op == EBinaryOp::Greater || op == EBinaryOp::GreaterOrEqual)  {
                        return makeBinaryExpr(
                                EBinaryOp::Or,
                                makeBinaryExpr(EBinaryOp::Greater, std::move(untypedLhs), std::move(untypedRhs), offset),
                                std::move(eq),
                                Null);
                    } else {
                        return eq;
                    }
                } else {
                    return makeBinaryExpr(op, std::move(untypedLhs), std::move(untypedRhs), offset);
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
                        << TErrorAttribute("source", binaryExpr->GetSource(Source));
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

                auto untypedLhs = DoBuildUntypedExpression(
                    binaryExpr->Lhs.front().Get(),
                    schema,
                    usedAliases);
                auto untypedRhs = DoBuildUntypedExpression(
                    binaryExpr->Rhs.front().Get(),
                    schema,
                    usedAliases);

                return makeBinaryExpr(binaryExpr->Opcode, std::move(untypedLhs), std::move(untypedRhs), 0);
            }
        } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
            std::vector<TConstExpressionPtr> typedArguments;
            std::unordered_set<TString> columnNames;
            std::vector<EValueType> argTypes;

            for (const auto& argument : inExpr->Expr) {
                auto untypedArgument = DoBuildUntypedExpression(argument.Get(), schema, usedAliases);

                if (untypedArgument.FeasibleTypes.GetSize() != 1) {
                    THROW_ERROR_EXCEPTION("Cannot infer argument type for IN operator")
                        << TErrorAttribute("source", argument->GetSource(Source));
                }
                EValueType argType = untypedArgument.FeasibleTypes.GetFront();
                auto typedArgument = untypedArgument.Generator(argType);

                typedArguments.push_back(typedArgument);
                argTypes.push_back(argType);
                if (auto reference = typedArgument->As<TReferenceExpression>()) {
                    if (!columnNames.insert(reference->ColumnName).second) {
                        THROW_ERROR_EXCEPTION("IN operator has multiple references to column %Qv", reference->ColumnName)
                            << TErrorAttribute("source", inExpr->GetSource(Source));
                    }
                }
            }

            auto capturedRows = LiteralTupleListToRows(inExpr->Values, argTypes, inExpr->GetSource(Source));
            auto result = New<TInOpExpression>(std::move(typedArguments), std::move(capturedRows));

            TTypeSet resultTypes({EValueType::Boolean});
            TExpressionGenerator generator = [result] (EValueType type) mutable {
                return result;
            };
            return TUntypedExpression{resultTypes, std::move(generator), false};
        }

        Y_UNREACHABLE();
    }

    TUntypedExpression BuildUntypedExpression(
        const NAst::TExpression* expr,
        ISchemaProxyPtr schema) const
    {
        std::set<TString> usedAliases;
        return DoBuildUntypedExpression(expr, schema, usedAliases);
    }

    TConstExpressionPtr BuildTypedExpression(
        const NAst::TExpression* expr,
        ISchemaProxyPtr schema) const
    {
        auto expressionTyper = BuildUntypedExpression(expr, schema);
        YCHECK(!expressionTyper.FeasibleTypes.IsEmpty());
        return TNotExpressionPropagator().Visit(expressionTyper.Generator(expressionTyper.FeasibleTypes.GetFront()));
    }

};

DECLARE_REFCOUNTED_CLASS(TSchemaProxy)

class TSchemaProxy
    : public ISchemaProxy
{
public:
    TSchemaProxy()
    { }

    TSchemaProxy(const yhash<std::pair<TString, TString>, TBaseColumn>& lookup)
        : Lookup_(lookup)
    { }

    virtual TNullable<TBaseColumn> GetColumnPtr(
        const TString& name,
        const TString& tableName) override
    {
        auto key = std::make_pair(name, tableName);
        auto found = Lookup_.find(key);
        if (found != Lookup_.end()) {
            return found->second;
        } else if (auto column = ProvideColumn(name, tableName)) {
            YCHECK(Lookup_.emplace(key, *column).second);
            return column;
        } else {
            return Null;
        }
    }

    virtual TUntypedExpression GetAggregateColumnPtr(
        const TString& name,
        const TAggregateTypeInferrer* aggregateFunction,
        const NAst::TExpression* arguments,
        TString subexprName,
        const TTypedExpressionBuilder& builder) override
    {
        auto typer = ProvideAggregateColumn(
            name,
            aggregateFunction,
            arguments,
            subexprName,
            builder);

        TExpressionGenerator generator = [=] (EValueType type) {
            auto found = AggregateLookup_.find(std::make_pair(subexprName, type));
            if (found != AggregateLookup_.end()) {
                TBaseColumn columnInfo = found->second;
                return New<TReferenceExpression>(columnInfo.Type, columnInfo.Name);
            } else {
                TBaseColumn columnInfo = typer.second(type);
                YCHECK(AggregateLookup_.emplace(std::make_pair(subexprName, type), columnInfo).second);
                return New<TReferenceExpression>(columnInfo.Type, columnInfo.Name);
            }
        };

        return TUntypedExpression{typer.first, std::move(generator), false};
    }

    virtual void Finish()
    { }

    const yhash<std::pair<TString, TString>, TBaseColumn>& GetLookup() const
    {
        return Lookup_;
    }

private:
    yhash<std::pair<TString, TString>, TBaseColumn> Lookup_;
    yhash<std::pair<TString, EValueType>, TBaseColumn> AggregateLookup_;

protected:
    virtual TNullable<TBaseColumn> ProvideColumn(const TString& name, const TString& tableName)
    {
        return Null;
    }

    virtual std::pair<TTypeSet, std::function<TBaseColumn(EValueType)>> ProvideAggregateColumn(
        const TString& name,
        const TAggregateTypeInferrer* aggregateFunction,
        const NAst::TExpression* arguments,
        TString subexprName,
        const TTypedExpressionBuilder& builder)
    {
        THROW_ERROR_EXCEPTION(
            "Misuse of aggregate function %v",
            name);
    }

};

DEFINE_REFCOUNTED_TYPE(TSchemaProxy)

class TScanSchemaProxy
    : public TSchemaProxy
{
public:
    TScanSchemaProxy(
        const TTableSchema& sourceTableSchema,
        const TString& tableName,
        std::vector<TColumnDescriptor>* mapping = nullptr)
        : Mapping_(mapping)
        , SourceTableSchema_(sourceTableSchema)
        , TableName_(tableName)
    { }

    virtual TNullable<TBaseColumn> ProvideColumn(const TString& name, const TString& tableName) override
    {
        if (tableName != TableName_) {
            return Null;
        }

        auto column = SourceTableSchema_.FindColumn(name);

        if (column) {
            auto columnName = NAst::FormatColumn(name, tableName);
            if (size_t collisionIndex = ColumnsCollisions_.emplace(columnName, 0).first->second++) {
                columnName = Format("%v#%v", columnName, collisionIndex);
            }

            if (Mapping_) {
                Mapping_->push_back(TColumnDescriptor{
                    columnName,
                    size_t(SourceTableSchema_.GetColumnIndex(*column))});
            }

            return TBaseColumn(columnName, column->Type);
        } else {
            return Null;
        }
    }

    virtual void Finish() override
    {
        for (const auto& column : SourceTableSchema_.Columns()) {
            GetColumnPtr(column.Name, TableName_);
        }
    }

private:
    std::vector<TColumnDescriptor>* Mapping_;
    yhash<TString, size_t> ColumnsCollisions_;
    const TTableSchema SourceTableSchema_;
    TString TableName_;

    DECLARE_NEW_FRIEND();
};

class TJoinSchemaProxy
    : public TSchemaProxy
{
public:
    TJoinSchemaProxy(
        std::vector<TString>* selfJoinedColumns,
        std::vector<TString>* foreignJoinedColumns,
        const std::set<std::pair<TString, TString>>& sharedColumns,
        TSchemaProxyPtr self,
        TSchemaProxyPtr foreign)
        : SelfJoinedColumns_(selfJoinedColumns)
        , ForeignJoinedColumns_(foreignJoinedColumns)
        , SharedColumns_(sharedColumns)
        , Self_(self)
        , Foreign_(foreign)
    { }

    virtual TNullable<TBaseColumn> ProvideColumn(const TString& name, const TString& tableName) override
    {
        if (auto column = Self_->GetColumnPtr(name, tableName)) {
            if (!SharedColumns_.count(std::make_pair(name, tableName)) &&
                Foreign_->GetColumnPtr(name, tableName))
            {
                THROW_ERROR_EXCEPTION("Column %Qv occurs both in main and joined tables",
                    NAst::FormatColumn(name, tableName));
            }
            SelfJoinedColumns_->push_back(column->Name);
            return column;
        } else if (auto column = Foreign_->GetColumnPtr(name, tableName)) {
            ForeignJoinedColumns_->push_back(column->Name);
            return column;
        } else {
            return Null;
        }
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
    std::vector<TString>* SelfJoinedColumns_;
    std::vector<TString>* ForeignJoinedColumns_;

    std::set<std::pair<TString, TString>> SharedColumns_;
    TSchemaProxyPtr Self_;
    TSchemaProxyPtr Foreign_;

};

const TNullable<TBaseColumn> FindColumn(const TNamedItemList& schema, const TString& name)
{
    for (size_t index = 0; index < schema.size(); ++index) {
        if (schema[index].Name == name) {
            return TBaseColumn(name, schema[index].Expression->Type);
        }
    }
    return Null;
}

class TGroupSchemaProxy
    : public TSchemaProxy
{
public:
    TGroupSchemaProxy(
        const TNamedItemList* groupItems,
        TSchemaProxyPtr base,
        TAggregateItemList* aggregateItems)
        : GroupItems_(groupItems)
        , Base_(base)
        , AggregateItems_(aggregateItems)
    { }

    virtual TNullable<TBaseColumn> ProvideColumn(const TString& name, const TString& tableName) override
    {
        if (!tableName.empty()) {
            return Null;
        }

        return FindColumn(*GroupItems_, name);
    }

    virtual std::pair<TTypeSet, std::function<TBaseColumn(EValueType)>> ProvideAggregateColumn(
        const TString& name,
        const TAggregateTypeInferrer* aggregateFunction,
        const NAst::TExpression* argument,
        TString subexprName,
        const TTypedExpressionBuilder& builder) override
    {
        auto untypedOperand = builder.BuildUntypedExpression(
            argument,
            Base_);

        TTypeSet constraint;
        TNullable<EValueType> stateType;
        TNullable<EValueType> resultType;

        aggregateFunction->GetNormalizedConstraints(&constraint, &stateType, &resultType, name);

        TTypeSet resultTypes;
        TTypeSet genericAssignments = constraint;

        if (!Unify(&genericAssignments, untypedOperand.FeasibleTypes)) {
            THROW_ERROR_EXCEPTION("Type mismatch in function %Qv: expected %Qv, got %Qv",
                name,
                genericAssignments,
                untypedOperand.FeasibleTypes)
                << TErrorAttribute("source", subexprName);
        }

        if (resultType) {
            resultTypes = TTypeSet({*resultType});
        } else {
            resultTypes = genericAssignments;
        }

        return std::make_pair(resultTypes, [=] (EValueType type) {
            EValueType argType;
            if (resultType) {
                YCHECK(!genericAssignments.IsEmpty());
                argType = genericAssignments.GetFront();
            } else {
                argType = type;
            }

            EValueType effectiveStateType;
            if (stateType) {
                effectiveStateType = *stateType;
            } else {
                effectiveStateType = argType;
            }

            auto typedOperand = untypedOperand.Generator(argType);
            CheckExpressionDepth(typedOperand);

            AggregateItems_->emplace_back(
                typedOperand,
                name,
                subexprName,
                effectiveStateType,
                type);

            return TBaseColumn(subexprName, type);
        });
    }

private:
    const TNamedItemList* GroupItems_;
    TSchemaProxyPtr Base_;
    TAggregateItemList* AggregateItems_;

};

TConstExpressionPtr BuildPredicate(
    const NAst::TExpressionList& expressionAst,
    const TSchemaProxyPtr& schemaProxy,
    const TTypedExpressionBuilder& builder,
    const TStringBuf& name)
{
    if (expressionAst.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            << TErrorAttribute("source", InferName(expressionAst));
    }

    auto typedPredicate = builder.BuildTypedExpression(expressionAst.front().Get(), schemaProxy);

    CheckExpressionDepth(typedPredicate);

    auto actualType = typedPredicate->Type;
    EValueType expectedType(EValueType::Boolean);
    if (actualType != expectedType) {
        THROW_ERROR_EXCEPTION("%v is not a boolean expression")
            << TErrorAttribute("source", InferName(expressionAst))
            << TErrorAttribute("actual_type", actualType)
            << TErrorAttribute("expected_type", expectedType);
    }

    return typedPredicate;
}

TConstGroupClausePtr BuildGroupClause(
    const NAst::TExpressionList& expressionsAst,
    ETotalsMode totalsMode,
    TSchemaProxyPtr& schemaProxy,
    const TTypedExpressionBuilder& builder)
{
    auto groupClause = New<TGroupClause>();
    groupClause->TotalsMode = totalsMode;

    for (const auto& expressionAst : expressionsAst) {
        auto typedExpr = builder.BuildTypedExpression(expressionAst.Get(), schemaProxy);

        CheckExpressionDepth(typedExpr);
        groupClause->AddGroupItem(typedExpr, InferName(expressionAst.Get()));
    }

    schemaProxy = New<TGroupSchemaProxy>(&groupClause->GroupItems, std::move(schemaProxy), &groupClause->AggregateItems);

    return groupClause;
}

TConstExpressionPtr BuildHavingClause(
    const NAst::TExpressionList& expressionsAst,
    const TSchemaProxyPtr& schemaProxy,
    const TTypedExpressionBuilder& builder)
{
    if (expressionsAst.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            << TErrorAttribute("source", InferName(expressionsAst));
    }

    auto typedPredicate = builder.BuildTypedExpression(expressionsAst.front().Get(), schemaProxy);

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
    TSchemaProxyPtr& schemaProxy,
    const TTypedExpressionBuilder& builder)
{
    auto projectClause = New<TProjectClause>();
    for (const auto& expressionAst : expressionsAst) {
        auto typedExpr = builder.BuildTypedExpression(expressionAst.Get(), schemaProxy);

        CheckExpressionDepth(typedExpr);
        projectClause->AddProjection(typedExpr, InferName(expressionAst.Get()));
    }

    schemaProxy = New<TScanSchemaProxy>(projectClause->GetTableSchema(), "");

    return projectClause;
}

void PrepareQuery(
    const TQueryPtr& query,
    const NAst::TQuery& ast,
    TSchemaProxyPtr& schemaProxy,
    const TTypedExpressionBuilder& builder)
{
    if (const auto* wherePredicate = ast.WherePredicate.GetPtr()) {
        query->WhereClause = BuildPredicate(
            *wherePredicate,
            schemaProxy,
            builder,
            "WHERE-clause");
    }

    if (const auto* groupExprs = ast.GroupExprs.GetPtr()) {
        query->GroupClause = BuildGroupClause(
            groupExprs->first,
            groupExprs->second,
            schemaProxy,
            builder);
    }

    if (ast.HavingPredicate) {
        if (!query->GroupClause) {
            THROW_ERROR_EXCEPTION("Expected GROUP BY before HAVING");
        }
        query->HavingClause = BuildHavingClause(
            ast.HavingPredicate.Get(),
            schemaProxy,
            builder);
    }

    if (!ast.OrderExpressions.empty()) {
        auto orderClause = New<TOrderClause>();

        for (const auto& orderExpr : ast.OrderExpressions) {
            for (const auto& expressionAst : orderExpr.first) {
                auto typedExpr = builder.BuildTypedExpression(expressionAst.Get(), schemaProxy);

                orderClause->OrderItems.emplace_back(typedExpr, orderExpr.second);
            }
        }

        query->OrderClause = std::move(orderClause);
    }

    if (ast.SelectExprs) {
        query->ProjectClause = BuildProjectClause(
            ast.SelectExprs.Get(),
            schemaProxy,
            builder);
    }

    schemaProxy->Finish();
}

void ParseQueryString(
    NAst::TAstHead* astHead,
    const TString& source,
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

} // namespace

////////////////////////////////////////////////////////////////////////////////

void DefaultFetchFunctions(const std::vector<TString>& names, const TTypeInferrerMapPtr& typeInferrers)
{
    MergeFrom(typeInferrers.Get(), *BuiltinTypeInferrersMap);
}

////////////////////////////////////////////////////////////////////////////////

// For testing
void ParseJobQuery(const TString& source)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>(), NAst::TAliasMap()};
    ParseQueryString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseJobQuery);
}

std::pair<TQueryPtr, TDataRanges> PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const TString& source,
    const TFetchFunctions& fetchFunctions,
    i64 inputRowLimit,
    i64 outputRowLimit,
    TTimestamp timestamp)
{
    auto query = New<TQuery>(inputRowLimit, outputRowLimit, TGuid::Create());

    auto Logger = MakeQueryLogger(query);

    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>(), NAst::TAliasMap()};
    ParseQueryString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseQuery);

    const auto& ast = astHead.first.As<NAst::TQuery>();

    auto functionNames = ExtractFunctionNames(std::make_pair(ast, astHead.second));

    auto functions = New<TTypeInferrerMap>();
    fetchFunctions(functionNames, functions);

    const auto& table = ast.Table;

    // The first one is the primary table; the others are foreign.
    std::vector<NYPath::TRichYPath> tablePaths;
    tablePaths.push_back(table.Path);
    for (const auto& join : ast.Joins) {
        tablePaths.push_back(join.Table.Path);
    }

    LOG_DEBUG("Getting initial data splits (Paths: %v)", tablePaths);

    auto dataSplits = WaitFor(callbacks->GetInitialSplits(tablePaths, timestamp))
        .ValueOrThrow();

    LOG_DEBUG("Initial data splits received");

    const auto& selfDataSplit = dataSplits[0];

    auto tableSchema = GetTableSchemaFromDataSplit(selfDataSplit);
    query->OriginalSchema = tableSchema;

    TSchemaProxyPtr schemaProxy = New<TScanSchemaProxy>(
        tableSchema,
        table.Alias,
        &query->SchemaMapping);

    TTypedExpressionBuilder builder{
        source,
        functions,
        astHead.second};

    size_t commonKeyPrefix = std::numeric_limits<size_t>::max();

    for (size_t index = 0; index < ast.Joins.size(); ++index) {
        const auto& join = ast.Joins[index];
        const auto& foreignDataSplit = dataSplits[index + 1];

        auto foreignTableSchema = GetTableSchemaFromDataSplit(foreignDataSplit);
        auto foreignKeyColumnsCount = foreignTableSchema.GetKeyColumns().size();

        auto joinClause = New<TJoinClause>();
        joinClause->OriginalSchema = foreignTableSchema;
        joinClause->ForeignDataId = GetObjectIdFromDataSplit(foreignDataSplit);
        joinClause->IsLeft = join.IsLeft;

        auto foreignSourceProxy = New<TScanSchemaProxy>(
            foreignTableSchema,
            join.Table.Alias,
            &joinClause->SchemaMapping);

        std::vector<std::pair<TConstExpressionPtr, bool>> selfEquations;
        std::vector<TConstExpressionPtr> foreignEquations;
        std::set<std::pair<TString, TString>> sharedColumns;
        // Merge columns.
        for (const auto& reference : join.Fields) {
            auto selfColumn = schemaProxy->GetColumnPtr(reference->ColumnName, reference->TableName);
            auto foreignColumn = foreignSourceProxy->GetColumnPtr(reference->ColumnName, reference->TableName);

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

            selfEquations.emplace_back(New<TReferenceExpression>(selfColumn->Type, selfColumn->Name), false);
            foreignEquations.push_back(New<TReferenceExpression>(foreignColumn->Type, foreignColumn->Name));

            // Add to mapping
            sharedColumns.emplace(reference->ColumnName, reference->TableName);
        }

        for (const auto& argument : join.Left) {
            selfEquations.emplace_back(builder.BuildTypedExpression(argument.Get(), schemaProxy), false);
        }

        for (const auto& argument : join.Right) {
            foreignEquations.push_back(builder.BuildTypedExpression(argument.Get(), foreignSourceProxy));
        }

        if (selfEquations.size() != foreignEquations.size()) {
            THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
                selfEquations.size(),
                foreignEquations.size())
                << TErrorAttribute("lhs_source", InferName(join.Left))
                << TErrorAttribute("rhs_source", InferName(join.Right));
        }

        for (size_t index = 0; index < selfEquations.size(); ++index) {
            if (selfEquations[index].first->Type != foreignEquations[index]->Type) {
                THROW_ERROR_EXCEPTION("Types mismatch in join equation \"%v = %v\"",
                    InferName(selfEquations[index].first),
                    InferName(foreignEquations[index]))
                    << TErrorAttribute("self_type", selfEquations[index].first->Type)
                    << TErrorAttribute("foreign_type", foreignEquations[index]->Type);
            }
        }

        // If can use ranges, rearrange equations according to key columns and enrich with evaluated columns

        std::vector<std::pair<TConstExpressionPtr, bool>> keySelfEquations(foreignKeyColumnsCount);
        std::vector<TConstExpressionPtr> keyForeignEquations(foreignKeyColumnsCount);

        bool canUseSourceRanges = true;
        for (size_t equationIndex = 0; equationIndex < foreignEquations.size(); ++equationIndex) {
            const auto& expr = foreignEquations[equationIndex];

            if (const auto* refExpr = expr->As<TReferenceExpression>()) {
                auto index = ColumnNameToKeyPartIndex(joinClause->GetKeyColumns(), refExpr->ColumnName);

                if (index >= 0) {
                    keySelfEquations[index] = selfEquations[equationIndex];
                    keyForeignEquations[index] = foreignEquations[equationIndex];
                    continue;
                }
            }
            canUseSourceRanges = false;
            break;
        }

        size_t keyPrefix = 0;
        for (; keyPrefix < foreignKeyColumnsCount; ++keyPrefix) {
            if (keyForeignEquations[keyPrefix]) {
                YCHECK(keySelfEquations[keyPrefix].first);

                if (const auto* refExpr = keySelfEquations[keyPrefix].first->As<TReferenceExpression>()) {
                    if (ColumnNameToKeyPartIndex(query->GetKeyColumns(), refExpr->ColumnName) != keyPrefix) {
                        commonKeyPrefix = std::min(commonKeyPrefix, keyPrefix);
                    }
                } else {
                    commonKeyPrefix = std::min(commonKeyPrefix, keyPrefix);
                }

                continue;
            }

            const auto& foreignColumnExpression = foreignTableSchema.Columns()[keyPrefix].Expression;

            if (!foreignColumnExpression) {
                break;
            }

            yhash_set<TString> references;
            auto evaluatedColumnExpression = PrepareExpression(
                foreignColumnExpression.Get(),
                foreignTableSchema,
                functions,
                &references);

            auto canEvaluate = true;
            for (const auto& reference : references) {
                int referenceIndex = foreignTableSchema.GetColumnIndexOrThrow(reference);
                if (!keySelfEquations[referenceIndex].first) {
                    YCHECK(!keyForeignEquations[referenceIndex]);
                    canEvaluate = false;
                }
            }

            if (!canEvaluate) {
                break;
            }

            keySelfEquations[keyPrefix] = std::make_pair(evaluatedColumnExpression, true);

            auto foreignColumn = foreignSourceProxy->GetColumnPtr(
                foreignTableSchema.Columns()[keyPrefix].Name,
                join.Table.Alias);

            keyForeignEquations[keyPrefix] = New<TReferenceExpression>(
                foreignColumn->Type,
                foreignColumn->Name);
        }

        commonKeyPrefix = std::min(commonKeyPrefix, keyPrefix);

        for (size_t index = 0; index < keyPrefix; ++index) {
            if (keySelfEquations[index].second) {
                const auto& evaluatedColumnExpression = keySelfEquations[index].first;

                if (const auto& selfColumnExpression = tableSchema.Columns()[index].Expression) {
                    auto evaluatedSelfColumnExpression = PrepareExpression(
                        selfColumnExpression.Get(),
                        tableSchema,
                        functions);

                    if (!Compare(
                        evaluatedColumnExpression,
                        foreignTableSchema,
                        evaluatedSelfColumnExpression,
                        tableSchema,
                        commonKeyPrefix))
                    {
                        commonKeyPrefix = std::min(commonKeyPrefix, index);
                    }
                } else {
                    commonKeyPrefix = std::min(commonKeyPrefix, index);
                }
            }
        }

        // Check that there are no join equations from keyPrefix to foreignKeyColumnsCount
        for (size_t index = keyPrefix; index < keyForeignEquations.size() && canUseSourceRanges; ++index) {
            if (keyForeignEquations[index]) {
                YCHECK(keySelfEquations[index].first);
                canUseSourceRanges = false;
            }
        }

        joinClause->CanUseSourceRanges = canUseSourceRanges;
        if (canUseSourceRanges) {
            keyForeignEquations.resize(keyPrefix);
            keySelfEquations.resize(keyPrefix);
            joinClause->SelfEquations = std::move(keySelfEquations);
            joinClause->ForeignEquations = std::move(keyForeignEquations);
            joinClause->CommonKeyPrefix = commonKeyPrefix;
            LOG_DEBUG("Creating join via source ranges (CommonKeyPrefix: %v)", commonKeyPrefix);
        } else {
            joinClause->SelfEquations = std::move(selfEquations);
            joinClause->ForeignEquations = std::move(foreignEquations);
            LOG_DEBUG("Creating join via IN clause");
            commonKeyPrefix = 0;
        }

        if (join.Predicate) {
            joinClause->Predicate = BuildPredicate(
                *join.Predicate,
                foreignSourceProxy,
                builder,
                "JOIN-PREDICATE-clause");
        }

        schemaProxy = New<TJoinSchemaProxy>(
            &joinClause->SelfJoinedColumns,
            &joinClause->ForeignJoinedColumns,
            sharedColumns,
            schemaProxy,
            foreignSourceProxy);

        query->JoinClauses.push_back(std::move(joinClause));
    }

    PrepareQuery(query, ast, schemaProxy, builder);

    if (auto groupClause = query->GroupClause) {
        auto keyColumns = query->GetKeyColumns();

        std::vector<bool> touchedKeyColumns(keyColumns.size(), false);
        for (const auto& item : groupClause->GroupItems) {
            if (auto referenceExpr = item.Expression->As<TReferenceExpression>()) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0) {
                    touchedKeyColumns[keyPartIndex] = true;
                }
            }
        }

        size_t keyPrefix = 0;
        for (; keyPrefix < touchedKeyColumns.size(); ++keyPrefix) {
            if (touchedKeyColumns[keyPrefix]) {
                continue;
            }

            const auto& expression = query->OriginalSchema.Columns()[keyPrefix].Expression;

            if (!expression) {
                break;
            }

            yhash_set<TString> references;
            auto evaluatedColumnExpression = PrepareExpression(
                expression.Get(),
                query->OriginalSchema,
                functions,
                &references);

            auto canEvaluate = true;
            for (const auto& reference : references) {
                int referenceIndex = query->OriginalSchema.GetColumnIndexOrThrow(reference);
                if (!touchedKeyColumns[referenceIndex]) {
                    canEvaluate = false;
                }
            }

            if (!canEvaluate) {
                break;
            }
        }

        bool containsPrimaryKey = keyPrefix == keyColumns.size();
        // not prefix, because of equal prefixes near borders

        query->UseDisjointGroupBy = containsPrimaryKey;

        LOG_DEBUG("Group key contains primary key, can omit top-level GROUP BY");
    }


    if (ast.Limit) {
        query->Limit = ast.Limit;
    } else if (query->OrderClause) {
        THROW_ERROR_EXCEPTION("ORDER BY used without LIMIT");
    }

    auto queryFingerprint = InferName(query, true);
    LOG_DEBUG("Prepared query (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v)",
        queryFingerprint,
        query->GetReadSchema(),
        query->GetTableSchema());

    auto range = GetBothBoundsFromDataSplit(selfDataSplit);

    SmallVector<TRowRange, 1> rowRanges;
    auto buffer = New<TRowBuffer>(TQueryPreparerBufferTag());
    rowRanges.push_back({
        buffer->Capture(range.first.Get()),
        buffer->Capture(range.second.Get())});

    TDataRanges dataSource;
    dataSource.Id = GetObjectIdFromDataSplit(selfDataSplit);
    dataSource.Ranges = MakeSharedRange(std::move(rowRanges), std::move(buffer));

    return std::make_pair(query, dataSource);
}

TQueryPtr PrepareJobQuery(
    const TString& source,
    const TTableSchema& tableSchema,
    const TFetchFunctions& fetchFunctions)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>(), NAst::TAliasMap()};
    ParseQueryString(
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

    auto parsedQueryInfo = std::make_pair(ast, astHead.second);

    auto unlimited = std::numeric_limits<i64>::max();

    auto query = New<TQuery>(unlimited, unlimited, TGuid::Create());
    query->OriginalSchema = tableSchema;

    TSchemaProxyPtr schemaProxy = New<TScanSchemaProxy>(
        tableSchema,
        TString(),
        &query->SchemaMapping);

    auto functionNames = ExtractFunctionNames(parsedQueryInfo);

    auto functions = New<TTypeInferrerMap>();
    fetchFunctions(functionNames, functions);

    TTypedExpressionBuilder builder{
        source,
        functions,
        parsedQueryInfo.second};

    PrepareQuery(
        query,
        parsedQueryInfo.first,
        schemaProxy,
        builder);

    return query;
}

TConstExpressionPtr PrepareExpression(
    const TString& source,
    TTableSchema tableSchema,
    const TConstTypeInferrerMapPtr& functions,
    yhash_set<TString>* references)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TExpressionPtr>(), NAst::TAliasMap()};
    ParseQueryString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseExpression);

    auto& expr = astHead.first.As<NAst::TExpressionPtr>();

    std::vector<TColumnDescriptor> mapping;
    auto schemaProxy = New<TScanSchemaProxy>(tableSchema, "", &mapping);

    TTypedExpressionBuilder builder{
        source,
        functions,
        astHead.second};

    auto result = builder.BuildTypedExpression(expr.Get(), schemaProxy);

    auto actualType = result->Type;
    if (actualType == EValueType::Null) {
        THROW_ERROR_EXCEPTION("Type inference failed")
            << TErrorAttribute("source", InferName(expr.Get()))
            << TErrorAttribute("actual_type", actualType);
    }

    if (references) {
        for (const auto& item : mapping) {
            references->insert(item.Name);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
