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

static const auto& Logger = QueryClientLogger;
static const int PlanFragmentDepthLimit = 50;

struct TQueryPreparerBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

namespace {

typedef std::pair<NAst::TQuery, NAst::TAliasMap> TParsedQueryInfo;

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<Stroka>* functions);

void ExtractFunctionNames(
    const NAst::TExpressionPtr& expr,
    std::vector<Stroka>* functions)
{
    if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
        functions->push_back(to_lower(functionExpr->FunctionName));
        ExtractFunctionNames(functionExpr->Arguments, functions);
    } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
        ExtractFunctionNames(unaryExpr->Operand, functions);
    } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
        ExtractFunctionNames(binaryExpr->Lhs, functions);
        ExtractFunctionNames(binaryExpr->Rhs, functions);
    } else if (expr->As<NAst::TInExpression>()) {
    } else if (expr->As<NAst::TLiteralExpression>()) {
    } else if (expr->As<NAst::TReferenceExpression>()) {
    } else {
        Y_UNREACHABLE();
    }
}

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<Stroka>* functions)
{
    if (!exprs) {
        return;
    }

    for (const auto& expr : exprs.Get()) {
        ExtractFunctionNames(expr, functions);
    }
}

std::vector<Stroka> ExtractFunctionNames(
    const TParsedQueryInfo& parsedQueryInfo)
{
    std::vector<Stroka> functions;

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
    if (value.Type == targetType) {
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

EValueType InferUnaryExprType(EUnaryOp opCode, EValueType operandType, const TStringBuf& source)
{
    if (operandType == EValueType::Null) {
        return EValueType::Null;
    }

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
            Y_UNREACHABLE();
    }
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
        case NAst::TLiteralValue::TagOf<Stroka>():
            return EValueType::String;
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
        case NAst::TLiteralValue::TagOf<Stroka>():
            return MakeUnversionedStringValue(
                literalValue.As<Stroka>().c_str(),
                literalValue.As<Stroka>().length());
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

TConstExpressionPtr FoldConstants(
    EUnaryOp opcode,
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
                        break;
                    default:
                        Y_UNREACHABLE();
                }
                return value;
            }
        }
        return TNullable<TUnversionedValue>();
    };

    if (auto value = foldConstants(opcode, operand)) {
        return New<TLiteralExpression>(value->Type, *value);
    }

    return TConstExpressionPtr();
}

TConstExpressionPtr FoldConstants(
    EBinaryOp opcode,
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

                YCHECK(lhs.Type == rhs.Type);
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
        return TNullable<TUnversionedValue>();
    };

    if (auto value = foldConstants(opcode, lhsExpr, rhsExpr)) {
        return New<TLiteralExpression>(value->Type, *value);
    }

    if (opcode == EBinaryOp::Divide) {
        auto lhsBinaryExpr = lhsExpr->As<TBinaryOpExpression>();
        auto rhsLiteralExpr = rhsExpr->As<TLiteralExpression>();
        if (lhsBinaryExpr && rhsLiteralExpr && lhsBinaryExpr->Opcode == EBinaryOp::Divide) {
            auto targetType = lhsBinaryExpr->Type;
            auto lhsLiteralExpr = lhsBinaryExpr->Rhs->As<TLiteralExpression>();
            if (lhsLiteralExpr) {
                TUnversionedValue lhs = lhsLiteralExpr->Value;
                TUnversionedValue rhs = rhsLiteralExpr->Value;

                YCHECK(targetType == rhs.Type);
                YCHECK(IsArithmeticType(targetType));

                if (lhs.Type != rhs.Type) {
                    if (IsArithmeticType(lhs.Type)) {
                        lhs = CastValueWithCheck(lhs, targetType);
                    } else {
                        ThrowTypeMismatchError(
                            lhs.Type,
                            rhs.Type,
                            "",
                            InferName(lhsLiteralExpr),
                            InferName(rhsLiteralExpr));
                    }
                }

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
    Y_UNREACHABLE();
}

struct TTypedExpressionBuilder;

DECLARE_REFCOUNTED_CLASS(ISchemaProxy)

class ISchemaProxy
    : public TIntrinsicRefCounted
{
public:
    virtual TNullable<TBaseColumn> GetColumnPtr(
        const Stroka& name,
        const Stroka& tableName) = 0;

    virtual TBaseColumn GetAggregateColumnPtr(
        const Stroka& name,
        const TAggregateTypeInferrer* aggregateFunction,
        const NAst::TExpression* arguments,
        Stroka subexprName,
        const TTypedExpressionBuilder& builder) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemaProxy)

struct TTypedExpressionBuilder
{
    const Stroka& Source;
    const TConstTypeInferrerMapPtr& Functions;
    const NAst::TAliasMap& AliasMap;

    TConstExpressionPtr DoBuildTypedExpression(
        const NAst::TExpression* expr,
        ISchemaProxyPtr schema,
        std::set<Stroka>& usedAliases) const
    {
        if (auto literalExpr = expr->As<NAst::TLiteralExpression>()) {
            const auto& literalValue = literalExpr->Value;

            return New<TLiteralExpression>(
                GetType(literalValue),
                GetValue(literalValue));
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
                        auto aliasExpr = DoBuildTypedExpression(
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

            return New<TReferenceExpression>(column->Type, column->Name);
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

                    return New<TReferenceExpression>(aggregateColumn.Type, aggregateColumn.Name);

                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error creating aggregate")
                        << TErrorAttribute("source", functionExpr->GetSource(Source))
                        << ex;
                }
            } else if (const auto* regularFunction = descriptor->As<TFunctionTypeInferrer>()) {
                std::vector<EValueType> types;
                std::vector<TConstExpressionPtr> typedOperands;
                for (const auto& argument : functionExpr->Arguments) {
                    auto typedArgument = DoBuildTypedExpression(
                        argument.Get(),
                        schema,
                        usedAliases);
                    types.push_back(typedArgument->Type);
                    typedOperands.push_back(typedArgument);
                }

                auto type = regularFunction->InferResultType(types, functionName, functionExpr->GetSource(Source));

                if (type == EValueType::Null) {
                    return New<TLiteralExpression>(
                        EValueType::Null,
                        MakeUnversionedSentinelValue(EValueType::Null));
                } else {
                    return New<TFunctionExpression>(
                        type,
                        functionName,
                        typedOperands);
                }
            }
        } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
            if (unaryExpr->Operand.size() != 1) {
                THROW_ERROR_EXCEPTION(
                    "Unary operator %Qv must have exactly one argument",
                    unaryExpr->Opcode);
            }

            auto typedOperand = DoBuildTypedExpression(
                unaryExpr->Operand.front().Get(),
                schema,
                usedAliases);

            if (auto foldedExpr = FoldConstants(unaryExpr->Opcode, typedOperand)) {
                return foldedExpr;
            } else {
                auto type = InferUnaryExprType(
                    unaryExpr->Opcode,
                    typedOperand->Type,
                    unaryExpr->GetSource(Source));

                if (type == EValueType::Null) {
                    return New<TLiteralExpression>(
                        EValueType::Null,
                        MakeUnversionedSentinelValue(EValueType::Null));
                } else {
                    return New<TUnaryOpExpression>(
                        type,
                        unaryExpr->Opcode,
                        typedOperand);
                }
            }
        } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
            auto makeBinaryExpr = [&] (EBinaryOp op, TConstExpressionPtr lhs, TConstExpressionPtr rhs) -> TConstExpressionPtr {
                if (auto foldedExpr = FoldConstants(op, lhs, rhs)) {
                    return foldedExpr;
                } else {
                    auto cast = [] (
                        const TLiteralExpression* literalExpr,
                        EValueType targetType) -> TConstExpressionPtr
                    {
                        auto literalType = literalExpr->Type;
                        if (literalType != targetType) {
                            if (IsArithmeticType(literalType) && IsArithmeticType(targetType)) {
                                // Cast literalType to targetType
                                return New<TLiteralExpression>(targetType, CastValueWithCheck(literalExpr->Value, targetType));
                            }
                        }

                        return literalExpr;
                    };

                    auto lhsLiteral = lhs->As<TLiteralExpression>();
                    auto rhsLiteral = rhs->As<TLiteralExpression>();

                    YCHECK(!lhsLiteral || !rhsLiteral);

                    auto lhsType = lhs->Type;
                    auto rhsType = rhs->Type;

                    if (lhsLiteral) {
                        lhs = cast(lhsLiteral, rhsType);
                    }

                    if (rhsLiteral) {
                        rhs = cast(rhsLiteral, lhsType);
                    }

                    auto type = InferBinaryExprType(
                        op,
                        lhs->Type,
                        rhs->Type,
                        binaryExpr->GetSource(Source),
                        InferName(lhs),
                        InferName(rhs));

                    if (type == EValueType::Null) {
                        return New<TLiteralExpression>(
                            EValueType::Null,
                            MakeUnversionedSentinelValue(EValueType::Null));
                    } else {
                        return New<TBinaryOpExpression>(type, op, lhs, rhs);
                    }

                }
            };

            std::function<TConstExpressionPtr(int, int, EBinaryOp)> gen = [&] (int offset, int keySize, EBinaryOp op) -> TConstExpressionPtr {
                auto typedLhs = DoBuildTypedExpression(
                    binaryExpr->Lhs[offset].Get(),
                    schema,
                    usedAliases);
                auto typedRhs = DoBuildTypedExpression(
                    binaryExpr->Rhs[offset].Get(),
                    schema,
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

                auto typedLhs = DoBuildTypedExpression(
                    binaryExpr->Lhs.front().Get(),
                    schema,
                    usedAliases);
                auto typedRhs = DoBuildTypedExpression(
                    binaryExpr->Rhs.front().Get(),
                    schema,
                    usedAliases);

                return makeBinaryExpr(binaryExpr->Opcode, typedLhs, typedRhs);
            }
        } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
            std::vector<TConstExpressionPtr> typedArguments;
            std::unordered_set<Stroka> columnNames;
            std::vector<EValueType> argTypes;

            for (const auto& argument : inExpr->Expr) {
                auto typedArgument = DoBuildTypedExpression(
                    argument.Get(),
                    schema,
                    usedAliases);

                typedArguments.push_back(typedArgument);
                argTypes.push_back(typedArgument->Type);
                if (auto reference = typedArgument->As<TReferenceExpression>()) {
                    if (!columnNames.insert(reference->ColumnName).second) {
                        THROW_ERROR_EXCEPTION("IN operator has multiple references to column %Qv", reference->ColumnName)
                            << TErrorAttribute("source", Source);
                    }
                }
            }

            auto capturedRows = LiteralTupleListToRows(inExpr->Values, argTypes, inExpr->GetSource(Source));
            return New<TInOpExpression>(std::move(typedArguments), std::move(capturedRows));
        }

        Y_UNREACHABLE();
    }

    TConstExpressionPtr BuildTypedExpression(
        const NAst::TExpression* expr,
        ISchemaProxyPtr schema) const
    {
        std::set<Stroka> usedAliases;
        return PropagateNotExpression(DoBuildTypedExpression(expr, schema, usedAliases));
    }

};

DECLARE_REFCOUNTED_CLASS(TSchemaProxy)

class TSchemaProxy
    : public ISchemaProxy
{
public:
    TSchemaProxy()
    { }

    TSchemaProxy(const yhash_map<std::pair<Stroka, Stroka>, TBaseColumn>& lookup)
        : Lookup_(lookup)
    { }

    virtual TNullable<TBaseColumn> GetColumnPtr(
        const Stroka& name,
        const Stroka& tableName) override
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

    virtual TBaseColumn GetAggregateColumnPtr(
        const Stroka& name,
        const TAggregateTypeInferrer* aggregateFunction,
        const NAst::TExpression* arguments,
        Stroka subexprName,
        const TTypedExpressionBuilder& builder) override
    {
        auto key = std::make_pair(subexprName, Stroka());
        auto found = Lookup_.find(key);
        if (found != Lookup_.end()) {
            return found->second;
        }

        auto column = ProvideAggregateColumn(
            name,
            aggregateFunction,
            arguments,
            subexprName,
            builder);

        YCHECK(Lookup_.emplace(key, column).second);
        return column;
    }

    virtual void Finish()
    { }

    const yhash_map<std::pair<Stroka, Stroka>, TBaseColumn>& GetLookup() const
    {
        return Lookup_;
    }

private:
    yhash_map<std::pair<Stroka, Stroka>, TBaseColumn> Lookup_;

protected:
    virtual TNullable<TBaseColumn> ProvideColumn(const Stroka& name, const Stroka& tableName)
    {
        return Null;
    }

    virtual TBaseColumn ProvideAggregateColumn(
        const Stroka& name,
        const TAggregateTypeInferrer* aggregateFunction,
        const NAst::TExpression* arguments,
        Stroka subexprName,
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
        const Stroka& tableName,
        std::vector<TColumnDescriptor>* mapping = nullptr)
        : Mapping_(mapping)
        , SourceTableSchema_(sourceTableSchema)
        , TableName_(tableName)
    { }

    virtual TNullable<TBaseColumn> ProvideColumn(const Stroka& name, const Stroka& tableName) override
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
    yhash_map<Stroka, size_t> ColumnsCollisions_;
    const TTableSchema SourceTableSchema_;
    Stroka TableName_;

    DECLARE_NEW_FRIEND();
};

class TJoinSchemaProxy
    : public TSchemaProxy
{
public:
    TJoinSchemaProxy(
        std::vector<Stroka>* selfJoinedColumns,
        std::vector<Stroka>* foreignJoinedColumns,
        const std::set<std::pair<Stroka, Stroka>>& sharedColumns,
        TSchemaProxyPtr self,
        TSchemaProxyPtr foreign)
        : SelfJoinedColumns_(selfJoinedColumns)
        , ForeignJoinedColumns_(foreignJoinedColumns)
        , SharedColumns_(sharedColumns)
        , Self_(self)
        , Foreign_(foreign)
    { }

    virtual TNullable<TBaseColumn> ProvideColumn(const Stroka& name, const Stroka& tableName) override
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
    std::vector<Stroka>* SelfJoinedColumns_;
    std::vector<Stroka>* ForeignJoinedColumns_;

    std::set<std::pair<Stroka, Stroka>> SharedColumns_;
    TSchemaProxyPtr Self_;
    TSchemaProxyPtr Foreign_;

};

const TNullable<TBaseColumn> FindColumn(const TNamedItemList& schema, const Stroka& name)
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

    virtual TNullable<TBaseColumn> ProvideColumn(const Stroka& name, const Stroka& tableName) override
    {
        if (!tableName.empty()) {
            return Null;
        }

        return FindColumn(*GroupItems_, name);
    }

    virtual TBaseColumn ProvideAggregateColumn(
        const Stroka& name,
        const TAggregateTypeInferrer* aggregateFunction,
        const NAst::TExpression* argument,
        Stroka subexprName,
        const TTypedExpressionBuilder& builder) override
    {
        auto typedOperand = builder.BuildTypedExpression(
            argument,
            Base_);

        auto stateType = aggregateFunction->InferStateType(typedOperand->Type, name, subexprName);
        auto resultType = aggregateFunction->InferResultType(typedOperand->Type, name, subexprName);

        CheckExpressionDepth(typedOperand);

        AggregateItems_->emplace_back(
            typedOperand,
            name,
            subexprName,
            stateType,
            resultType);

        return TBaseColumn(subexprName, resultType);
    }

private:
    const TNamedItemList* GroupItems_;
    TSchemaProxyPtr Base_;
    TAggregateItemList* AggregateItems_;

};

TConstExpressionPtr BuildWhereClause(
    const NAst::TExpressionList& expressionAst,
    const TSchemaProxyPtr& schemaProxy,
    const TTypedExpressionBuilder& builder)
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
        THROW_ERROR_EXCEPTION("WHERE-clause is not a boolean expression")
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
    groupClause->IsMerge = false;
    groupClause->IsFinal = true;
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
        query->WhereClause = BuildWhereClause(
            *wherePredicate,
            schemaProxy,
            builder);
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

} // namespace

////////////////////////////////////////////////////////////////////////////////

void DefaultFetchFunctions(const std::vector<Stroka>& names, const TTypeInferrerMapPtr& typeInferrers)
{
    MergeFrom(typeInferrers.Get(), *BuiltinTypeInferrersMap);
}

////////////////////////////////////////////////////////////////////////////////

// For testing
void ParseJobQuery(const Stroka& source)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>(), NAst::TAliasMap()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseJobQuery);
}

std::pair<TQueryPtr, TDataRanges> PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    const TFetchFunctions& fetchFunctions,
    i64 inputRowLimit,
    i64 outputRowLimit,
    TTimestamp timestamp)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>(), NAst::TAliasMap()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseQuery);

    const auto& ast = astHead.first.As<NAst::TQuery>();

    auto functionNames = ExtractFunctionNames(std::make_pair(ast, astHead.second));

    auto functions = New<TTypeInferrerMap>();
    fetchFunctions(functionNames, functions);

    TDataSplit selfDataSplit;

    auto query = New<TQuery>(inputRowLimit, outputRowLimit, TGuid::Create());
    TSchemaProxyPtr schemaProxy;

    auto table = ast.Table;
    LOG_DEBUG("Getting initial data split for %v", table.Path);

    selfDataSplit = WaitFor(callbacks->GetInitialSplit(table.Path, timestamp))
        .ValueOrThrow();
    auto tableSchema = GetTableSchemaFromDataSplit(selfDataSplit);
    query->OriginalSchema = tableSchema;

    schemaProxy = New<TScanSchemaProxy>(
        tableSchema,
        table.Alias,
        &query->SchemaMapping);

    TTypedExpressionBuilder builder{
        source,
        functions,
        astHead.second};

    for (const auto& join : ast.Joins) {
        auto foreignDataSplit = WaitFor(callbacks->GetInitialSplit(join.Table.Path, timestamp))
            .ValueOrThrow();

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
        std::set<std::pair<Stroka, Stroka>> sharedColumns;
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
                continue;
            }

            if (!foreignTableSchema.Columns()[keyPrefix].Expression) {
                break;
            }

            yhash_set<Stroka> references;
            auto evaluatedColumnExpression = PrepareExpression(
                foreignTableSchema.Columns()[keyPrefix].Expression.Get(),
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
        } else {
            joinClause->SelfEquations = std::move(selfEquations);
            joinClause->ForeignEquations = std::move(foreignEquations);
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

            yhash_set<Stroka> references;
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
    const Stroka& source,
    const TTableSchema& tableSchema,
    const TFetchFunctions& fetchFunctions)
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

    auto parsedQueryInfo = std::make_pair(ast, astHead.second);

    auto unlimited = std::numeric_limits<i64>::max();

    auto query = New<TQuery>(unlimited, unlimited, TGuid::Create());
    query->OriginalSchema = tableSchema;

    TSchemaProxyPtr schemaProxy = New<TScanSchemaProxy>(
        tableSchema,
        Stroka(),
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
    const Stroka& source,
    TTableSchema tableSchema,
    const TConstTypeInferrerMapPtr& functions,
    yhash_set<Stroka>* references)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TExpressionPtr>(), NAst::TAliasMap()};
    ParseYqlString(
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
