#include "query_preparer.h"

#include "callbacks.h"
#include "functions.h"
#include "helpers.h"
#include "lexer.h"
#include "parser.h"
#include "query_helpers.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/misc/variant.h>

#include <unordered_set>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t MaxExpressionDepth = 50;

struct TQueryPreparerBufferTag
{ };

constexpr ssize_t MaxQueryLimit = 10000000;

////////////////////////////////////////////////////////////////////////////////

namespace {

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<TString>* functions);

void ExtractFunctionNames(
    const NAst::TWhenThenExpressionList& exprs,
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
    } else if (auto betweenExpr = expr->As<NAst::TBetweenExpression>()) {
        ExtractFunctionNames(betweenExpr->Expr, functions);
    } else if (auto transformExpr = expr->As<NAst::TTransformExpression>()) {
        ExtractFunctionNames(transformExpr->Expr, functions);
        ExtractFunctionNames(transformExpr->DefaultExpr, functions);
    } else if (auto caseExpr = expr->As<NAst::TCaseExpression>()) {
        ExtractFunctionNames(caseExpr->OptionalOperand, functions);
        ExtractFunctionNames(caseExpr->WhenThenExpressions, functions);
        ExtractFunctionNames(caseExpr->DefaultExpression, functions);
    } else if (auto likeExpr = expr->As<NAst::TLikeExpression>()) {
        ExtractFunctionNames(likeExpr->Text, functions);
        ExtractFunctionNames(likeExpr->Pattern, functions);
        ExtractFunctionNames(likeExpr->EscapeCharacter, functions);
    } else if (expr->As<NAst::TLiteralExpression>()) {
    } else if (expr->As<NAst::TReferenceExpression>()) {
    } else if (expr->As<NAst::TAliasExpression>()) {
    } else {
        YT_ABORT();
    }
}

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<TString>* functions)
{
    if (!exprs) {
        return;
    }

    CheckStackDepth();

    for (const auto& expr : *exprs) {
        ExtractFunctionNames(expr, functions);
    }
}

void ExtractFunctionNames(
    const NAst::TWhenThenExpressionList& whenThenExpressions,
    std::vector<TString>* functions)
{
    CheckStackDepth();

    for (const auto& [condition, result] : whenThenExpressions) {
        ExtractFunctionNames(condition, functions);
        ExtractFunctionNames(result, functions);
    }
}

std::vector<TString> ExtractFunctionNames(
    const NAst::TQuery& query,
    const NAst::TAliasMap& aliasMap)
{
    std::vector<TString> functions;

    ExtractFunctionNames(query.WherePredicate, &functions);
    ExtractFunctionNames(query.HavingPredicate, &functions);
    ExtractFunctionNames(query.SelectExprs, &functions);

    if (query.GroupExprs) {
        for (const auto& expr : query.GroupExprs->first) {
            ExtractFunctionNames(expr, &functions);
        }
    }

    for (const auto& join : query.Joins) {
        Visit(join,
            [&] (const NAst::TJoin& tableJoin) {
                ExtractFunctionNames(tableJoin.Lhs, &functions);
                ExtractFunctionNames(tableJoin.Rhs, &functions);
            },
            [&] (const NAst::TArrayJoin& arrayJoin) {
                ExtractFunctionNames(arrayJoin.Columns, &functions);
            });
    }

    for (const auto& orderExpression : query.OrderExpressions) {
        for (const auto& expr : orderExpression.first) {
            ExtractFunctionNames(expr, &functions);
        }
    }

    for (const auto& aliasedExpression : aliasMap) {
        ExtractFunctionNames(aliasedExpression.second, &functions);
    }

    std::sort(functions.begin(), functions.end());
    functions.erase(
        std::unique(functions.begin(), functions.end()),
        functions.end());

    return functions;
}

////////////////////////////////////////////////////////////////////////////////

TTypeSet ComparableTypes({
    EValueType::Boolean,
    EValueType::Int64,
    EValueType::Uint64,
    EValueType::Double,
    EValueType::String});

////////////////////////////////////////////////////////////////////////////////

EValueType GetType(const NAst::TLiteralValue& literalValue)
{
    return Visit(literalValue,
        [] (const NAst::TNullLiteralValue&) {
            return EValueType::Null;
        },
        [] (i64) {
            return EValueType::Int64;
        },
        [] (ui64) {
            return EValueType::Uint64;
        },
        [] (double) {
            return EValueType::Double;
        },
        [] (bool) {
            return EValueType::Boolean;
        },
        [] (const TString&) {
            return EValueType::String;
        });
}

TTypeSet GetTypes(const NAst::TLiteralValue& literalValue)
{
    return Visit(literalValue,
        [] (const NAst::TNullLiteralValue&) {
            return TTypeSet({
                EValueType::Null,
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::String,
                EValueType::Any
            });
        },
        [] (i64) {
            return TTypeSet({
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double
            });
        },
        [] (ui64) {
            return TTypeSet({
                EValueType::Uint64,
                EValueType::Double
            });
        },
        [] (double) {
            return TTypeSet({
                EValueType::Double
            });
        },
        [] (bool) {
            return TTypeSet({
                EValueType::Boolean
            });
        },
        [] (const TString&) {
            return TTypeSet({
                EValueType::String
            });
        });
}

TValue GetValue(const NAst::TLiteralValue& literalValue)
{
    return Visit(literalValue,
        [] (const NAst::TNullLiteralValue&) {
            return MakeUnversionedSentinelValue(EValueType::Null);
        },
        [] (i64 value) {
            return MakeUnversionedInt64Value(value);
        },
        [] (ui64 value) {
            return MakeUnversionedUint64Value(value);
        },
        [] (double value) {
            return MakeUnversionedDoubleValue(value);
        },
        [] (bool value) {
            return MakeUnversionedBooleanValue(value);
        },
        [] (const TString& value) {
            return MakeUnversionedStringValue(TStringBuf(value.c_str(), value.length()));
        });
}

void BuildRow(
    TUnversionedRowBuilder* rowBuilder,
    const NAst::TLiteralValueTuple& tuple,
    const std::vector<EValueType>& argTypes,
    TStringBuf source)
{
    for (int i = 0; i < std::ssize(tuple); ++i) {
        auto valueType = GetType(tuple[i]);
        auto value = GetValue(tuple[i]);

        if (valueType == EValueType::Null) {
            value = MakeUnversionedSentinelValue(EValueType::Null);
        } else if (valueType != argTypes[i]) {
            if (IsArithmeticType(valueType) && IsArithmeticType(argTypes[i])) {
                value = CastValueWithCheck(value, argTypes[i]);
            } else {
                THROW_ERROR_EXCEPTION("Types mismatch in tuple")
                << TErrorAttribute("source", source)
                << TErrorAttribute("actual_type", valueType)
                << TErrorAttribute("expected_type", argTypes[i]);
            }
        }
        rowBuilder->AddValue(value);
    }
}

TSharedRange<TRow> LiteralTupleListToRows(
    const NAst::TLiteralValueTupleList& literalTuples,
    const std::vector<EValueType>& argTypes,
    TStringBuf source)
{
    auto rowBuffer = New<TRowBuffer>(TQueryPreparerBufferTag());
    TUnversionedRowBuilder rowBuilder;
    std::vector<TRow> rows;
    rows.reserve(literalTuples.size());
    for (const auto& tuple : literalTuples) {
        if (tuple.size() != argTypes.size()) {
            THROW_ERROR_EXCEPTION("Arguments size mismatch in tuple")
                << TErrorAttribute("source", source);
        }

        BuildRow(&rowBuilder, tuple, argTypes, source);

        rows.push_back(rowBuffer->CaptureRow(rowBuilder.GetRow()));
        rowBuilder.Reset();
    }

    std::sort(rows.begin(), rows.end());
    return MakeSharedRange(std::move(rows), std::move(rowBuffer));
}

TSharedRange<TRowRange> LiteralRangesListToRows(
    const NAst::TLiteralValueRangeList& literalRanges,
    const std::vector<EValueType>& argTypes,
    TStringBuf source)
{
    auto rowBuffer = New<TRowBuffer>(TQueryPreparerBufferTag());
    TUnversionedRowBuilder rowBuilder;
    std::vector<TRowRange> ranges;
    for (const auto& range : literalRanges) {
        if (range.first.size() > argTypes.size()) {
            THROW_ERROR_EXCEPTION("Arguments size mismatch in tuple")
                << TErrorAttribute("source", source);
        }

        if (range.second.size() > argTypes.size()) {
            THROW_ERROR_EXCEPTION("Arguments size mismatch in tuple")
                << TErrorAttribute("source", source);
        }

        BuildRow(&rowBuilder, range.first, argTypes, source);
        auto lower = rowBuffer->CaptureRow(rowBuilder.GetRow());
        rowBuilder.Reset();

        BuildRow(&rowBuilder, range.second, argTypes, source);
        auto upper = rowBuffer->CaptureRow(rowBuilder.GetRow());
        rowBuilder.Reset();

        if (CompareRows(lower, upper, std::min(lower.GetCount(), upper.GetCount())) > 0) {
            THROW_ERROR_EXCEPTION("Lower bound is greater than upper")
                << TErrorAttribute("lower", lower)
                << TErrorAttribute("upper", upper);
        }

        ranges.emplace_back(lower, upper);
    }

    std::sort(ranges.begin(), ranges.end());

    for (int index = 1; index < std::ssize(ranges); ++index) {
        TRow previousUpper = ranges[index - 1].second;
        TRow currentLower = ranges[index].first;

        if (CompareRows(
            previousUpper,
            currentLower,
            std::min(previousUpper.GetCount(), currentLower.GetCount())) >= 0)
        {
            THROW_ERROR_EXCEPTION("Ranges are not disjoint")
                << TErrorAttribute("first", ranges[index - 1])
                << TErrorAttribute("second", ranges[index]);
        }
    }

    return MakeSharedRange(std::move(ranges), std::move(rowBuffer));
}

std::optional<TUnversionedValue> FoldConstants(
    EUnaryOp opcode,
    const TConstExpressionPtr& operand)
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
                case EValueType::Null:
                    break;
                default:
                    YT_ABORT();
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
                case EValueType::Null:
                    break;
                default:
                    YT_ABORT();
            }
            return value;
        }
    }
    return std::nullopt;
}

std::optional<TUnversionedValue> FoldConstants(
    EBinaryOp opcode,
    const TConstExpressionPtr& lhsExpr,
    const TConstExpressionPtr& rhsExpr)
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
            YT_VERIFY(lhs.Type == EValueType::Null || lhs.Type == EValueType::Boolean);
            YT_VERIFY(rhs.Type == EValueType::Null || rhs.Type == EValueType::Boolean);

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
            case EBinaryOp::Concatenate:
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

                        if (lhs.Data.Int64 == std::numeric_limits<i64>::min() && rhs.Data.Int64 == -1) {
                            THROW_ERROR_EXCEPTION("Division of INT_MIN by -1");
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

                        if (lhs.Data.Int64 == std::numeric_limits<i64>::min() && rhs.Data.Int64 == -1) {
                            THROW_ERROR_EXCEPTION("Division of INT_MIN by -1");
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
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNan(lhs, rhs) == 0);
                break;
            case EBinaryOp::NotEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNan(lhs, rhs) != 0);
                break;
            case EBinaryOp::Less:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNan(lhs, rhs) < 0);
                break;
            case EBinaryOp::Greater:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNan(lhs, rhs) > 0);
                break;
            case EBinaryOp::LessOrEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNan(lhs, rhs) <= 0);
                break;
            case EBinaryOp::GreaterOrEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNan(lhs, rhs) >= 0);
                break;
            default:
                break;
        }
    }
    return std::nullopt;
}

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
                            operandBinaryOp->Lhs->GetWireType(),
                            EUnaryOp::Not,
                            operandBinaryOp->Lhs),
                        New<TUnaryOpExpression>(
                            operandBinaryOp->Rhs->GetWireType(),
                            EUnaryOp::Not,
                            operandBinaryOp->Rhs)));
                } else if (operandBinaryOp->Opcode == EBinaryOp::Or) {
                    return Visit(New<TBinaryOpExpression>(
                        EValueType::Boolean,
                        EBinaryOp::And,
                        New<TUnaryOpExpression>(
                            operandBinaryOp->Lhs->GetWireType(),
                            EUnaryOp::Not,
                            operandBinaryOp->Lhs),
                        New<TUnaryOpExpression>(
                            operandBinaryOp->Rhs->GetWireType(),
                            EUnaryOp::Not,
                            operandBinaryOp->Rhs)));
                } else if (IsRelationalBinaryOp(operandBinaryOp->Opcode)) {
                    return Visit(New<TBinaryOpExpression>(
                        operandBinaryOp->GetWireType(),
                        GetInversedBinaryOpcode(operandBinaryOp->Opcode),
                        operandBinaryOp->Lhs,
                        operandBinaryOp->Rhs));
                }
            } else if (auto literal = operand->As<TLiteralExpression>()) {
                TUnversionedValue value = literal->Value;
                value.Data.Boolean = !value.Data.Boolean;
                return New<TLiteralExpression>(
                    literal->GetWireType(),
                    value);
            }
        }

        return TBase::OnUnary(unaryExpr);
    }
};

struct TCastEliminator
    : TRewriter<TCastEliminator>
{
    using TBase = TRewriter<TCastEliminator>;

    TConstExpressionPtr OnFunction(const TFunctionExpression* functionExpr)
    {
        if (IsUserCastFunction(functionExpr->FunctionName)) {
            YT_VERIFY(functionExpr->Arguments.size() == 1);

            if (*functionExpr->LogicalType == *functionExpr->Arguments[0]->LogicalType) {
                return Visit(functionExpr->Arguments[0]);
            }
        }

        return TBase::OnFunction(functionExpr);
    }
};

struct TExpressionSimplifier
    : TRewriter<TExpressionSimplifier>
{
    using TBase = TRewriter<TExpressionSimplifier>;

    TConstExpressionPtr OnFunction(const TFunctionExpression* functionExpr)
    {
        if (functionExpr->FunctionName == "if") {
            if (auto functionCondition = functionExpr->Arguments[0]->As<TFunctionExpression>()) {
                auto reference1 = functionExpr->Arguments[2]->As<TReferenceExpression>();
                if (functionCondition->FunctionName == "is_null" && reference1) {
                    auto reference0 = functionCondition->Arguments[0]->As<TReferenceExpression>();
                    if (reference0 && reference1->ColumnName == reference0->ColumnName) {
                        return New<TFunctionExpression>(
                            functionExpr->GetWireType(),
                            "if_null",
                            std::vector<TConstExpressionPtr>{
                                functionCondition->Arguments[0],
                                functionExpr->Arguments[1]});

                    }
                }
            }
        }

        return TBase::OnFunction(functionExpr);
    }
};

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

EValueType GetFrontWithCheck(const TTypeSet& typeSet, TStringBuf source)
{
    auto result = typeSet.GetFront();
    if (result == EValueType::Null) {
        THROW_ERROR_EXCEPTION("Type inference failed")
            << TErrorAttribute("actual_type", EValueType::Null)
            << TErrorAttribute("source", source);
    }
    return result;
}

TTypeSet InferFunctionTypes(
    const TFunctionTypeInferrer* inferrer,
    const std::vector<TTypeSet>& effectiveTypes,
    std::vector<TTypeSet>* genericAssignments,
    TStringBuf functionName,
    TStringBuf source)
{
    std::vector<TTypeSet> typeConstraints;
    std::vector<int> formalArguments;
    std::optional<std::pair<int, bool>> repeatedType;
    int formalResultType = inferrer->GetNormalizedConstraints(
        &typeConstraints,
        &formalArguments,
        &repeatedType);

    *genericAssignments = typeConstraints;

    int argIndex = 1;
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

    bool hasNoRepeatedArgument = !repeatedType.operator bool();

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
        int constraintIndex = repeatedType->first;
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
    int argumentCount,
    std::vector<TTypeSet>* genericAssignments,
    TStringBuf source)
{
    std::vector<TTypeSet> typeConstraints;
    std::vector<int> formalArguments;
    std::optional<std::pair<int, bool>> repeatedType;
    int formalResultType = inferrer->GetNormalizedConstraints(
        &typeConstraints,
        &formalArguments,
        &repeatedType);

    (*genericAssignments)[formalResultType] = TTypeSet({resultType});

    std::vector<EValueType> genericAssignmentsMin;
    for (auto& constraint : *genericAssignments) {
        genericAssignmentsMin.push_back(GetFrontWithCheck(constraint, source));
    }

    std::vector<EValueType> effectiveTypes;
    effectiveTypes.reserve(argumentCount);
    int argIndex = 0;
    auto formalArg = formalArguments.begin();
    for (;
        formalArg != formalArguments.end() && argIndex < argumentCount;
        ++formalArg, ++argIndex)
    {
        effectiveTypes.push_back(genericAssignmentsMin[*formalArg]);
    }

    for (; argIndex < argumentCount; ++argIndex) {
        int constraintIndex = repeatedType->first;
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

void IntersectGenericsWithArgumentTypes(
    const std::vector<TTypeSet>& effectiveTypes,
    std::vector<TTypeSet>* genericAssignments,
    const std::vector<int>& formalArguments,
    TStringBuf functionName,
    TStringBuf source)
{
    if (formalArguments.size() != effectiveTypes.size()) {
        THROW_ERROR_EXCEPTION("Expected %v number of arguments to function %Qv, got %v",
            formalArguments.size(),
            functionName,
            effectiveTypes.size());
    }

    for (int argIndex = 0; argIndex < std::ssize(formalArguments); ++argIndex)
    {
        auto& constraints = (*genericAssignments)[formalArguments[argIndex]];
        if (!Unify(&constraints, effectiveTypes[argIndex])) {
            THROW_ERROR_EXCEPTION("Wrong type for argument %v to function %Qv: expected %Qv, got %Qv",
                argIndex + 1,
                functionName,
                constraints,
                effectiveTypes[argIndex])
                << TErrorAttribute("expression", source);
        }
    }
}

std::vector<EValueType> RefineFunctionTypes(
    int formalResultType,
    int formalStateType,
    const std::vector<int>& formalArguments,
    EValueType resultType,
    EValueType* stateType,
    std::vector<TTypeSet>* genericAssignments,
    TStringBuf source)
{
    (*genericAssignments)[formalResultType] = TTypeSet({resultType});

    std::vector<EValueType> genericAssignmentsMin;
    for (const auto& constraint : *genericAssignments) {
        genericAssignmentsMin.push_back(GetFrontWithCheck(constraint, source));
    }

    *stateType = genericAssignmentsMin[formalStateType];

    std::vector<EValueType> effectiveTypes;
    for (int formalArgConstraint : formalArguments)
    {
        effectiveTypes.push_back(genericAssignmentsMin[formalArgConstraint]);
    }

    return effectiveTypes;
}

struct TOperatorTyper
{
    TTypeSet Constraint;
    std::optional<EValueType> ResultType;
};

TEnumIndexedArray<EBinaryOp, TOperatorTyper> BuildBinaryOperatorTypers()
{
    TEnumIndexedArray<EBinaryOp, TOperatorTyper> result;

    for (auto op : {
        EBinaryOp::Plus,
        EBinaryOp::Minus,
        EBinaryOp::Multiply,
        EBinaryOp::Divide})
    {
        result[op] = {
            TTypeSet({EValueType::Int64, EValueType::Uint64, EValueType::Double}),
            std::nullopt
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
            TTypeSet({EValueType::Int64, EValueType::Uint64}),
            std::nullopt
        };
    }

    for (auto op : {
        EBinaryOp::And,
        EBinaryOp::Or})
    {
        result[op] = {
            TTypeSet({EValueType::Boolean}),
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
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::String,
                EValueType::Any}),
            EValueType::Boolean
        };
    }

    for (auto op : {EBinaryOp::Concatenate}) {
        result[op] = {
            TTypeSet({ EValueType::String, }),
            EValueType::String
        };
    }

    return result;
}

const TEnumIndexedArray<EBinaryOp, TOperatorTyper>& GetBinaryOperatorTypers()
{
    static auto result = BuildBinaryOperatorTypers();
    return result;
}

TEnumIndexedArray<EUnaryOp, TOperatorTyper> BuildUnaryOperatorTypers()
{
    TEnumIndexedArray<EUnaryOp, TOperatorTyper> result;

    for (auto op : {
        EUnaryOp::Plus,
        EUnaryOp::Minus})
    {
        result[op] = {
            TTypeSet({EValueType::Int64, EValueType::Uint64, EValueType::Double}),
            std::nullopt
        };
    }

    result[EUnaryOp::BitNot] = {
        TTypeSet({EValueType::Int64, EValueType::Uint64}),
        std::nullopt
    };

    result[EUnaryOp::Not] = {
        TTypeSet({EValueType::Boolean}),
        std::nullopt
    };

    return result;
}

const TEnumIndexedArray<EUnaryOp, TOperatorTyper>& GetUnaryOperatorTypers()
{
    static auto result = BuildUnaryOperatorTypers();
    return result;
}

TTypeSet InferBinaryExprTypes(
    EBinaryOp opCode,
    const TTypeSet& lhsTypes,
    const TTypeSet& rhsTypes,
    TTypeSet* genericAssignments,
    TStringBuf lhsSource,
    TStringBuf rhsSource)
{
    if (IsRelationalBinaryOp(opCode) && (lhsTypes & rhsTypes).IsEmpty()) {
        return TTypeSet{EValueType::Boolean};
    }

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
    const TTypeSet& lhsTypes,
    const TTypeSet& rhsTypes,
    TTypeSet* genericAssignments,
    TStringBuf lhsSource,
    TStringBuf rhsSource,
    TStringBuf source)
{
    if (IsRelationalBinaryOp(opCode) && (lhsTypes & rhsTypes).IsEmpty()) {
        // Empty intersection (Any, alpha) || (alpha, Any), where alpha = {bool, int, uint, double, string}
        if (lhsTypes.Get(EValueType::Any)) {
            return std::pair(EValueType::Any, GetFrontWithCheck(rhsTypes, rhsSource));
        }

        if (rhsTypes.Get(EValueType::Any)) {
            return std::pair(GetFrontWithCheck(lhsTypes, lhsSource), EValueType::Any);
        }

        THROW_ERROR_EXCEPTION("Type mismatch in expression")
            << TErrorAttribute("lhs_source", lhsSource)
            << TErrorAttribute("rhs_source", rhsSource);
    }

    const auto& binaryOperators = GetBinaryOperatorTypers();

    EValueType argType;
    if (binaryOperators[opCode].ResultType) {
        argType = GetFrontWithCheck(*genericAssignments, source);
    } else {
        YT_VERIFY(genericAssignments->Get(resultType));
        argType = resultType;
    }

    return std::pair(argType, argType);
}

TTypeSet InferUnaryExprTypes(
    EUnaryOp opCode,
    const TTypeSet& argTypes,
    TTypeSet* genericAssignments,
    TStringBuf opSource)
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
    TTypeSet* genericAssignments,
    TStringBuf opSource)
{
    const auto& unaryOperators = GetUnaryOperatorTypers();

    EValueType argType;
    if (unaryOperators[opCode].ResultType) {
        argType = GetFrontWithCheck(*genericAssignments, opSource);
    } else {
        YT_VERIFY(genericAssignments->Get(resultType));
        argType = resultType;
    }

    return argType;
}

////////////////////////////////////////////////////////////////////////////////

struct TBaseColumn
{
    TBaseColumn(const TString& name, TLogicalTypePtr type)
        : Name(name)
        , LogicalType(type)
    { }

    TString Name;
    TLogicalTypePtr LogicalType;
};


struct TBuilderCtxBase
{
private:
    struct TTable
    {
        const TTableSchema& Schema;
        std::optional<TString> Alias;
        std::vector<TColumnDescriptor>* Mapping = nullptr;
    };

    // TODO: Enrich TMappedSchema with alias and keep here pointers to TMappedSchema.
    std::vector<TTable> Tables;

protected:
    // TODO: Combine in Structure? Move out?
    const TNamedItemList* GroupItems = nullptr;
    TAggregateItemList* AggregateItems = nullptr;

    bool AfterGroupBy = false;

public:
    struct TColumnEntry
    {
        TBaseColumn Column;

        size_t LastTableIndex;
        size_t OriginTableIndex;
    };

    //! Lookup is a cache of resolved columns.
    THashMap<
        NAst::TReference,
        TColumnEntry,
        NAst::TCompositeAgnosticReferenceHasher,
        NAst::TCompositeAgnosticReferenceEqComparer> Lookup;

    TBuilderCtxBase(
        const TTableSchema& schema,
        std::optional<TString> alias,
        std::vector<TColumnDescriptor>* mapping)
    {
        Tables.push_back(TTable{schema, alias, mapping});
    }

    // Columns already presented in Lookup are shared.
    // In mapping presented all columns needed for read and renamed schema.
    // SelfJoinedColumns and ForeignJoinedColumns are built from Lookup using OriginTableIndex and LastTableIndex.
    void Merge(TBuilderCtxBase& other)
    {
        size_t otherTablesCount = other.Tables.size();
        size_t tablesCount = Tables.size();
        size_t lastTableIndex = tablesCount + otherTablesCount - 1;

        std::move(other.Tables.begin(), other.Tables.end(), std::back_inserter(Tables));

        for (const auto& [reference, entry] : other.Lookup) {
            auto [it, emplaced] = Lookup.emplace(
                reference,
                TColumnEntry{
                    entry.Column,
                    0, // Consider not used yet.
                    tablesCount + entry.OriginTableIndex});

            if (!emplaced) {
                // Column is shared. Increment LastTableIndex to prevent search in new (other merged) tables.
                it->second.LastTableIndex = lastTableIndex;
            }
        }
    }

    void PopulateAllColumns()
    {
        for (const auto& table : Tables) {
            for (const auto& column : table.Schema.Columns()) {
                GetColumnPtr(NAst::TReference(column.Name(), table.Alias));
            }
        }
    }

    void SetGroupData(const TNamedItemList* groupItems, TAggregateItemList* aggregateItems)
    {
        YT_VERIFY(!GroupItems && !AggregateItems);

        GroupItems = groupItems;
        AggregateItems = aggregateItems;
        AfterGroupBy = true;
    }

    void CheckNoOtherColumn(const NAst::TReference& reference, size_t startTableIndex) const
    {
        for (int index = startTableIndex; index < std::ssize(Tables); ++index) {
            auto& [schema, alias, mapping] = Tables[index];

            if (alias == reference.TableName && schema.FindColumn(reference.ColumnName)) {
                THROW_ERROR_EXCEPTION("Ambiguous resolution for column %Qv",
                    NAst::InferColumnName(reference));
            }
        }
    }

    std::pair<const TTable*, TLogicalTypePtr> ResolveColumn(const NAst::TReference& reference) const
    {
        const TTable* result = nullptr;
        TLogicalTypePtr type;

        int index = 0;
        for (; index < std::ssize(Tables); ++index) {
            auto& [schema, alias, mapping] = Tables[index];

            if (alias != reference.TableName) {
                continue;
            }

            if (auto* column = schema.FindColumn(reference.ColumnName)) {
                auto formattedName = NAst::InferColumnName(reference);

                if (mapping) {
                    mapping->push_back(TColumnDescriptor{
                        formattedName,
                        schema.GetColumnIndex(*column)
                    });
                }
                result = &Tables[index];
                type = column->LogicalType();
                ++index;
                break;
            }
        }

        CheckNoOtherColumn(reference, index);

        return {result, type};
    }

    static const std::optional<TBaseColumn> FindColumn(const TNamedItemList& schema, const TString& name)
    {
        for (int index = 0; index < std::ssize(schema); ++index) {
            if (schema[index].Name == name) {
                return TBaseColumn(name, schema[index].Expression->LogicalType);
            }
        }
        return std::nullopt;
    }

    std::optional<TBaseColumn> GetColumnPtr(const NAst::TReference& reference)
    {
        if (AfterGroupBy) {
            // Search other way after group by.
            if (reference.TableName) {
                return std::nullopt;
            }

            return FindColumn(*GroupItems, reference.ColumnName);
        }

        size_t lastTableIndex = Tables.size() - 1;

        auto found = Lookup.find(reference);
        if (found != Lookup.end()) {
            // Provide column from max table index till end.

            size_t nextTableIndex = std::max(found->second.OriginTableIndex, found->second.LastTableIndex) + 1;

            CheckNoOtherColumn(reference, nextTableIndex);

            // Update LastTableIndex after check.
            found->second.LastTableIndex = lastTableIndex;

            return found->second.Column;
        } else if (auto [table, type] = ResolveColumn(reference); table) {
            auto formattedName = NAst::InferColumnName(reference);
            auto column = TBaseColumn(formattedName, type);

            auto emplaced = Lookup.emplace(
                reference,
                TColumnEntry{
                    column,
                    lastTableIndex,
                    size_t(table - Tables.data())});

            YT_VERIFY(emplaced.second);
            return column;
        } else {
            return std::nullopt;
        }
    }
};

using TExpressionGenerator = std::function<TConstExpressionPtr(EValueType)>;

struct TUntypedExpression
{
    TTypeSet FeasibleTypes;
    TExpressionGenerator Generator;
    bool IsConstant;
};

struct TBuilderCtx
    : public TBuilderCtxBase
{
public:
    const TString& Source;
    const TConstTypeInferrerMapPtr Functions;
    const NAst::TAliasMap& AliasMap;

private:
    std::set<TString> UsedAliases;
    size_t Depth = 0;

    THashMap<std::pair<TString, EValueType>, TConstAggregateFunctionExpressionPtr> AggregateLookup;

public:
    TBuilderCtx(
        const TString& source,
        const TConstTypeInferrerMapPtr& functions,
        const NAst::TAliasMap& aliasMap,
        const TTableSchema& schema,
        std::optional<TString> alias,
        std::vector<TColumnDescriptor>* mapping)
        : TBuilderCtxBase(schema, alias, mapping)
        , Source(source)
        , Functions(functions)
        , AliasMap(aliasMap)
    { }

    // TODO: Move ProvideAggregateColumn and GetAggregateColumnPtr to TBuilderCtxBase and provide callback
    //  OnExpression.
    // Or split into two functions. GetAggregate and SetAggregate.
    std::pair<TTypeSet, std::function<TConstExpressionPtr(EValueType)>> ProvideAggregateColumn(
        const TString& name,
        const TAggregateTypeInferrer* aggregateItem,
        const NAst::TExpression* argument,
        const TString& subexpressionName)
    {
        YT_VERIFY(AfterGroupBy);

        // TODO: Use guard.
        AfterGroupBy = false;
        auto untypedOperand = OnExpression(argument);
        AfterGroupBy = true;

        TTypeSet constraint;
        std::optional<EValueType> stateType;
        std::optional<EValueType> resultType;

        aggregateItem->GetNormalizedConstraints(&constraint, &stateType, &resultType, name);

        TTypeSet resultTypes;
        TTypeSet genericAssignments = constraint;

        if (!Unify(&genericAssignments, untypedOperand.FeasibleTypes)) {
            THROW_ERROR_EXCEPTION("Type mismatch in function %Qv: expected %v, actual %v",
                name,
                genericAssignments,
                untypedOperand.FeasibleTypes)
                << TErrorAttribute("source", subexpressionName);
        }

        if (resultType) {
            resultTypes = TTypeSet({*resultType});
        } else {
            resultTypes = genericAssignments;
        }

        return std::pair(resultTypes, [=, this] (EValueType type) {
            EValueType argType;
            if (resultType) {
                YT_VERIFY(!genericAssignments.IsEmpty());
                argType = GetFrontWithCheck(genericAssignments, argument->GetSource(Source));
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

            typedOperand = TCastEliminator().Visit(typedOperand);
            typedOperand = TExpressionSimplifier().Visit(typedOperand);
            typedOperand = TNotExpressionPropagator().Visit(typedOperand);

            AggregateItems->emplace_back(
                std::vector<TConstExpressionPtr>{typedOperand},
                name,
                subexpressionName,
                effectiveStateType,
                type);

            return typedOperand;
        });
    }

    TUntypedExpression GetAggregateColumnPtr(
        const TString& functionName,
        const TAggregateTypeInferrer* aggregateItem,
        const NAst::TExpression* argument,
        const TString& subexpressionName)
    {
        if (!AfterGroupBy) {
            THROW_ERROR_EXCEPTION("Misuse of aggregate function %Qv", functionName);
        }

        auto typer = ProvideAggregateColumn(
            functionName,
            aggregateItem,
            argument,
            subexpressionName);

        TExpressionGenerator generator = [=, this] (EValueType type) {
            auto key = std::pair(subexpressionName, type);
            auto found = AggregateLookup.find(key);
            if (found != AggregateLookup.end()) {
                return found->second;
            } else {
                auto argExpression = typer.second(type);
                TConstAggregateFunctionExpressionPtr expr = New<TAggregateFunctionExpression>(
                    MakeLogicalType(GetLogicalType(type), false),
                    subexpressionName,
                    std::vector{argExpression},
                    type,
                    type,
                    functionName);
                YT_VERIFY(AggregateLookup.emplace(key, expr).second);
                return expr;
            }
        };

        return TUntypedExpression{typer.first, std::move(generator), false};
    }


    TUntypedExpression OnExpression(
        const NAst::TExpression* expr);

private:
    struct ResolveNestedTypesResult
    {
        TCompositeMemberAccessorPath NestedStructOrTupleItemAccessor;
        TLogicalTypePtr IntermediateType;
        TLogicalTypePtr ResultType;
    };

    ResolveNestedTypesResult ResolveNestedTypes(
        const TLogicalTypePtr& type,
        const NAst::TReference& reference);

    TConstExpressionPtr UnwrapListOrDictItemAccessor(
        const NAst::TReference& reference,
        ELogicalMetatype metaType);

    TUntypedExpression UnwrapCompositeMemberAccessor(
        const NAst::TReference& reference,
        TBaseColumn column);

    TUntypedExpression OnReference(
        const NAst::TReference& reference);

    TUntypedExpression OnFunction(
        const NAst::TFunctionExpression* functionExpr);

    TUntypedExpression OnUnaryOp(
        const NAst::TUnaryOpExpression* unaryExpr);

    TUntypedExpression MakeBinaryExpr(
        const NAst::TBinaryOpExpression* binaryExpr,
        EBinaryOp op,
        TUntypedExpression lhs,
        TUntypedExpression rhs,
        std::optional<size_t> offset);

    friend struct TBinaryOpGenerator;

    TUntypedExpression OnBinaryOp(
        const NAst::TBinaryOpExpression* binaryExpr);

    void InferArgumentTypes(
        std::vector<TConstExpressionPtr>* typedArguments,
        std::vector<EValueType>* argTypes,
        const NAst::TExpressionList& expressions,
        TStringBuf operatorName,
        TStringBuf source);

    TUntypedExpression OnInOp(
        const NAst::TInExpression* inExpr);

    TUntypedExpression OnBetweenOp(
        const NAst::TBetweenExpression* betweenExpr);

    TUntypedExpression OnTransformOp(
        const NAst::TTransformExpression* transformExpr);

    TUntypedExpression OnCaseOp(
        const NAst::TCaseExpression* caseExpr);

    TUntypedExpression OnLikeOp(
        const NAst::TLikeExpression* likeExpr);

public:
    TConstExpressionPtr BuildTypedExpression(
        const NAst::TExpression* expr,
        TTypeSet feasibleTypes = TTypeSet({
            EValueType::Null,
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Double,
            EValueType::Boolean,
            EValueType::String,
            EValueType::Any,
            EValueType::Composite}))
    {
        auto expressionTyper = OnExpression(expr);
        YT_VERIFY(!expressionTyper.FeasibleTypes.IsEmpty());

        if (!Unify(&feasibleTypes, expressionTyper.FeasibleTypes)) {
            THROW_ERROR_EXCEPTION("Type mismatch in expression: expected %Qv, got %Qv",
                feasibleTypes,
                expressionTyper.FeasibleTypes)
                << TErrorAttribute("source", expr->GetSource(Source));
        }

        auto result = expressionTyper.Generator(
            GetFrontWithCheck(feasibleTypes, expr->GetSource(Source)));

        result = TCastEliminator().Visit(result);
        result = TExpressionSimplifier().Visit(result);
        result = TNotExpressionPropagator().Visit(result);
        return result;
    }

};

TUntypedExpression TBuilderCtx::OnExpression(
    const NAst::TExpression* expr)
{
    CheckStackDepth();

    ++Depth;
    auto depthGuard = Finally([&] {
        --Depth;
    });

    if (Depth > MaxExpressionDepth) {
        THROW_ERROR_EXCEPTION("Maximum expression depth exceeded")
            << TErrorAttribute("max_expression_depth", MaxExpressionDepth);
    }

    if (auto literalExpr = expr->As<NAst::TLiteralExpression>()) {
        const auto& literalValue = literalExpr->Value;

        auto resultTypes = GetTypes(literalValue);
        TExpressionGenerator generator = [literalValue] (EValueType type) {
            return New<TLiteralExpression>(
                type,
                CastValueWithCheck(GetValue(literalValue), type));
        };
        return TUntypedExpression{resultTypes, std::move(generator), true};
    } else if (auto aliasExpr = expr->As<NAst::TAliasExpression>()) {
        return OnReference(NAst::TReference(aliasExpr->Name));
    } else if (auto referenceExpr = expr->As<NAst::TReferenceExpression>()) {
        return OnReference(referenceExpr->Reference);
    } else if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
        return OnFunction(functionExpr);
    } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
        return OnUnaryOp(unaryExpr);
    } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
        return OnBinaryOp(binaryExpr);
    } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
        return OnInOp(inExpr);
    } else if (auto betweenExpr = expr->As<NAst::TBetweenExpression>()) {
        return OnBetweenOp(betweenExpr);
    } else if (auto transformExpr = expr->As<NAst::TTransformExpression>()) {
        return OnTransformOp(transformExpr);
    } else if (auto caseExpr = expr->As<NAst::TCaseExpression>()) {
        return OnCaseOp(caseExpr);
    } else if (auto likeExpr = expr->As<NAst::TLikeExpression>()) {
        return OnLikeOp(likeExpr);
    }

    YT_ABORT();
}

TBuilderCtx::ResolveNestedTypesResult TBuilderCtx::ResolveNestedTypes(
    const TLogicalTypePtr& type,
    const NAst::TReference& reference)
{
    TCompositeMemberAccessorPath nestedStructOrTupleItemAccessor;
    nestedStructOrTupleItemAccessor.Reserve(std::ssize(reference.CompositeTypeAccessor.NestedStructOrTupleItemAccessor));

    TLogicalTypePtr current = type;

    for (const auto& item : reference.CompositeTypeAccessor.NestedStructOrTupleItemAccessor) {
        Visit(item,
            [&] (const NAst::TStructMemberAccessor& structMember) {
                if (current->GetMetatype() != ELogicalMetatype::Struct) {
                    THROW_ERROR_EXCEPTION("Member %Qv is not found", structMember)
                        << TErrorAttribute("source", NAst::FormatReference(reference));
                }

                const auto& fields = current->AsStructTypeRef().GetFields();
                for (int index = 0; index < std::ssize(fields); ++index) {
                    if (fields[index].Name == structMember) {
                        current = fields[index].Type;
                        nestedStructOrTupleItemAccessor.AppendStructMember(structMember, index);
                        return;
                    }
                }

                THROW_ERROR_EXCEPTION("Member %Qv is not found", structMember)
                    << TErrorAttribute("source", NAst::FormatReference(reference));
            },
            [&] (const NAst::TTupleItemIndexAccessor& itemIndex) {
                if (current->GetMetatype() != ELogicalMetatype::Tuple) {
                    THROW_ERROR_EXCEPTION("Member %Qv is not found", itemIndex)
                        << TErrorAttribute("source", NAst::FormatReference(reference));
                }

                const auto& tupleElements = current->AsTupleTypeRef().GetElements();

                if (itemIndex < 0 || itemIndex >= std::ssize(tupleElements)) {
                    THROW_ERROR_EXCEPTION("Member %Qv is not found", itemIndex)
                        << TErrorAttribute("source", NAst::FormatReference(reference));
                }

                current = tupleElements[itemIndex];
                nestedStructOrTupleItemAccessor.AppendTupleItem(itemIndex);
            });
    }

    auto intermediateType = current;
    auto resultType = current;

    if (reference.CompositeTypeAccessor.DictOrListItemAccessor) {
        if (current->GetMetatype() == ELogicalMetatype::List) {
            resultType = current->GetElement();
        } else if (current->GetMetatype() == ELogicalMetatype::Dict) {
            auto keyType = GetWireType(current->AsDictTypeRef().GetKey());
            if (keyType != EValueType::String) {
                THROW_ERROR_EXCEPTION("Expected string key type, but got %Qlv",
                    keyType)
                    << TErrorAttribute("source", NAst::FormatReference(reference));
            }
            resultType = current->AsDictTypeRef().GetValue();
        } else {
            THROW_ERROR_EXCEPTION("Incorrect nested item accessor")
                << TErrorAttribute("source", NAst::FormatReference(reference));
        }
    }

    return {std::move(nestedStructOrTupleItemAccessor), std::move(intermediateType), std::move(resultType)};
}

TConstExpressionPtr TBuilderCtx::UnwrapListOrDictItemAccessor(
    const NAst::TReference& reference,
    ELogicalMetatype metaType)
{
    if (!reference.CompositeTypeAccessor.DictOrListItemAccessor.has_value()) {
        return {};
    }

    auto itemIndex = *reference.CompositeTypeAccessor.DictOrListItemAccessor;

    if (std::ssize(itemIndex) != 1) {
        THROW_ERROR_EXCEPTION("Expression inside of the list or dict item accessor should be scalar")
            << TErrorAttribute("source", NAst::FormatReference(reference));
    }

    auto resultTypes = TTypeSet{};
    if (metaType == ELogicalMetatype::List) {
        resultTypes = TTypeSet{EValueType::Int64};
    } else if (metaType == ELogicalMetatype::Dict) {
        resultTypes = TTypeSet{EValueType::String};
    } else {
        YT_ABORT();
    }

    auto untypedExpression = OnExpression(itemIndex.front());
    if (!Unify(&resultTypes, untypedExpression.FeasibleTypes)) {
        THROW_ERROR_EXCEPTION("Incorrect type inside of the list or dict item accessor")
            << TErrorAttribute("source", NAst::FormatReference(reference))
            << TErrorAttribute("actual_type", ToString(untypedExpression.FeasibleTypes))
            << TErrorAttribute("expected_type", ToString(resultTypes));
    }

    if (metaType == ELogicalMetatype::List) {
        return untypedExpression.Generator(EValueType::Int64);
    } else if (metaType == ELogicalMetatype::Dict) {
        return untypedExpression.Generator(EValueType::String);
    } else {
        YT_ABORT();
    }
}

TUntypedExpression TBuilderCtx::UnwrapCompositeMemberAccessor(
    const NAst::TReference& reference,
    TBaseColumn column)
{
    auto columnType = column.LogicalType;
    auto columnReference = New<TReferenceExpression>(columnType, column.Name);

    if (reference.CompositeTypeAccessor.IsEmpty()) {
        auto generator = [columnReference] (EValueType /*type*/) {
            return columnReference;
        };

        return {TTypeSet({GetWireType(columnType)}), std::move(generator), false};
    }

    auto resolved = ResolveNestedTypes(columnType, reference);
    auto listOrDictItemAccessor = UnwrapListOrDictItemAccessor(reference, resolved.IntermediateType->GetMetatype());

    auto memberAccessor = New<TCompositeMemberAccessorExpression>(
        resolved.ResultType,
        columnReference,
        std::move(resolved.NestedStructOrTupleItemAccessor),
        listOrDictItemAccessor);

    auto generator = [memberAccessor] (EValueType /*type*/) {
        return memberAccessor;
    };

    return {TTypeSet({GetWireType(resolved.ResultType)}), std::move(generator), /*IsConstant*/ false};
}

TUntypedExpression TBuilderCtx::OnReference(const NAst::TReference& reference)
{
    if (AfterGroupBy) {
        if (auto column = GetColumnPtr(reference)) {
            return UnwrapCompositeMemberAccessor(reference, *column);
        }
    }

    if (!reference.TableName) {
        const auto& columnName = reference.ColumnName;
        auto found = AliasMap.find(columnName);

        if (found != AliasMap.end()) {
            // try InferName(found, expand aliases = true)

            if (UsedAliases.insert(columnName).second) {
                auto aliasExpr = OnExpression(found->second);
                UsedAliases.erase(columnName);
                return aliasExpr;
            }
        }
    }

    if (!AfterGroupBy) {
        if (auto column = GetColumnPtr(reference)) {
            return UnwrapCompositeMemberAccessor(reference, *column);
        }
    }

    THROW_ERROR_EXCEPTION("Undefined reference %Qv",
        NAst::InferColumnName(reference));
}

TUntypedExpression TBuilderCtx::OnFunction(const NAst::TFunctionExpression* functionExpr)
{
    auto functionName = functionExpr->FunctionName;
    functionName.to_lower();

    const auto& descriptor = Functions->GetFunction(functionName);

    if (const auto* aggregateFunction = descriptor->As<TAggregateFunctionTypeInferrer>()) {
        auto subexpressionName = InferColumnName(*functionExpr);

        std::vector<TTypeSet> argTypes;
        argTypes.reserve(functionExpr->Arguments.size());
        std::vector<TTypeSet> genericAssignments;
        std::vector<TExpressionGenerator> operandTypers;
        operandTypers.reserve(functionExpr->Arguments.size());
        std::vector<int> formalArguments;

        YT_VERIFY(AfterGroupBy);

        AfterGroupBy = false;
        for (const auto& argument : functionExpr->Arguments) {
            auto untypedArgument = OnExpression(argument);
            argTypes.push_back(untypedArgument.FeasibleTypes);
            operandTypers.push_back(untypedArgument.Generator);
        }
        AfterGroupBy = true;

        int stateConstraintIndex;
        int resultConstraintIndex;

        std::tie(stateConstraintIndex, resultConstraintIndex) = aggregateFunction->GetNormalizedConstraints(
            &genericAssignments,
            &formalArguments);
        IntersectGenericsWithArgumentTypes(
            argTypes,
            &genericAssignments,
            formalArguments,
            functionName,
            functionExpr->GetSource(Source));

        auto resultTypes = genericAssignments[resultConstraintIndex];

        TExpressionGenerator generator = [
            this,
            stateConstraintIndex,
            resultConstraintIndex,
            functionName = std::move(functionName),
            subexpressionName = std::move(subexpressionName),
            operandTypers = std::move(operandTypers),
            genericAssignments = std::move(genericAssignments),
            formalArguments = std::move(formalArguments),
            source = functionExpr->GetSource(Source)
        ] (EValueType type) mutable {
            auto key = std::pair(subexpressionName, type);
            auto foundCached = AggregateLookup.find(key);
            if (foundCached != AggregateLookup.end()) {
                return foundCached->second;
            }

            EValueType stateType;
            auto effectiveTypes = RefineFunctionTypes(
                resultConstraintIndex,
                stateConstraintIndex,
                formalArguments,
                type,
                &stateType,
                &genericAssignments,
                source);

            std::vector<TConstExpressionPtr> typedOperands;
            for (int index = 0; index < std::ssize(effectiveTypes); ++index) {
                typedOperands.push_back(operandTypers[index](effectiveTypes[index]));
                typedOperands.back() = TCastEliminator().Visit(typedOperands.back());
                typedOperands.back() = TExpressionSimplifier().Visit(typedOperands.back());
                typedOperands.back() = TNotExpressionPropagator().Visit(typedOperands.back());
            }

            AggregateItems->emplace_back(
                typedOperands,
                functionName,
                subexpressionName,
                stateType,
                type);

            TConstAggregateFunctionExpressionPtr expr = New<TAggregateFunctionExpression>(
                MakeLogicalType(GetLogicalType(type), false),
                subexpressionName,
                typedOperands,
                stateType,
                type,
                functionName);
            AggregateLookup.emplace(key, expr);

            return expr;
        };

        return TUntypedExpression{resultTypes, std::move(generator), false};
    } else if (const auto* aggregateItem = descriptor->As<TAggregateTypeInferrer>()) {
        auto subexpressionName = InferColumnName(*functionExpr);

        try {
            if (functionExpr->Arguments.size() != 1) {
                THROW_ERROR_EXCEPTION("Aggregate function %Qv must have exactly one argument", functionName);
            }

            auto aggregateColumn = GetAggregateColumnPtr(
                functionName,
                aggregateItem,
                functionExpr->Arguments.front(),
                subexpressionName);

            return aggregateColumn;
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error creating aggregate")
                << TErrorAttribute("source", functionExpr->GetSource(Source))
                << ex;
        }
    } else if (const auto* regularFunction = descriptor->As<TFunctionTypeInferrer>()) {
        std::vector<TTypeSet> argTypes;
        std::vector<TExpressionGenerator> operandTypers;
        argTypes.reserve(functionExpr->Arguments.size());
        operandTypers.reserve(functionExpr->Arguments.size());
        for (const auto& argument : functionExpr->Arguments) {
            auto untypedArgument = OnExpression(argument);
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
            genericAssignments,
            source = functionExpr->GetSource(Source)
        ] (EValueType type) mutable {
            auto effectiveTypes = RefineFunctionTypes(
                regularFunction,
                type,
                operandTypers.size(),
                &genericAssignments,
                source);

            std::vector<TConstExpressionPtr> typedOperands;
            typedOperands.reserve(std::ssize(effectiveTypes));
            for (int index = 0; index < std::ssize(effectiveTypes); ++index) {
                typedOperands.push_back(operandTypers[index](effectiveTypes[index]));
            }

            return New<TFunctionExpression>(type, functionName, std::move(typedOperands));
        };

        return TUntypedExpression{resultTypes, std::move(generator), false};
    } else {
        YT_ABORT();
    }
}

TUntypedExpression TBuilderCtx::OnUnaryOp(const NAst::TUnaryOpExpression* unaryExpr)
{
    if (unaryExpr->Operand.size() != 1) {
        THROW_ERROR_EXCEPTION(
            "Unary operator %Qv must have exactly one argument",
            unaryExpr->Opcode);
    }

    auto untypedOperand = OnExpression(unaryExpr->Operand.front());

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
        genericAssignments,
        opSource = unaryExpr->Operand.front()->GetSource(Source)
    ] (EValueType type) mutable {
        auto argType = RefineUnaryExprTypes(
            op,
            type,
            &genericAssignments,
            opSource);
        return New<TUnaryOpExpression>(type, op, untypedOperand.Generator(argType));
    };
    return TUntypedExpression{resultTypes, std::move(generator), false};
}

TUntypedExpression TBuilderCtx::MakeBinaryExpr(
    const NAst::TBinaryOpExpression* binaryExpr,
    EBinaryOp op,
    TUntypedExpression lhs,
    TUntypedExpression rhs,
    std::optional<size_t> offset)
{
    TTypeSet genericAssignments;

    auto lhsSource = offset ? binaryExpr->Lhs[*offset]->GetSource(Source) : "";
    auto rhsSource = offset ? binaryExpr->Rhs[*offset]->GetSource(Source) : "";

    auto resultTypes = InferBinaryExprTypes(
        op,
        lhs.FeasibleTypes,
        rhs.FeasibleTypes,
        &genericAssignments,
        lhsSource,
        rhsSource);

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

    TExpressionGenerator generator = [
        op,
        lhs,
        rhs,
        genericAssignments,
        lhsSource,
        rhsSource,
        source = binaryExpr->GetSource(Source)
    ] (EValueType type) mutable {
        auto argTypes = RefineBinaryExprTypes(
            op,
            type,
            lhs.FeasibleTypes,
            rhs.FeasibleTypes,
            &genericAssignments,
            lhsSource,
            rhsSource,
            source);

        return New<TBinaryOpExpression>(
            type,
            op,
            lhs.Generator(argTypes.first),
            rhs.Generator(argTypes.second));
    };
    return TUntypedExpression{resultTypes, std::move(generator), false};
}

struct TBinaryOpGenerator
{
    TBuilderCtx& Builder;
    const NAst::TBinaryOpExpression* BinaryExpr;

    TUntypedExpression Do(size_t keySize, EBinaryOp op)
    {
        YT_VERIFY(keySize > 0);
        size_t offset = keySize - 1;

        auto untypedLhs = Builder.OnExpression(BinaryExpr->Lhs[offset]);
        auto untypedRhs = Builder.OnExpression(BinaryExpr->Rhs[offset]);

        auto result = Builder.MakeBinaryExpr(BinaryExpr, op, std::move(untypedLhs), std::move(untypedRhs), offset);

        while (offset > 0) {
            --offset;
            auto untypedLhs = Builder.OnExpression(BinaryExpr->Lhs[offset]);
            auto untypedRhs = Builder.OnExpression(BinaryExpr->Rhs[offset]);

            auto eq = Builder.MakeBinaryExpr(
                BinaryExpr,
                op == EBinaryOp::NotEqual ? EBinaryOp::Or : EBinaryOp::And,
                Builder.MakeBinaryExpr(
                    BinaryExpr,
                    op == EBinaryOp::NotEqual ? EBinaryOp::NotEqual : EBinaryOp::Equal,
                    untypedLhs,
                    untypedRhs,
                    offset),
                std::move(result),
                std::nullopt);

            if (op == EBinaryOp::Equal || op == EBinaryOp::NotEqual) {
                result = eq;
                continue;
            }

            EBinaryOp strongOp = op;
            if (op == EBinaryOp::LessOrEqual) {
                strongOp = EBinaryOp::Less;
            } else if (op == EBinaryOp::GreaterOrEqual)  {
                strongOp = EBinaryOp::Greater;
            }

            result = Builder.MakeBinaryExpr(
                BinaryExpr,
                EBinaryOp::Or,
                Builder.MakeBinaryExpr(
                    BinaryExpr,
                    strongOp,
                    std::move(untypedLhs),
                    std::move(untypedRhs),
                    offset),
                std::move(eq),
                std::nullopt);
        }

        return result;
    }
};

TUntypedExpression TBuilderCtx::OnBinaryOp(
    const NAst::TBinaryOpExpression* binaryExpr)
{
    if (IsRelationalBinaryOp(binaryExpr->Opcode)) {
        if (binaryExpr->Lhs.size() != binaryExpr->Rhs.size()) {
            THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
                binaryExpr->Lhs.size(),
                binaryExpr->Rhs.size())
                << TErrorAttribute("source", binaryExpr->GetSource(Source));
        }

        int keySize = binaryExpr->Lhs.size();
        return TBinaryOpGenerator{*this, binaryExpr}.Do(keySize, binaryExpr->Opcode);
    } else {
        if (binaryExpr->Lhs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", FormatExpression(binaryExpr->Lhs));
        }

        if (binaryExpr->Rhs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", FormatExpression(binaryExpr->Rhs));
        }

        auto untypedLhs = OnExpression(binaryExpr->Lhs.front());
        auto untypedRhs = OnExpression(binaryExpr->Rhs.front());

        return MakeBinaryExpr(binaryExpr, binaryExpr->Opcode, std::move(untypedLhs), std::move(untypedRhs), 0);
    }
}

void TBuilderCtx::InferArgumentTypes(
    std::vector<TConstExpressionPtr>* typedArguments,
    std::vector<EValueType>* argTypes,
    const NAst::TExpressionList& expressions,
    TStringBuf operatorName,
    TStringBuf source)
{
    std::unordered_set<TString> columnNames;

    for (const auto& argument : expressions) {
        auto untypedArgument = OnExpression(argument);

        EValueType argType = GetFrontWithCheck(untypedArgument.FeasibleTypes, argument->GetSource(Source));
        auto typedArgument = untypedArgument.Generator(argType);

        typedArguments->push_back(typedArgument);
        argTypes->push_back(argType);
        if (auto reference = typedArgument->As<TReferenceExpression>()) {
            if (!columnNames.insert(reference->ColumnName).second) {
                THROW_ERROR_EXCEPTION("%v operator has multiple references to column %Qv",
                    operatorName,
                    reference->ColumnName)
                    << TErrorAttribute("source", source);
            }
        }
    }
}

TUntypedExpression TBuilderCtx::OnInOp(
    const NAst::TInExpression* inExpr)
{
    std::vector<TConstExpressionPtr> typedArguments;
    std::vector<EValueType> argTypes;

    auto source = inExpr->GetSource(Source);

    InferArgumentTypes(
        &typedArguments,
        &argTypes,
        inExpr->Expr,
        "IN",
        inExpr->GetSource(Source));

    auto capturedRows = LiteralTupleListToRows(inExpr->Values, argTypes, source);
    auto result = New<TInExpression>(std::move(typedArguments), std::move(capturedRows));

    TTypeSet resultTypes({EValueType::Boolean});
    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };
    return TUntypedExpression{resultTypes, std::move(generator), false};
}

TUntypedExpression TBuilderCtx::OnBetweenOp(
    const NAst::TBetweenExpression* betweenExpr)
{
    std::vector<TConstExpressionPtr> typedArguments;
    std::vector<EValueType> argTypes;

    auto source = betweenExpr->GetSource(Source);

    InferArgumentTypes(
        &typedArguments,
        &argTypes,
        betweenExpr->Expr,
        "BETWEEN",
        source);

    auto capturedRows = LiteralRangesListToRows(betweenExpr->Values, argTypes, source);
    auto result = New<TBetweenExpression>(std::move(typedArguments), std::move(capturedRows));

    TTypeSet resultTypes({EValueType::Boolean});
    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };
    return TUntypedExpression{resultTypes, std::move(generator), false};
}

TUntypedExpression TBuilderCtx::OnTransformOp(
    const NAst::TTransformExpression* transformExpr)
{
    std::vector<TConstExpressionPtr> typedArguments;
    std::vector<EValueType> argTypes;

    auto source = transformExpr->GetSource(Source);

    InferArgumentTypes(
        &typedArguments,
        &argTypes,
        transformExpr->Expr,
        "TRANSFORM",
        source);

    if (transformExpr->From.size() != transformExpr->To.size()) {
        THROW_ERROR_EXCEPTION("Size mismatch for source and result arrays in TRANSFORM operator")
            << TErrorAttribute("source", source);
    }

    TTypeSet resultTypes({
        EValueType::Null,
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
        EValueType::Boolean,
        EValueType::String,
        EValueType::Any});

    for (const auto& tuple : transformExpr->To) {
        if (tuple.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", source);
        }

        auto valueTypes = GetTypes(tuple.front());

        if (!Unify(&resultTypes, valueTypes)) {
            THROW_ERROR_EXCEPTION("Types mismatch in tuple")
                << TErrorAttribute("source", source)
                << TErrorAttribute("actual_type", ToString(valueTypes))
                << TErrorAttribute("expected_type", ToString(resultTypes));
        }
    }

    const auto& defaultExpr = transformExpr->DefaultExpr;

    TConstExpressionPtr defaultTypedExpr;

    EValueType resultType;
    if (defaultExpr) {
        if (defaultExpr->size() != 1) {
            THROW_ERROR_EXCEPTION("Default expression must scalar")
                << TErrorAttribute("source", source);
        }

        auto untypedArgument = OnExpression(defaultExpr->front());

        if (!Unify(&resultTypes, untypedArgument.FeasibleTypes)) {
            THROW_ERROR_EXCEPTION("Type mismatch in default expression: expected %Qlv, got %Qlv",
                resultTypes,
                untypedArgument.FeasibleTypes)
                << TErrorAttribute("source", source);
        }

        resultType = GetFrontWithCheck(resultTypes, source);

        defaultTypedExpr = untypedArgument.Generator(resultType);
    } else {
        resultType = GetFrontWithCheck(resultTypes, source);
    }

    auto rowBuffer = New<TRowBuffer>(TQueryPreparerBufferTag());
    TUnversionedRowBuilder rowBuilder;
    std::vector<TRow> rows;
    rows.reserve(std::ssize(transformExpr->From));

    for (int index = 0; index < std::ssize(transformExpr->From); ++index) {
        const auto& sourceTuple = transformExpr->From[index];
        if (sourceTuple.size() != argTypes.size()) {
            THROW_ERROR_EXCEPTION("Arguments size mismatch in tuple")
                << TErrorAttribute("source", source);
        }
        for (int i = 0; i < std::ssize(sourceTuple); ++i) {
            auto valueType = GetType(sourceTuple[i]);
            auto value = GetValue(sourceTuple[i]);

            if (valueType == EValueType::Null) {
                value = MakeUnversionedSentinelValue(EValueType::Null);
            } else if (valueType != argTypes[i]) {
                if (IsArithmeticType(valueType) && IsArithmeticType(argTypes[i])) {
                    value = CastValueWithCheck(value, argTypes[i]);
                } else {
                    THROW_ERROR_EXCEPTION("Types mismatch in tuple")
                    << TErrorAttribute("source", source)
                    << TErrorAttribute("actual_type", valueType)
                    << TErrorAttribute("expected_type", argTypes[i]);
                }
            }
            rowBuilder.AddValue(value);
        }

        const auto& resultTuple = transformExpr->To[index];

        YT_VERIFY(resultTuple.size() == 1);
        auto value = CastValueWithCheck(GetValue(resultTuple.front()), resultType);
        rowBuilder.AddValue(value);

        rows.push_back(rowBuffer->CaptureRow(rowBuilder.GetRow()));
        rowBuilder.Reset();
    }

    std::sort(rows.begin(), rows.end(), [argCount = argTypes.size()] (TRow lhs, TRow rhs) {
        return CompareRows(lhs, rhs, argCount) < 0;
    });

    auto capturedRows = MakeSharedRange(std::move(rows), std::move(rowBuffer));
    auto result = New<TTransformExpression>(
        resultType,
        std::move(typedArguments),
        std::move(capturedRows),
        std::move(defaultTypedExpr));

    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };
    return TUntypedExpression{TTypeSet({resultType}), std::move(generator), false};
}

TUntypedExpression TBuilderCtx::OnCaseOp(const NAst::TCaseExpression* caseExpr)
{
    auto source = caseExpr->GetSource(Source);

    TUntypedExpression untypedOperand;
    TTypeSet operandTypes;
    bool hasOptionalOperand = false;
    {
        if (caseExpr->OptionalOperand) {
            if (caseExpr->OptionalOperand->size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE should be scalar")
                    << TErrorAttribute("source", source);
            }

            untypedOperand = OnExpression(caseExpr->OptionalOperand->front());
            operandTypes = untypedOperand.FeasibleTypes;
            hasOptionalOperand = true;
        }
    }

    std::vector<TUntypedExpression> untypedConditions;
    untypedConditions.reserve(caseExpr->WhenThenExpressions.size());
    EValueType conditionType{};
    std::optional<EValueType> operandType;
    {
        TTypeSet conditionTypes;
        if (hasOptionalOperand) {
            conditionTypes = operandTypes;
        } else {
            conditionTypes = TTypeSet({EValueType::Boolean});
        }

        for (auto& [condition, _] : caseExpr->WhenThenExpressions) {
            if (condition.size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE WHEN should be scalar")
                    << TErrorAttribute("source", source);
            }

            auto untypedExpression = OnExpression(condition.front());
            if (!Unify(&conditionTypes, untypedExpression.FeasibleTypes)) {
                if (hasOptionalOperand) {
                    THROW_ERROR_EXCEPTION("Types mismatch in CASE WHEN expression")
                        << TErrorAttribute("source", source)
                        << TErrorAttribute("actual_type", ToString(untypedExpression.FeasibleTypes))
                        << TErrorAttribute("expected_type", ToString(conditionTypes));
                } else {
                    THROW_ERROR_EXCEPTION("Expression inside CASE WHEN should be boolean")
                        << TErrorAttribute("source", source);
                }
            }

            untypedConditions.push_back(std::move(untypedExpression));
        }

        conditionType = GetFrontWithCheck(conditionTypes, source);

        if (hasOptionalOperand) {
            operandType = conditionType;
        }
    }

    std::vector<TUntypedExpression> untypedResults;
    untypedResults.reserve(caseExpr->WhenThenExpressions.size());
    TUntypedExpression untypedDefault{};
    EValueType resultType{};
    {
        TTypeSet resultTypes({
            EValueType::Null,
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Double,
            EValueType::Boolean,
            EValueType::String,
            EValueType::Any});

        for (auto& [_, result] : caseExpr->WhenThenExpressions) {
            if (result.size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE THEN should be scalar")
                    << TErrorAttribute("source", source);
            }

            auto untypedExpression = OnExpression(result.front());
            if (!Unify(&resultTypes, untypedExpression.FeasibleTypes)) {
                THROW_ERROR_EXCEPTION("Types mismatch in CASE THEN expression")
                    << TErrorAttribute("source", source)
                    << TErrorAttribute("actual_type", ToString(untypedExpression.FeasibleTypes))
                    << TErrorAttribute("expected_type", ToString(resultTypes));
            }

            untypedResults.push_back(std::move(untypedExpression));
        }

        if (caseExpr->DefaultExpression) {
            if (caseExpr->DefaultExpression->size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE ELSE should be scalar")
                    << TErrorAttribute("source", source);
            }

            untypedDefault = OnExpression(caseExpr->DefaultExpression->front());
            if (!Unify(&resultTypes, untypedDefault.FeasibleTypes)) {
                THROW_ERROR_EXCEPTION("Types mismatch in CASE ELSE expression")
                    << TErrorAttribute("source", source)
                    << TErrorAttribute("actual_type", ToString(untypedDefault.FeasibleTypes))
                    << TErrorAttribute("expected_type", ToString(resultTypes));
            }
        }

        resultType = GetFrontWithCheck(resultTypes, source);
    }

    TConstExpressionPtr typedOptionalOperand;
    std::vector<TWhenThenExpressionPtr> typedWhenThenExpressions;
    typedWhenThenExpressions.reserve(caseExpr->WhenThenExpressions.size());
    TConstExpressionPtr typedDefaultExpression;
    {
        if (hasOptionalOperand) {
            typedOptionalOperand = untypedOperand.Generator(*operandType);
        }

        for (size_t index = 0; index < untypedConditions.size(); ++index) {
            typedWhenThenExpressions.push_back(New<TWhenThenExpression>(
                untypedConditions[index].Generator(conditionType),
                untypedResults[index].Generator(resultType)));
        }

        if (caseExpr->DefaultExpression) {
            typedDefaultExpression = untypedDefault.Generator(resultType);
        }
    }

    auto result = New<TCaseExpression>(
        resultType,
        std::move(typedOptionalOperand),
        std::move(typedWhenThenExpressions),
        std::move(typedDefaultExpression));

    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };

    return TUntypedExpression{TTypeSet({resultType}), std::move(generator), /*IsConstant=*/ false};
}

TUntypedExpression TBuilderCtx::OnLikeOp(const NAst::TLikeExpression* likeExpr)
{
    auto source = likeExpr->GetSource(Source);

    if (likeExpr->Opcode == EStringMatchOp::Regex && likeExpr->EscapeCharacter) {
        THROW_ERROR_EXCEPTION("ESCAPE should not be used together with REGEXP (RLIKE)")
            << TErrorAttribute("source", source);
    }

    auto makeTypedStringExpression = [this] (
        const NAst::TExpressionList& expression,
        TStringBuf name,
        TStringBuf source)
    {
        auto stringTypes = TTypeSet({EValueType::String,});

        if (expression.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar %Qv expression",
                name)
                << TErrorAttribute("source", source);
        }

        auto untypedExpression = OnExpression(expression.front());
        if (!Unify(&stringTypes, untypedExpression.FeasibleTypes)) {
                THROW_ERROR_EXCEPTION("Types mismatch in %v", name)
                    << TErrorAttribute("source", source)
                    << TErrorAttribute("actual_type", ToString(untypedExpression.FeasibleTypes))
                    << TErrorAttribute("expected_type", ToString(stringTypes));
        }

        return untypedExpression.Generator(EValueType::String);
    };

    auto typedText = makeTypedStringExpression(likeExpr->Text, "LIKE matched value", source);
    auto typedPattern = makeTypedStringExpression(likeExpr->Pattern, "LIKE pattern", source);

    TConstExpressionPtr typedEscapeCharacter;
    if (likeExpr->EscapeCharacter) {
        typedEscapeCharacter = makeTypedStringExpression(likeExpr->EscapeCharacter.value(), "escape character", source);
    }

    auto result = New<TLikeExpression>(
        std::move(typedText),
        likeExpr->Opcode,
        std::move(typedPattern),
        std::move(typedEscapeCharacter));

    TExpressionGenerator generator = [result] (EValueType /*type*/) {
        return result;
    };

    return TUntypedExpression{TTypeSet({EValueType::Boolean}), std::move(generator), /*IsConstant=*/ false};
}

////////////////////////////////////////////////////////////////////////////////

TConstExpressionPtr BuildPredicate(
    const NAst::TExpressionList& expressionAst,
    TBuilderCtx& builder,
    TStringBuf name)
{
    if (expressionAst.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            << TErrorAttribute("source", FormatExpression(expressionAst));
    }

    auto typedPredicate = builder.BuildTypedExpression(expressionAst.front());

    auto actualType = typedPredicate->GetWireType();
    EValueType expectedType(EValueType::Boolean);
    if (actualType != expectedType) {
        THROW_ERROR_EXCEPTION("%v is not a boolean expression", name)
            << TErrorAttribute("source", expressionAst.front()->GetSource(builder.Source))
            << TErrorAttribute("actual_type", actualType)
            << TErrorAttribute("expected_type", expectedType);
    }

    return typedPredicate;
}

TGroupClausePtr BuildGroupClause(
    const NAst::TExpressionList& expressionsAst,
    ETotalsMode totalsMode,
    TBuilderCtx& builder)
{
    auto groupClause = New<TGroupClause>();
    groupClause->TotalsMode = totalsMode;

    for (const auto& expressionAst : expressionsAst) {
        auto typedExpr = builder.BuildTypedExpression(expressionAst, ComparableTypes);

        groupClause->AddGroupItem(typedExpr, InferColumnName(*expressionAst));
    }

    builder.SetGroupData(
        &groupClause->GroupItems,
        &groupClause->AggregateItems);

    return groupClause;
}

TConstProjectClausePtr BuildProjectClause(
    const NAst::TExpressionList& expressionsAst,
    TBuilderCtx& builder)
{
    auto projectClause = New<TProjectClause>();
    for (const auto& expressionAst : expressionsAst) {
        auto typedExpr = builder.BuildTypedExpression(expressionAst);

        projectClause->AddProjection(typedExpr, InferColumnName(*expressionAst));
    }

    return projectClause;
}

void PrepareQuery(
    const TQueryPtr& query,
    const NAst::TQuery& ast,
    TBuilderCtx& builder)
{
    if (ast.WherePredicate) {
        auto wherePredicate = BuildPredicate(*ast.WherePredicate, builder, "WHERE-clause");
        query->WhereClause = IsTrue(wherePredicate) ? nullptr : wherePredicate;
    }

    if (ast.GroupExprs) {
        auto groupClause = BuildGroupClause(ast.GroupExprs->first, ast.GroupExprs->second, builder);

        auto keyColumns = query->GetKeyColumns();

        TNamedItemList groupItems = std::move(groupClause->GroupItems);

        std::vector<int> touchedKeyColumns(keyColumns.size(), -1);
        for (int index = 0; index < std::ssize(groupItems); ++index) {
            const auto& item = groupItems[index];
            if (auto referenceExpr = item.Expression->As<TReferenceExpression>()) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0) {
                    touchedKeyColumns[keyPartIndex] = index;
                }
            }
        }

        size_t keyPrefix = 0;
        for (; keyPrefix < touchedKeyColumns.size(); ++keyPrefix) {
            if (touchedKeyColumns[keyPrefix] >= 0) {
                continue;
            }

            const auto& expression = query->Schema.Original->Columns()[keyPrefix].Expression();

            if (!expression) {
                break;
            }

            // Call PrepareExpression to extract references only.
            THashSet<TString> references;
            PrepareExpression(*expression, *query->Schema.Original, builder.Functions, &references);

            auto canEvaluate = true;
            for (const auto& reference : references) {
                int referenceIndex = query->Schema.Original->GetColumnIndexOrThrow(reference);
                if (touchedKeyColumns[referenceIndex] < 0) {
                    canEvaluate = false;
                }
            }

            if (!canEvaluate) {
                break;
            }
        }

        touchedKeyColumns.resize(keyPrefix);
        for (int index : touchedKeyColumns) {
            if (index >= 0) {
                groupClause->GroupItems.push_back(std::move(groupItems[index]));
            }
        }

        groupClause->CommonPrefixWithPrimaryKey = groupClause->GroupItems.size();

        for (auto& item : groupItems) {
            if (item.Expression) {
                groupClause->GroupItems.push_back(std::move(item));
            }
        }

        query->GroupClause = groupClause;

        // not prefix, because of equal prefixes near borders
        bool containsPrimaryKey = keyPrefix == query->GetKeyColumns().size();
        // COMPAT(lukyan)
        query->UseDisjointGroupBy = containsPrimaryKey && !keyColumns.empty();
    }

    if (ast.HavingPredicate) {
        if (!query->GroupClause) {
            THROW_ERROR_EXCEPTION("Expected GROUP BY before HAVING");
        }
        query->HavingClause = BuildPredicate(
            *ast.HavingPredicate,
            builder,
            "HAVING-clause");
    }

    if (!ast.OrderExpressions.empty()) {
        auto orderClause = New<TOrderClause>();

        for (const auto& orderExpr : ast.OrderExpressions) {
            for (const auto& expressionAst : orderExpr.first) {
                auto typedExpr = builder.BuildTypedExpression(
                    expressionAst,
                    ComparableTypes);

                orderClause->OrderItems.push_back({typedExpr, orderExpr.second});
            }
        }

        ssize_t keyPrefix = 0;
        while (keyPrefix < std::ssize(orderClause->OrderItems)) {
            const auto& item = orderClause->OrderItems[keyPrefix];

            if (item.Descending) {
                break;
            }

            const auto* referenceExpr = item.Expression->As<TReferenceExpression>();

            if (!referenceExpr) {
                break;
            }

            auto columnIndex = ColumnNameToKeyPartIndex(query->GetKeyColumns(), referenceExpr->ColumnName);

            if (keyPrefix != columnIndex) {
                break;
            }
            ++keyPrefix;
        }

        if (keyPrefix < std::ssize(orderClause->OrderItems)) {
            query->OrderClause = std::move(orderClause);
        }

        // Use ordered scan otherwise
    }

    if (ast.SelectExprs) {
        query->ProjectClause = BuildProjectClause(
            *ast.SelectExprs,
            builder);
    } else {
        // Select all columns.
        builder.PopulateAllColumns();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TYsonToQueryExpressionConvertVisitor
    : public TYsonConsumerBase
{
public:
    explicit TYsonToQueryExpressionConvertVisitor(TStringBuilder* builder)
        : Builder_(builder)
    { }

    void OnStringScalar(TStringBuf value) override
    {
        Builder_->AppendChar('"');
        Builder_->AppendString(EscapeC(value));
        Builder_->AppendChar('"');
    }

    void OnInt64Scalar(i64 value) override
    {
        Builder_->AppendFormat("%v", value);
    }

    void OnUint64Scalar(ui64 value) override
    {
        Builder_->AppendFormat("%vu", value);
    }

    void OnDoubleScalar(double value) override
    {
        Builder_->AppendFormat("%lf", value);
    }

    void OnBooleanScalar(bool value) override
    {
        Builder_->AppendFormat("%lv", value);
    }

    void OnEntity() override
    {
        Builder_->AppendString("null");
    }

    void OnBeginList() override
    {
        Builder_->AppendChar('(');
        InListBeginning_ = true;
    }

    void OnListItem() override
    {
        if (!InListBeginning_) {
            Builder_->AppendString(", ");
        }
        InListBeginning_ = false;
    }

    void OnEndList() override
    {
        Builder_->AppendChar(')');
    }

    void OnBeginMap() override
    {
        THROW_ERROR_EXCEPTION("Maps inside YSON placeholder are not allowed");
    }

    void OnKeyedItem(TStringBuf) override
    {
        THROW_ERROR_EXCEPTION("Maps inside YSON placeholder are not allowed");
    }

    void OnEndMap() override
    {
        THROW_ERROR_EXCEPTION("Maps inside YSON placeholder are not allowed");
    }

    void OnBeginAttributes() override
    {
        THROW_ERROR_EXCEPTION("Attributes inside YSON placeholder are not allowed");
    }

    void OnEndAttributes() override
    {
        THROW_ERROR_EXCEPTION("Attributes inside YSON placeholder are not allowed");
    }

private:
    TStringBuilder* Builder_;
    bool InListBeginning_;
};

void YsonParseError(TStringBuf message, TYsonStringBuf source)
{
    THROW_ERROR_EXCEPTION("%v", message)
        << TErrorAttribute("context", Format("%v", source.AsStringBuf()));
}

THashMap<TString, TString> ConvertYsonPlaceholdersToQueryLiterals(TYsonStringBuf placeholders)
{
    TMemoryInput input{placeholders.AsStringBuf()};
    TYsonPullParser ysonParser{&input, EYsonType::Node};
    TYsonPullParserCursor ysonCursor{&ysonParser};

    if (ysonCursor->GetType() != EYsonItemType::BeginMap) {
        YsonParseError("Incorrect placeholder argument: YSON map expected", placeholders);
    }

    ysonCursor.Next();

    THashMap<TString, TString> queryLiterals;
    while (ysonCursor->GetType() != EYsonItemType::EndMap) {
        if (ysonCursor->GetType() != EYsonItemType::StringValue) {
            YsonParseError("Incorrect YSON map placeholder: keys should be strings", placeholders);
        }
        auto key = TString(ysonCursor->UncheckedAsString());

        ysonCursor.Next();
        switch (ysonCursor->GetType()) {
            case EYsonItemType::EntityValue:
            case EYsonItemType::BooleanValue:
            case EYsonItemType::Int64Value:
            case EYsonItemType::Uint64Value:
            case EYsonItemType::DoubleValue:
            case EYsonItemType::StringValue:
            case EYsonItemType::BeginList: {
                TStringBuilder valueBuilder;
                TYsonToQueryExpressionConvertVisitor ysonValueTransferrer{&valueBuilder};
                ysonCursor.TransferComplexValue(&ysonValueTransferrer);
                queryLiterals.emplace(std::move(key), valueBuilder.Flush());
                break;
            }
            default:
                YsonParseError("Incorrect placeholder map: values should be plain types or lists", placeholders);
        }
    }

    return queryLiterals;
}

void ParseQueryString(
    NAst::TAstHead* astHead,
    const TString& source,
    NAst::TParser::token::yytokentype strayToken,
    TYsonStringBuf placeholderValues = {},
    int syntaxVersion = 1)
{
    THashMap<TString, TString> queryLiterals;
    if (placeholderValues) {
        queryLiterals = ConvertYsonPlaceholdersToQueryLiterals(placeholderValues);
    }

    NAst::TLexer lexer(source, strayToken, std::move(queryLiterals), syntaxVersion);
    NAst::TParser parser(lexer, astHead, source);

    int result = parser.parse();

    if (result != 0) {
        THROW_ERROR_EXCEPTION("Parse failure")
            << TErrorAttribute("source", source);
    }
}

////////////////////////////////////////////////////////////////////////////////

NAst::TParser::token::yytokentype GetStrayToken(EParseMode mode)
{
    switch (mode) {
        case EParseMode::Query:      return NAst::TParser::token::StrayWillParseQuery;
        case EParseMode::JobQuery:   return NAst::TParser::token::StrayWillParseJobQuery;
        case EParseMode::Expression: return NAst::TParser::token::StrayWillParseExpression;
        default:                     YT_ABORT();
    }
}

NAst::TAstHead MakeAstHead(EParseMode mode)
{
    switch (mode) {
        case EParseMode::Query:
        case EParseMode::JobQuery:   return NAst::TAstHead::MakeQuery();
        case EParseMode::Expression: return NAst::TAstHead::MakeExpression();
        default:                     YT_ABORT();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void DefaultFetchFunctions(const std::vector<TString>& /*names*/, const TTypeInferrerMapPtr& typeInferrers)
{
    MergeFrom(typeInferrers.Get(), *GetBuiltinTypeInferrers());
}

////////////////////////////////////////////////////////////////////////////////

TParsedSource::TParsedSource(const TString& source, NAst::TAstHead astHead)
    : Source(source)
    , AstHead(std::move(astHead))
{ }

std::unique_ptr<TParsedSource> ParseSource(
    const TString& source,
    EParseMode mode,
    TYsonStringBuf placeholderValues,
    int syntaxVersion)
{
    auto parsedSource = std::make_unique<TParsedSource>(
        source,
        MakeAstHead(mode));
    ParseQueryString(
        &parsedSource->AstHead,
        source,
        GetStrayToken(mode),
        placeholderValues,
        syntaxVersion);
    return parsedSource;
}

////////////////////////////////////////////////////////////////////////////////

TJoinClausePtr BuildJoinClause(
    const TDataSplit& foreignDataSplit,
    const NAst::TJoin& tableJoin,
    const TParsedSource& parsedSource,
    const TConstTypeInferrerMapPtr& functions,
    size_t* commonKeyPrefix,
    const TTableSchemaPtr& tableSchema,
    const TQueryPtr& query,
    TBuilderCtx& builder,
    const NLogging::TLogger& Logger)
{
    const auto& aliasMap = parsedSource.AstHead.AliasMap;

    auto foreignTableSchema = foreignDataSplit.TableSchema;
    auto foreignKeyColumnsCount = foreignTableSchema->GetKeyColumns().size();

    auto joinClause = New<TJoinClause>();
    joinClause->Schema.Original = foreignTableSchema;
    joinClause->ForeignObjectId = foreignDataSplit.ObjectId;
    joinClause->ForeignCellId = foreignDataSplit.CellId;
    joinClause->IsLeft = tableJoin.IsLeft;

    // BuildPredicate and BuildTypedExpression are used with foreignBuilder.
    TBuilderCtx foreignBuilder{
        parsedSource.Source,
        functions,
        aliasMap,
        *joinClause->Schema.Original,
        tableJoin.Table.Alias,
        &joinClause->Schema.Mapping};

    std::vector<TSelfEquation> selfEquations;
    selfEquations.reserve(tableJoin.Fields.size() + tableJoin.Lhs.size());
    std::vector<TConstExpressionPtr> foreignEquations;
        foreignEquations.reserve(tableJoin.Fields.size() + tableJoin.Rhs.size());
    // Merge columns.
    for (const auto& referenceExpr : tableJoin.Fields) {
        auto selfColumn = builder.GetColumnPtr(referenceExpr->Reference);
        auto foreignColumn = foreignBuilder.GetColumnPtr(referenceExpr->Reference);

        if (!selfColumn || !foreignColumn) {
            THROW_ERROR_EXCEPTION("Column %Qv not found",
                NAst::InferColumnName(referenceExpr->Reference));
        }

        if (!NTableClient::IsV1Type(selfColumn->LogicalType) || !NTableClient::IsV1Type(foreignColumn->LogicalType)) {
            THROW_ERROR_EXCEPTION("Cannot join column %Qv of nonsimple type",
                NAst::InferColumnName(referenceExpr->Reference))
                << TErrorAttribute("self_type", selfColumn->LogicalType)
                << TErrorAttribute("foreign_type", foreignColumn->LogicalType);
        }

        // N.B. When we try join optional<int32> and int16 columns it must work.
        if (NTableClient::GetWireType(selfColumn->LogicalType) != NTableClient::GetWireType(foreignColumn->LogicalType)) {
            THROW_ERROR_EXCEPTION("Column %Qv type mismatch in join",
                NAst::InferColumnName(referenceExpr->Reference))
                << TErrorAttribute("self_type", selfColumn->LogicalType)
                << TErrorAttribute("foreign_type", foreignColumn->LogicalType);
        }

        selfEquations.push_back({New<TReferenceExpression>(selfColumn->LogicalType, selfColumn->Name), false});
        foreignEquations.push_back(New<TReferenceExpression>(foreignColumn->LogicalType, foreignColumn->Name));
    }

    for (const auto& argument : tableJoin.Lhs) {
        selfEquations.push_back({builder.BuildTypedExpression(argument, ComparableTypes), false});
    }

    for (const auto& argument : tableJoin.Rhs) {
        foreignEquations.push_back(
            foreignBuilder.BuildTypedExpression(argument, ComparableTypes));
    }

    if (selfEquations.size() != foreignEquations.size()) {
        THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
            selfEquations.size(),
            foreignEquations.size())
            << TErrorAttribute("lhs_source", FormatExpression(tableJoin.Lhs))
            << TErrorAttribute("rhs_source", FormatExpression(tableJoin.Rhs));
    }

    for (int index = 0; index < std::ssize(selfEquations); ++index) {
        if (*selfEquations[index].Expression->LogicalType != *foreignEquations[index]->LogicalType) {
            THROW_ERROR_EXCEPTION("Types mismatch in join equation \"%v = %v\"",
                InferName(selfEquations[index].Expression),
                InferName(foreignEquations[index]))
                << TErrorAttribute("self_type", selfEquations[index].Expression->LogicalType)
                << TErrorAttribute("foreign_type", foreignEquations[index]->LogicalType);
        }
    }

    // If can use ranges, rearrange equations according to key columns and enrich with evaluated columns

    std::vector<TSelfEquation> keySelfEquations(foreignKeyColumnsCount);
    std::vector<TConstExpressionPtr> keyForeignEquations(foreignKeyColumnsCount);

    for (size_t equationIndex = 0; equationIndex < foreignEquations.size(); ++equationIndex) {
        const auto& expr = foreignEquations[equationIndex];

        if (const auto* referenceExpr = expr->As<TReferenceExpression>()) {
            int index = ColumnNameToKeyPartIndex(joinClause->GetKeyColumns(), referenceExpr->ColumnName);

            if (index >= 0) {
                THROW_ERROR_EXCEPTION_IF(keyForeignEquations[index], "Foreign key column occurs more than once in a join clause");
                keySelfEquations[index] = selfEquations[equationIndex];
                keyForeignEquations[index] = foreignEquations[equationIndex];
                continue;
            }
        }

        keySelfEquations.push_back(selfEquations[equationIndex]);
        keyForeignEquations.push_back(foreignEquations[equationIndex]);
    }

    size_t keyPrefix = 0;
    for (; keyPrefix < foreignKeyColumnsCount; ++keyPrefix) {
        if (keyForeignEquations[keyPrefix]) {
            YT_VERIFY(keySelfEquations[keyPrefix].Expression);

            if (const auto* referenceExpr = keySelfEquations[keyPrefix].Expression->As<TReferenceExpression>()) {
                if (ColumnNameToKeyPartIndex(query->GetKeyColumns(), referenceExpr->ColumnName) != static_cast<ssize_t>(keyPrefix)) {
                    *commonKeyPrefix = std::min(*commonKeyPrefix, keyPrefix);
                }
            } else {
                *commonKeyPrefix = std::min(*commonKeyPrefix, keyPrefix);
            }

            continue;
        }

        const auto& foreignColumnExpression = foreignTableSchema->Columns()[keyPrefix].Expression();

        if (!foreignColumnExpression) {
            break;
        }

        THashSet<TString> references;
        auto evaluatedColumnExpression = PrepareExpression(
            *foreignColumnExpression,
            *foreignTableSchema,
            functions,
            &references);

        auto canEvaluate = true;
        for (const auto& reference : references) {
            int referenceIndex = foreignTableSchema->GetColumnIndexOrThrow(reference);
            if (!keySelfEquations[referenceIndex].Expression) {
                YT_VERIFY(!keyForeignEquations[referenceIndex]);
                canEvaluate = false;
            }
        }

        if (!canEvaluate) {
            break;
        }

        keySelfEquations[keyPrefix] = {evaluatedColumnExpression, true};

        auto reference = NAst::TReference(
            foreignTableSchema->Columns()[keyPrefix].Name(),
            tableJoin.Table.Alias);

        auto foreignColumn = foreignBuilder.GetColumnPtr(reference);

        keyForeignEquations[keyPrefix] = New<TReferenceExpression>(
            foreignColumn->LogicalType,
            foreignColumn->Name);
    }

    *commonKeyPrefix = std::min(*commonKeyPrefix, keyPrefix);

    for (size_t index = 0; index < keyPrefix; ++index) {
        if (keySelfEquations[index].Evaluated) {
            const auto& evaluatedColumnExpression = keySelfEquations[index].Expression;

            if (const auto& selfColumnExpression = tableSchema->Columns()[index].Expression()) {
                auto evaluatedSelfColumnExpression = PrepareExpression(
                    *selfColumnExpression,
                    *tableSchema,
                    functions);

                if (!Compare(
                    evaluatedColumnExpression,
                    *foreignTableSchema,
                    evaluatedSelfColumnExpression,
                    *tableSchema,
                    *commonKeyPrefix))
                {
                    *commonKeyPrefix = std::min(*commonKeyPrefix, index);
                }
            } else {
                *commonKeyPrefix = std::min(*commonKeyPrefix, index);
            }
        }
    }

    YT_VERIFY(keyForeignEquations.size() == keySelfEquations.size());

    size_t lastEmptyIndex = keyPrefix;
    for (int index = keyPrefix; index < std::ssize(keyForeignEquations); ++index) {
        if (keyForeignEquations[index]) {
            YT_VERIFY(keySelfEquations[index].Expression);
            keyForeignEquations[lastEmptyIndex] = std::move(keyForeignEquations[index]);
            keySelfEquations[lastEmptyIndex] = std::move(keySelfEquations[index]);
            ++lastEmptyIndex;
        }
    }

    keyForeignEquations.resize(lastEmptyIndex);
    keySelfEquations.resize(lastEmptyIndex);

    joinClause->SelfEquations = std::move(keySelfEquations);
    joinClause->ForeignEquations = std::move(keyForeignEquations);
    joinClause->ForeignKeyPrefix = keyPrefix;
    joinClause->CommonKeyPrefix = *commonKeyPrefix;

    YT_LOG_DEBUG("Creating join (CommonKeyPrefix: %v, ForeignKeyPrefix: %v)",
        *commonKeyPrefix,
        keyPrefix);

    if (tableJoin.Predicate) {
        joinClause->Predicate = BuildPredicate(
            *tableJoin.Predicate,
            foreignBuilder,
            "JOIN-PREDICATE-clause");
    }

    builder.Merge(foreignBuilder);

    return joinClause;
}

TJoinClausePtr BuildArrayJoinClause(
    const NAst::TArrayJoin& arrayJoin,
    const TParsedSource& parsedSource,
    const TConstTypeInferrerMapPtr& functions,
    TBuilderCtx& builder)
{
    auto arrayJoinClause = New<TJoinClause>();
    arrayJoinClause->IsLeft = arrayJoin.IsLeft;

    int arrayCount = std::ssize(arrayJoin.Columns);

    TSchemaColumns nestedColumns(arrayCount);
    arrayJoinClause->ArrayExpressions.resize(arrayCount);
    for (int index = 0; index < arrayCount; ++index) {
        const auto& expr = arrayJoin.Columns[index];
        const auto* aliasExpression = expr->As<NAst::TAliasExpression>();
        YT_ASSERT(aliasExpression);

        const auto& typedExpression =
            arrayJoinClause->ArrayExpressions[index] =
                builder.BuildTypedExpression(
                    aliasExpression->Expression,
                    {EValueType::Composite});

        auto logicalType = typedExpression->LogicalType;
        auto metatype = logicalType->GetMetatype();
        if (metatype == ELogicalMetatype::Optional) {
            logicalType = logicalType->UncheckedAsOptionalTypeRef().GetElement();
            metatype = logicalType->GetMetatype();
        }

        THROW_ERROR_EXCEPTION_IF(metatype != ELogicalMetatype::List,
            "Expected a list-like type expression in the ARRAY JOIN operator, got %v",
            *typedExpression->LogicalType);

        auto containedType = logicalType->UncheckedAsListTypeRef().GetElement();
        nestedColumns[index] = TColumnSchema(aliasExpression->Name, std::move(containedType));
    }

    arrayJoinClause->Schema.Original = New<TTableSchema>(std::move(nestedColumns));

    TBuilderCtx arrayBuilder{
        parsedSource.Source,
        functions,
        parsedSource.AstHead.AliasMap,
        *arrayJoinClause->Schema.Original,
        std::nullopt,
        &arrayJoinClause->Schema.Mapping,
    };

    for (const auto& nestedTableColumn : arrayJoinClause->Schema.Original->Columns()) {
        auto column = arrayBuilder.GetColumnPtr(NAst::TReference(nestedTableColumn.Name()));
        YT_ASSERT(column);
    }

    if (arrayJoin.Predicate) {
        arrayJoinClause->Predicate = BuildPredicate(
            *arrayJoin.Predicate,
            arrayBuilder,
            "JOIN-PREDICATE-clause");
    }

    builder.Merge(arrayBuilder);

    return arrayJoinClause;
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TPlanFragment> PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const TString& source,
    const TFunctionsFetcher& functionsFetcher,
    TYsonStringBuf placeholderValues,
    int syntaxVersion,
    IMemoryUsageTrackerPtr memoryTracker)
{
    return PreparePlanFragment(
        callbacks,
        *ParseSource(source, EParseMode::Query, placeholderValues, syntaxVersion),
        functionsFetcher,
        memoryTracker);
}

std::unique_ptr<TPlanFragment> PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const TParsedSource& parsedSource,
    const TFunctionsFetcher& functionsFetcher,
    IMemoryUsageTrackerPtr memoryTracker)
{
    auto query = New<TQuery>(TGuid::Create());

    auto Logger = MakeQueryLogger(query);

    const auto& ast = std::get<NAst::TQuery>(parsedSource.AstHead.Ast);
    const auto& aliasMap = parsedSource.AstHead.AliasMap;

    auto functionNames = ExtractFunctionNames(ast, aliasMap);

    auto functions = New<TTypeInferrerMap>();
    functionsFetcher(functionNames, functions);

    const auto& table = ast.Table;

    YT_LOG_DEBUG("Getting initial data splits (PrimaryPath: %v, ForeignPaths: %v)",
        table.Path,
        MakeFormattableView(
            ast.Joins,
            [] (TStringBuilderBase* builder, const std::variant<NAst::TJoin, NAst::TArrayJoin>& join) {
                if (auto* tableJoin = std::get_if<NAst::TJoin>(&join)) {
                    FormatValue(builder, tableJoin->Table.Path, TStringBuf());
                }
        }));

    std::vector<TFuture<TDataSplit>> asyncDataSplits;
    asyncDataSplits.reserve(ast.Joins.size() + 1);
    asyncDataSplits.push_back(callbacks->GetInitialSplit(table.Path));
    for (const auto& join : ast.Joins) {
        Visit(join,
            [&] (const NAst::TJoin& tableJoin) {
                asyncDataSplits.push_back(callbacks->GetInitialSplit(tableJoin.Table.Path));
            },
            [&] (const NAst::TArrayJoin& /*arrayJoin*/) {
                auto defaultPromise = NewPromise<TDataSplit>();
                defaultPromise.Set(TDataSplit{});
                asyncDataSplits.push_back(defaultPromise.ToFuture());
            });
    }

    auto dataSplits = WaitFor(AllSucceeded(asyncDataSplits))
        .ValueOrThrow();

    YT_LOG_DEBUG("Initial data splits received");

    const auto& selfDataSplit = dataSplits[0];

    auto tableSchema = selfDataSplit.TableSchema;
    query->Schema.Original = tableSchema;

    TBuilderCtx builder{
        parsedSource.Source,
        functions,
        aliasMap,
        *query->Schema.Original,
        table.Alias,
        &query->Schema.Mapping};

    size_t commonKeyPrefix = std::numeric_limits<size_t>::max();

    std::vector<TJoinClausePtr> joinClauses;
    for (size_t joinIndex = 0; joinIndex < ast.Joins.size(); ++joinIndex) {
        const auto& join = ast.Joins[joinIndex];
        Visit(join,
            [&] (const NAst::TJoin& tableJoin) {
                joinClauses.push_back(BuildJoinClause(
                    dataSplits[joinIndex + 1],
                    tableJoin,
                    parsedSource,
                    functions,
                    &commonKeyPrefix,
                    tableSchema,
                    query,
                    builder,
                    Logger));
            },
            [&] (const NAst::TArrayJoin& arrayJoin) {
                joinClauses.push_back(BuildArrayJoinClause(
                    arrayJoin,
                    parsedSource,
                    functions,
                    builder));
            });


    }

    PrepareQuery(query, ast, builder);

    // Must be filled after builder.Finish()
    for (const auto& [reference, entry] : builder.Lookup) {
        auto formattedName = NAst::InferColumnName(reference);

        for (size_t index = entry.OriginTableIndex; index < entry.LastTableIndex; ++index) {
            YT_VERIFY(index < joinClauses.size());
            joinClauses[index]->SelfJoinedColumns.insert(formattedName);
        }

        if (entry.OriginTableIndex > 0 && entry.LastTableIndex > 0) {
            joinClauses[entry.OriginTableIndex - 1]->ForeignJoinedColumns.insert(formattedName);
        }
    }

    // Why after PrepareQuery? GetTableSchema is called inside PrepareQuery?
    query->JoinClauses.assign(joinClauses.begin(), joinClauses.end());

    if (ast.Limit) {
        if (*ast.Limit > MaxQueryLimit) {
            THROW_ERROR_EXCEPTION("Maximum LIMIT exceeded")
                << TErrorAttribute("limit", *ast.Limit)
                << TErrorAttribute("max_limit", MaxQueryLimit);
        }

        query->Limit = *ast.Limit;

        if (!query->OrderClause && query->HavingClause) {
            THROW_ERROR_EXCEPTION("HAVING with LIMIT is not allowed");
        }
    } else if (!ast.OrderExpressions.empty()) {
        THROW_ERROR_EXCEPTION("ORDER BY used without LIMIT");
    }

    if (ast.Offset) {
        if (!query->OrderClause && query->HavingClause) {
            THROW_ERROR_EXCEPTION("HAVING with OFFSET is not allowed");
        }

        query->Offset = *ast.Offset;

        if (!ast.Limit) {
            THROW_ERROR_EXCEPTION("OFFSET used without LIMIT");
        }
    }

    auto queryFingerprint = InferName(query, {.OmitValues = true});
    YT_LOG_DEBUG("Prepared query (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v)",
        queryFingerprint,
        *query->GetReadSchema(),
        *query->GetTableSchema());

    auto rowBuffer = New<TRowBuffer>(
        TQueryPreparerBufferTag(),
        TChunkedMemoryPool::DefaultStartChunkSize,
        memoryTracker);

    auto fragment = std::make_unique<TPlanFragment>();
    fragment->Query = query;
    fragment->DataSource.ObjectId = selfDataSplit.ObjectId;
    fragment->DataSource.CellId = selfDataSplit.CellId;
    fragment->DataSource.Ranges = MakeSingletonRowRange(
        selfDataSplit.LowerBound,
        selfDataSplit.UpperBound,
        std::move(rowBuffer));

    return fragment;
}

TQueryPtr PrepareJobQuery(
    const TString& source,
    const TTableSchemaPtr& tableSchema,
    const TFunctionsFetcher& functionsFetcher)
{
    auto astHead = NAst::TAstHead::MakeQuery();
    ParseQueryString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseJobQuery);

    const auto& ast = std::get<NAst::TQuery>(astHead.Ast);
    const auto& aliasMap = astHead.AliasMap;

    if (ast.Offset) {
        THROW_ERROR_EXCEPTION("OFFSET is not supported in map-reduce queries");
    }

    if (ast.Limit) {
        THROW_ERROR_EXCEPTION("LIMIT is not supported in map-reduce queries");
    }

    if (ast.GroupExprs) {
        THROW_ERROR_EXCEPTION("GROUP BY is not supported in map-reduce queries");
    }

    auto query = New<TQuery>(TGuid::Create());
    query->Schema.Original = tableSchema;

    auto functionNames = ExtractFunctionNames(ast, aliasMap);

    auto functions = New<TTypeInferrerMap>();
    functionsFetcher(functionNames, functions);

    TBuilderCtx builder{
        source,
        functions,
        aliasMap,
        *tableSchema,
        std::nullopt,
        &query->Schema.Mapping};

    PrepareQuery(
        query,
        ast,
        builder);

    return query;
}

TConstExpressionPtr PrepareExpression(
    const TString& source,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions,
    THashSet<TString>* references)
{
    return PrepareExpression(
        *ParseSource(source, EParseMode::Expression),
        tableSchema,
        functions,
        references);
}

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions,
    THashSet<TString>* references)
{
    auto expr = std::get<NAst::TExpressionPtr>(parsedSource.AstHead.Ast);
    const auto& aliasMap = parsedSource.AstHead.AliasMap;

    std::vector<TColumnDescriptor> mapping;

    TBuilderCtx builder{
        parsedSource.Source,
        functions,
        aliasMap,
        tableSchema,
        std::nullopt,
        &mapping};

    auto result = builder.BuildTypedExpression(expr);

    if (references) {
        for (const auto& item : mapping) {
            references->insert(item.Name);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
