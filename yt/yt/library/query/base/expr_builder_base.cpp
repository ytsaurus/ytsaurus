#include "expr_builder_base.h"
#include "functions.h"
#include "helpers.h"
#include "query_helpers.h"

#include "query_visitors.h"

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;

using NYson::TYsonStringBuf;

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

////////////////////////////////////////////////////////////////////////////////

TString InferReferenceName(const NAst::TReference& ref)
{
    TStringBuilder builder;

    if (ref.TableName) {
        builder.AppendString(*ref.TableName);
        builder.AppendChar('.');
    }

    // TODO(lukyan): Do not use final = true if query has any table aliases.
    NAst::FormatIdFinal(&builder, ref.ColumnName);

    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

struct TNotExpressionPropagator
    : public TRewriter<TNotExpressionPropagator>
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
                TValue value = literal->Value;
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
    : public TRewriter<TCastEliminator>
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

        if (functionExpr->FunctionName == "yson_string_to_any") {
            YT_VERIFY(functionExpr->Arguments.size() == 1);
            if (auto* literal = functionExpr->Arguments[0]->As<TLiteralExpression>()) {
                YT_VERIFY(literal->Value.Type() == EValueType::String);
                auto asUnversionedValue = TValue(literal->Value);
                asUnversionedValue.Type = EValueType::Any;
                ValidateYson(TYsonStringBuf(asUnversionedValue.AsStringBuf()), /*nestingLevelLimit*/ 256);
                return New<TLiteralExpression>(EValueType::Any, TOwningValue(asUnversionedValue));
            }
        }

        return TBase::OnFunction(functionExpr);
    }
};

struct TExpressionSimplifier
    : public TRewriter<TExpressionSimplifier>
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

TConstExpressionPtr ApplyRewriters(TConstExpressionPtr expr)
{
    expr = TCastEliminator().Visit(expr);
    expr = TExpressionSimplifier().Visit(expr);
    expr = TNotExpressionPropagator().Visit(expr);
    return expr;
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TValue> FoldConstants(
    EUnaryOp opcode,
    const TConstExpressionPtr& operand)
{
    if (auto literalExpr = operand->As<TLiteralExpression>()) {
        if (opcode == EUnaryOp::Plus) {
            return static_cast<TValue>(literalExpr->Value);
        } else if (opcode == EUnaryOp::Minus) {
            TValue value = literalExpr->Value;
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
            TValue value = literalExpr->Value;
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

std::optional<TValue> FoldConstants(
    EBinaryOp opcode,
    const TConstExpressionPtr& lhsExpr,
    const TConstExpressionPtr& rhsExpr)
{
    auto lhsLiteral = lhsExpr->As<TLiteralExpression>();
    auto rhsLiteral = rhsExpr->As<TLiteralExpression>();
    if (lhsLiteral && rhsLiteral) {
        auto lhs = static_cast<TValue>(lhsLiteral->Value);
        auto rhs = static_cast<TValue>(rhsLiteral->Value);

        auto checkType = [&] {
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

        auto checkTypeIfNotNull = [&] {
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
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) == 0);
                break;
            case EBinaryOp::NotEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) != 0);
                break;
            case EBinaryOp::Less:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) < 0);
                break;
            case EBinaryOp::Greater:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) > 0);
                break;
            case EBinaryOp::LessOrEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) <= 0);
                break;
            case EBinaryOp::GreaterOrEqual:
                checkTypeIfNotNull();
                return MakeUnversionedBooleanValue(CompareRowValuesCheckingNaN(lhs, rhs) >= 0);
                break;
            default:
                break;
        }
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TExprBuilder::TExprBuilder(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions)
    : Source_(source)
    , Functions_(functions)
{ }

void TExprBuilder::AddTable(
    const TTableSchema& schema,
    std::optional<TString> alias,
    std::vector<TColumnDescriptor>* mapping)
{
    Tables_.push_back(TTable{schema, alias, mapping});
}

// Columns already presented in Lookup are shared.
// In mapping presented all columns needed for read and renamed schema.
// SelfJoinedColumns and ForeignJoinedColumns are built from Lookup using OriginTableIndex and LastTableIndex.
void TExprBuilder::Merge(TExprBuilder& other)
{
    size_t otherTablesCount = other.Tables_.size();
    size_t tablesCount = Tables_.size();
    size_t lastTableIndex = tablesCount + otherTablesCount - 1;

    std::move(other.Tables_.begin(), other.Tables_.end(), std::back_inserter(Tables_));

    for (const auto& [reference, entry] : other.Lookup()) {
        auto [it, emplaced] = Lookup_.emplace(
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

void TExprBuilder::PopulateAllColumns()
{
    for (const auto& table : Tables_) {
        for (const auto& column : table.Schema.Columns()) {
            GetColumnPtr(NAst::TReference(column.Name(), table.Alias));
        }
    }
}

void TExprBuilder::SetGroupData(const TNamedItemList* groupItems, TAggregateItemList* aggregateItems)
{
    YT_VERIFY(!GroupItems_ && !AggregateItems_);

    GroupItems_ = groupItems;
    AggregateItems_ = aggregateItems;
    AfterGroupBy_ = true;
}

TLogicalTypePtr TExprBuilder::GetColumnPtr(const NAst::TReference& reference)
{
    if (AfterGroupBy_) {
        // Search other way after group by.
        if (reference.TableName) {
            return nullptr;
        }

        for (int index = 0; index < std::ssize(*GroupItems_); ++index) {
            const auto& item = (*GroupItems_)[index];
            if (item.Name == reference.ColumnName) {
                return item.Expression->LogicalType;
            }
        }
        return nullptr;
    }

    size_t lastTableIndex = Tables_.size() - 1;

    auto found = Lookup_.find(reference);
    if (found != Lookup_.end()) {
        // Provide column from max table index till end.

        size_t nextTableIndex = std::max(found->second.OriginTableIndex, found->second.LastTableIndex) + 1;

        CheckNoOtherColumn(reference, nextTableIndex);

        // Update LastTableIndex after check.
        found->second.LastTableIndex = lastTableIndex;

        return found->second.Column.LogicalType;
    } else if (auto [table, type] = ResolveColumn(reference); table) {
        auto formattedName = InferReferenceName(reference);
        auto column = TBaseColumn(formattedName, type);

        auto emplaced = Lookup_.emplace(
            reference,
            TColumnEntry{
                column,
                lastTableIndex,
                size_t(table - Tables_.data())});

        YT_VERIFY(emplaced.second);
        return type;
    } else {
        return nullptr;
    }
}

TConstExpressionPtr TExprBuilder::BuildTypedExpression(
    const NAst::TExpression* expr, TRange<EValueType> resultTypes)
{
    return DoBuildTypedExpression(expr, resultTypes);
}

void TExprBuilder::CheckNoOtherColumn(const NAst::TReference& reference, size_t startTableIndex) const
{
    for (int index = startTableIndex; index < std::ssize(Tables_); ++index) {
        auto& [schema, alias, mapping] = Tables_[index];

        if (alias == reference.TableName && schema.FindColumn(reference.ColumnName)) {
            THROW_ERROR_EXCEPTION("Ambiguous resolution for column %Qv",
                InferReferenceName(reference));
        }
    }
}

std::pair<const TTable*, TLogicalTypePtr> TExprBuilder::ResolveColumn(const NAst::TReference& reference) const
{
    const TTable* result = nullptr;
    TLogicalTypePtr type;

    int index = 0;
    for (; index < std::ssize(Tables_); ++index) {
        auto& [schema, alias, mapping] = Tables_[index];

        if (alias != reference.TableName) {
            continue;
        }

        if (auto* column = schema.FindColumn(reference.ColumnName)) {
            auto formattedName = InferReferenceName(reference);

            if (mapping) {
                mapping->push_back(TColumnDescriptor{
                    formattedName,
                    schema.GetColumnIndex(*column)
                });
            }
            result = &Tables_[index];
            type = column->LogicalType();
            ++index;
            break;
        }
    }

    CheckNoOtherColumn(reference, index);

    return {result, type};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
