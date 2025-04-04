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

TString InferReferenceName(const NAst::TColumnReference& ref)
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

struct TTableReferenceResolver
    : public IReferenceResolver
{
    const TTableSchema* Schema;
    std::optional<TString> Alias;
    std::vector<TColumnDescriptor>* Mapping;

    TTableReferenceResolver(
        const TTableSchema* schema,
        std::optional<TString> alias,
        std::vector<TColumnDescriptor>* mapping)
        : Schema(schema)
        , Alias(alias)
        , Mapping(mapping)
    { }

    THashMap<
        NAst::TColumnReference,
        TLogicalTypePtr,
        NAst::TColumnReferenceHasher,
        NAst::TColumnReferenceEqComparer> Lookup;

    TLogicalTypePtr Resolve(const NAst::TColumnReference& reference) override
    {
        auto foundIt = Lookup.find(reference);
        if (foundIt != Lookup.end()) {
            return foundIt->second;
        }

        if (Alias == reference.TableName) {
            if (auto* column = Schema->FindColumn(reference.ColumnName)) {
                if (Mapping) {
                    Mapping->push_back(TColumnDescriptor{
                        InferReferenceName(reference),
                        Schema->GetColumnIndex(*column)
                    });
                }

                Lookup.emplace(reference, column->LogicalType());

                return column->LogicalType();
            }
        }

        return nullptr;
    }

    void PopulateAllColumns(IReferenceResolver* targetResolver) override
    {
        for (const auto& column : Schema->Columns()) {
            targetResolver->Resolve(NAst::TReference(column.Name(), Alias));
        }
    }
};

std::unique_ptr<IReferenceResolver> CreateColumnResolver(
    const TTableSchema* schema,
    std::optional<TString> alias,
    std::vector<TColumnDescriptor>* mapping)
{
    return std::make_unique<TTableReferenceResolver>(schema, alias, mapping);
}

struct TJoinReferenceResolver
    : public IReferenceResolver
{
    std::unique_ptr<IReferenceResolver> ParentProvider;

    const TTableSchema* Schema;
    std::optional<TString> Alias;
    std::vector<TColumnDescriptor>* Mapping;

    THashSet<std::string>* SelfJoinedColumns;
    THashSet<std::string>* ForeignJoinedColumns;
    THashSet<std::string> CommonColumnNames;

    THashMap<
        NAst::TColumnReference,
        TLogicalTypePtr,
        NAst::TColumnReferenceHasher,
        NAst::TColumnReferenceEqComparer> Lookup;

    TJoinReferenceResolver(
        std::unique_ptr<IReferenceResolver> parentProvider,
        const TTableSchema* schema,
        std::optional<TString> alias,
        std::vector<TColumnDescriptor>* mapping,
        THashSet<std::string>* selfJoinedColumns,
        THashSet<std::string>* foreignJoinedColumns,
        THashSet<std::string> commonColumnNames)
        : ParentProvider(std::move(parentProvider))
        , Schema(schema)
        , Alias(alias)
        , Mapping(mapping)
        , SelfJoinedColumns(selfJoinedColumns)
        , ForeignJoinedColumns(foreignJoinedColumns)
        , CommonColumnNames(commonColumnNames)
    { }

    TLogicalTypePtr Resolve(const NAst::TColumnReference& reference) override
    {
        auto foundIt = Lookup.find(reference);
        if (foundIt != Lookup.end()) {
            return foundIt->second;
        }

        auto type = ParentProvider->Resolve(reference);

        auto formattedName = InferReferenceName(reference);

        if (type) {
            SelfJoinedColumns->insert(formattedName);
            Lookup.emplace(reference, type);
        }

        if (Alias == reference.TableName) {
            if (auto* column = Schema->FindColumn(reference.ColumnName)) {
                if (type) {
                    if (!CommonColumnNames.contains(formattedName)) {
                        THROW_ERROR_EXCEPTION("Ambiguous resolution for column %Qv",
                            formattedName);
                    }
                } else {
                    if (Mapping) {
                        Mapping->push_back(TColumnDescriptor{
                            formattedName,
                            Schema->GetColumnIndex(*column)
                        });
                    }

                    ForeignJoinedColumns->insert(formattedName);
                    Lookup.emplace(reference, column->LogicalType());

                    type = column->LogicalType();
                }
            }
        }

        return type;
    }

    void PopulateAllColumns(IReferenceResolver* targetResolver) override
    {
        ParentProvider->PopulateAllColumns(targetResolver);

        for (const auto& column : Schema->Columns()) {
            targetResolver->Resolve(NAst::TReference(column.Name(), Alias));
        }
    }
};

std::unique_ptr<IReferenceResolver> CreateJoinColumnResolver(
    std::unique_ptr<IReferenceResolver> parentProvider,
    const TTableSchema* schema,
    std::optional<TString> alias,
    std::vector<TColumnDescriptor>* mapping,
    THashSet<std::string>* selfJoinedColumns,
    THashSet<std::string>* foreignJoinedColumns,
    THashSet<std::string> commonColumnNames)
{
    return std::make_unique<TJoinReferenceResolver>(
        std::move(parentProvider),
        schema,
        alias,
        mapping,
        selfJoinedColumns,
        foreignJoinedColumns,
        commonColumnNames);
}

////////////////////////////////////////////////////////////////////////////////

TExprBuilder::TExprBuilder(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions)
    : Source_(source)
    , Functions_(functions)
{ }

void TExprBuilder::SetGroupData(const TNamedItemList* groupItems, TAggregateItemList* aggregateItems)
{
    YT_VERIFY(!GroupItems_ && !AggregateItems_);

    GroupItems_ = groupItems;
    AggregateItems_ = aggregateItems;
    AfterGroupBy_ = true;
}

TLogicalTypePtr TExprBuilder::GetColumnType(const NAst::TColumnReference& reference)
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

    return ColumnResolver->Resolve(reference);
}

TConstExpressionPtr TExprBuilder::BuildTypedExpression(
    const NAst::TExpression* expr, TRange<EValueType> resultTypes)
{
    return DoBuildTypedExpression(expr, resultTypes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
