#include "stdafx.h"
#include "plan_helpers.h"
#include "key_trie.h"

#include "private.h"
#include "helpers.h"

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include "plan_fragment.h"

namespace NYT {
namespace NQueryClient {

using namespace NVersionedTableClient;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

// Computes key index for a given column name.
int ColumnNameToKeyPartIndex(const TKeyColumns& keyColumns, const Stroka& columnName)
{
    for (size_t index = 0; index < keyColumns.size(); ++index) {
        if (keyColumns[index] == columnName) {
            return index;
        }
    }
    return -1;
};

// Descend down to conjuncts and disjuncts and extract all constraints.
TKeyTrieNode ExtractMultipleConstraints(
    const TConstExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    TRowBuffer* rowBuffer)
{
    if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        auto opcode = binaryOpExpr->Opcode;
        auto lhsExpr = binaryOpExpr->Lhs;
        auto rhsExpr = binaryOpExpr->Rhs;

        if (opcode == EBinaryOp::And) {
            return IntersectKeyTrie(
                ExtractMultipleConstraints(lhsExpr, keyColumns, rowBuffer),
                ExtractMultipleConstraints(rhsExpr, keyColumns, rowBuffer));
        } if (opcode == EBinaryOp::Or) {
            return UniteKeyTrie(
                ExtractMultipleConstraints(lhsExpr, keyColumns, rowBuffer),
                ExtractMultipleConstraints(rhsExpr, keyColumns, rowBuffer));
        } else {
            if (rhsExpr->As<TReferenceExpression>()) {
                // Ensure that references are on the left.
                std::swap(lhsExpr, rhsExpr);
                switch (opcode) {
                    case EBinaryOp::Equal:
                        opcode = EBinaryOp::Equal;
                    case EBinaryOp::Less:
                        opcode = EBinaryOp::Greater;
                    case EBinaryOp::LessOrEqual:
                        opcode = EBinaryOp::GreaterOrEqual;
                    case EBinaryOp::Greater:
                        opcode = EBinaryOp::Less;
                    case EBinaryOp::GreaterOrEqual:
                        opcode = EBinaryOp::LessOrEqual;
                    default:
                        break;
                }
            }

            auto referenceExpr = lhsExpr->As<TReferenceExpression>();
            auto constantExpr = rhsExpr->As<TLiteralExpression>();

            TKeyTrieNode result;

            if (referenceExpr && constantExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0) {
                    auto value = TValue(constantExpr->Value);

                    auto& bounds = result.Bounds;
                    switch (opcode) {
                        case EBinaryOp::Equal:
                            result.Offset = keyPartIndex;
                            result.Next[value] = TKeyTrieNode();
                            break;
                        case EBinaryOp::NotEqual:
                            result.Offset = keyPartIndex;
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);

                            break;
                        case EBinaryOp::Less:
                            result.Offset = keyPartIndex;
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, false);

                            break;
                        case EBinaryOp::LessOrEqual:
                            result.Offset = keyPartIndex;
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, true);

                            break;
                        case EBinaryOp::Greater:
                            result.Offset = keyPartIndex;
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);

                            break;
                        case EBinaryOp::GreaterOrEqual:
                            result.Offset = keyPartIndex;
                            bounds.emplace_back(value, true);
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);

                            break;
                        default:
                            break;
                    }
                }
            }

            return result;
        }
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        Stroka functionName(functionExpr->FunctionName);
        functionName.to_lower();

        auto lhsExpr = functionExpr->Arguments[0];
        auto rhsExpr = functionExpr->Arguments[1];

        auto referenceExpr = rhsExpr->As<TReferenceExpression>();
        auto constantExpr = lhsExpr->As<TLiteralExpression>();

        TKeyTrieNode result;

        if (functionName == "is_prefix" && referenceExpr && constantExpr) {
            int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
            if (keyPartIndex >= 0) {
                auto value = TValue(constantExpr->Value);

                YCHECK(value.Type == EValueType::String);

                result.Offset = keyPartIndex;
                result.Bounds.emplace_back(value, true);

                ui32 length = value.Length;
                while (length > 0 && value.Data.String[length - 1] == std::numeric_limits<char>::max()) {
                    --length;
                }

                if (length > 0) {
                    char* newValue = rowBuffer->GetUnalignedPool()->AllocateUnaligned(length);
                    memcpy(newValue, value.Data.String, length);
                    ++newValue[length - 1];

                    value.Length = length;
                    value.Data.String = newValue;
                } else {
                    value = MakeSentinelValue<TUnversionedValue>(EValueType::Max);
                }
                result.Bounds.emplace_back(value, false);
            }
        }

        return result;
    } else if (auto inExpr = expr->As<TInOpExpression>()) {
        size_t keySize = inExpr->Arguments.size();
        auto emitConstraint = [&] (size_t index, const TRow& literalTuple) {
            auto referenceExpr = inExpr->Arguments[index]->As<TReferenceExpression>();

            TKeyTrieNode result;
            if (referenceExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);

                if (keyPartIndex >= 0) {
                    result.Offset = keyPartIndex;
                    result.Next[literalTuple[index]] = TKeyTrieNode();
                }
            }

            return result;
        };

        auto result = TKeyTrieNode::Empty();

        for (size_t rowIndex = 0; rowIndex < inExpr->Values.size(); ++rowIndex) {

            TKeyTrieNode rowConstraint;
            for (size_t keyIndex = 0; keyIndex < keySize; ++keyIndex) {
                rowConstraint = IntersectKeyTrie(rowConstraint, emitConstraint(keyIndex, inExpr->Values[rowIndex].Get()));
            }

            result = UniteKeyTrie(result, rowConstraint);
        }

        return result;
    }

    return TKeyTrieNode();
};

TConstExpressionPtr MakeAndExpression(const TConstExpressionPtr& lhs, const TConstExpressionPtr& rhs)
{
    if (auto literalExpr = lhs->As<TLiteralExpression>()) {
        TValue value = literalExpr->Value;
        if (value.Type == EValueType::Boolean) {
            return value.Data.Boolean ? rhs : lhs;
        }
    }

    if (auto literalExpr = rhs->As<TLiteralExpression>()) {
        TValue value = literalExpr->Value;
        if (value.Type == EValueType::Boolean) {
            return value.Data.Boolean ? lhs : rhs;
        }
    }

    return New<TBinaryOpExpression>(
        NullSourceLocation,
        InferBinaryExprType(
            EBinaryOp::And,
            lhs->Type,
            rhs->Type,
            ""),
        EBinaryOp::And,
        lhs,
        rhs);
}

TConstExpressionPtr MakeOrExpression(const TConstExpressionPtr& lhs, const TConstExpressionPtr& rhs)
{
    if (auto literalExpr = lhs->As<TLiteralExpression>()) {
        TValue value = literalExpr->Value;
        if (value.Type == EValueType::Boolean) {
            return value.Data.Boolean ? lhs : rhs;
        }
    }

    if (auto literalExpr = rhs->As<TLiteralExpression>()) {
        TValue value = literalExpr->Value;
        if (value.Type == EValueType::Boolean) {
            return value.Data.Boolean ? rhs : lhs;
        }
    }

    return New<TBinaryOpExpression>(
        NullSourceLocation,
        InferBinaryExprType(
            EBinaryOp::Or,
            lhs->Type,
            rhs->Type,
            ""),
        EBinaryOp::Or,
        lhs,
        rhs);
}

TConstExpressionPtr RefinePredicate(
    const TKeyRange& keyRange,
    size_t commonPrefixSize,
    const TConstExpressionPtr& expr,
    const TKeyColumns& keyColumns)
{
    auto trueLiteral = New<TLiteralExpression>(
        NullSourceLocation,
        EValueType::Boolean,
        MakeUnversionedBooleanValue(true));
    auto falseLiteral = New<TLiteralExpression>(
        NullSourceLocation,
        EValueType::Boolean,
        MakeUnversionedBooleanValue(false));

    if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        auto opcode = binaryOpExpr->Opcode;
        auto lhsExpr = binaryOpExpr->Lhs;
        auto rhsExpr = binaryOpExpr->Rhs;

        if (opcode == EBinaryOp::And) {
            return MakeAndExpression( // eliminate constants
                RefinePredicate(keyRange, commonPrefixSize, lhsExpr, keyColumns),
                RefinePredicate(keyRange, commonPrefixSize, rhsExpr, keyColumns));
        } if (opcode == EBinaryOp::Or) {
            return MakeOrExpression(
                RefinePredicate(keyRange, commonPrefixSize, lhsExpr, keyColumns),
                RefinePredicate(keyRange, commonPrefixSize, rhsExpr, keyColumns));
        } else {
            if (rhsExpr->As<TReferenceExpression>()) {
                // Ensure that references are on the left.
                std::swap(lhsExpr, rhsExpr);
                switch (opcode) {
                    case EBinaryOp::Equal:
                        opcode = EBinaryOp::Equal;
                    case EBinaryOp::Less:
                        opcode = EBinaryOp::Greater;
                    case EBinaryOp::LessOrEqual:
                        opcode = EBinaryOp::GreaterOrEqual;
                    case EBinaryOp::Greater:
                        opcode = EBinaryOp::Less;
                    case EBinaryOp::GreaterOrEqual:
                        opcode = EBinaryOp::LessOrEqual;
                    default:
                        break;
                }
            }

            auto referenceExpr = lhsExpr->As<TReferenceExpression>();
            auto constantExpr = rhsExpr->As<TLiteralExpression>();

            if (referenceExpr && constantExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0 && keyPartIndex < commonPrefixSize) {
                    auto value = TValue(constantExpr->Value);

                    std::vector<TBound> bounds;

                    switch (opcode) {
                        case EBinaryOp::Equal:
                            bounds.emplace_back(value, true);
                            bounds.emplace_back(value, true);
                            break;

                        case EBinaryOp::NotEqual:
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);
                            break;

                        case EBinaryOp::Less:
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, false);
                            break;

                        case EBinaryOp::LessOrEqual:
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, true);
                            break;

                        case EBinaryOp::Greater:
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);
                            break;

                        case EBinaryOp::GreaterOrEqual:
                            bounds.emplace_back(value, true);
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);
                            break;

                        default:
                            break;
                    }

                    if (!bounds.empty()) {
                        auto lowerBound = keyRange.first[keyPartIndex];
                        auto upperBound = keyRange.second[keyPartIndex];
                        bool upperIncluded = keyPartIndex != keyRange.second.GetCount();

                        std::vector<TBound> dataBounds;

                        dataBounds.emplace_back(lowerBound, true);
                        dataBounds.emplace_back(upperBound, upperIncluded);

                        auto resultBounds = IntersectBounds(bounds, dataBounds);

                        if (resultBounds.empty()) {
                            return falseLiteral;
                        } else if (resultBounds == dataBounds) {
                            return trueLiteral;
                        }
                    }
                }
            }
        }
    } else if (auto inExpr = expr->As<TInOpExpression>()) {
        size_t keySize = inExpr->Arguments.size();

        std::vector<TOwningRow> filteredValues;

        auto emitConstraint = [&] (size_t index, const TRow& literalTuple) {
            auto referenceExpr = inExpr->Arguments[index]->As<TReferenceExpression>();

            TKeyTrieNode result;
            if (referenceExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);

                if (keyPartIndex >= 0) {
                    result.Offset = keyPartIndex;
                    result.Next[literalTuple[index]] = TKeyTrieNode();
                }
            }

            return result;
        };

        for (size_t rowIndex = 0; rowIndex < inExpr->Values.size(); ++rowIndex) {

            TKeyTrieNode rowConstraint;
            for (size_t keyIndex = 0; keyIndex < keySize; ++keyIndex) {
                rowConstraint = IntersectKeyTrie(rowConstraint, emitConstraint(keyIndex, inExpr->Values[rowIndex].Get()));
            }

            auto ranges = GetRangesFromTrieWithinRange(keyRange, rowConstraint);

            if (!ranges.empty()) {
                filteredValues.push_back(inExpr->Values[rowIndex]);
            }

        }

        return New<TInOpExpression>(NullSourceLocation, inExpr->Arguments, filteredValues);
    }

    return expr;
}

TKeyRange Unite(const TKeyRange& first, const TKeyRange& second)
{
    const TKey& lower = ChooseMinKey(first.first, second.first);
    const TKey& upper = ChooseMaxKey(first.second, second.second);
    return std::make_pair(lower, upper);
}

TKeyRange Intersect(const TKeyRange& first, const TKeyRange& second)
{
    const TKeyRange* leftmost = &first;
    const TKeyRange* rightmost = &second;

    if (leftmost->first > rightmost->first) {
        std::swap(leftmost, rightmost);
    }

    if (rightmost->first > leftmost->second) {
        // Empty intersection.
        return std::make_pair(rightmost->first, rightmost->first);
    }

    if (rightmost->second > leftmost->second) {
        return std::make_pair(rightmost->first, leftmost->second);
    } else {
        return std::make_pair(rightmost->first, rightmost->second);
    }
}

bool IsEmpty(const TKeyRange& keyRange)
{
    return keyRange.first >= keyRange.second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

