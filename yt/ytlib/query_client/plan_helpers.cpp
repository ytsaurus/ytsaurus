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
    const TOwningRow& literals,
    const std::vector<TRow>& literalRows,
    const TKeyColumns& keyColumns,
    TRowBuffer* rowBuffer)
{
    if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        auto opcode = binaryOpExpr->Opcode;
        auto lhsExpr = binaryOpExpr->Lhs;
        auto rhsExpr = binaryOpExpr->Rhs;

        if (opcode == EBinaryOp::And) {
            return IntersectKeyTrie(
                ExtractMultipleConstraints(lhsExpr, literals, literalRows, keyColumns, rowBuffer),
                ExtractMultipleConstraints(rhsExpr, literals, literalRows, keyColumns, rowBuffer));
        } if (opcode == EBinaryOp::Or) {
            return UniteKeyTrie(
                ExtractMultipleConstraints(lhsExpr, literals, literalRows, keyColumns, rowBuffer),
                ExtractMultipleConstraints(rhsExpr, literals, literalRows, keyColumns, rowBuffer));
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
                    auto value = literals[constantExpr->Index];

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
                auto value = literals[constantExpr->Index];

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

            return TKeyTrieNode();
        };

        TKeyTrieNode result;

        for (size_t rowIndex = inExpr->RowBegin; rowIndex < inExpr->RowEnd; ++rowIndex) {
            for (size_t keyIndex = 0; keyIndex < keySize; ++keyIndex) {
                result = IntersectKeyTrie(result, emitConstraint(keyIndex, literalRows[rowIndex]));
            }
        }

        return result;
    }

    return TKeyTrieNode();
};

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

