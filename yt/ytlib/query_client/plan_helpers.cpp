#include "stdafx.h"
#include "plan_helpers.h"
#include "key_trie.h"
#include "functions.h"

#include "private.h"
#include "helpers.h"

#include "plan_fragment.h"
#include "function_registry.h"
#include "column_evaluator.h"

#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/name_table.h>
#include <ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NQueryClient {

using namespace NTableClient;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

TKeyTriePtr ExtractMultipleConstraints(
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer,
    const IFunctionRegistryPtr& functionRegistry)
{
    if (!expr) {
        return TKeyTrie::Universal();
    }

    if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        auto opcode = binaryOpExpr->Opcode;
        auto lhsExpr = binaryOpExpr->Lhs;
        auto rhsExpr = binaryOpExpr->Rhs;

        if (opcode == EBinaryOp::And) {
            return IntersectKeyTrie(
                ExtractMultipleConstraints(lhsExpr, keyColumns, rowBuffer, functionRegistry),
                ExtractMultipleConstraints(rhsExpr, keyColumns, rowBuffer, functionRegistry));
        } if (opcode == EBinaryOp::Or) {
            return UniteKeyTrie(
                ExtractMultipleConstraints(lhsExpr, keyColumns, rowBuffer, functionRegistry),
                ExtractMultipleConstraints(rhsExpr, keyColumns, rowBuffer, functionRegistry));
        } else {
            if (rhsExpr->As<TReferenceExpression>()) {
                // Ensure that references are on the left.
                std::swap(lhsExpr, rhsExpr);
                opcode = GetReversedBinaryOpcode(opcode);
            }

            auto referenceExpr = lhsExpr->As<TReferenceExpression>();
            auto constantExpr = rhsExpr->As<TLiteralExpression>();

            auto result = TKeyTrie::Universal();

            if (referenceExpr && constantExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0) {
                    auto value = TValue(constantExpr->Value);

                    result = New<TKeyTrie>(0);

                    auto& bounds = result->Bounds;
                    switch (opcode) {
                        case EBinaryOp::Equal:
                            result->Offset = keyPartIndex;
                            result->Next.emplace_back(value, TKeyTrie::Universal());
                            break;
                        case EBinaryOp::NotEqual:
                            result->Offset = keyPartIndex;
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);

                            break;
                        case EBinaryOp::Less:
                            result->Offset = keyPartIndex;
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, false);

                            break;
                        case EBinaryOp::LessOrEqual:
                            result->Offset = keyPartIndex;
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                            bounds.emplace_back(value, true);

                            break;
                        case EBinaryOp::Greater:
                            result->Offset = keyPartIndex;
                            bounds.emplace_back(value, false);
                            bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);

                            break;
                        case EBinaryOp::GreaterOrEqual:
                            result->Offset = keyPartIndex;
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
        Stroka functionName = functionExpr->FunctionName;
        auto function = functionRegistry->GetFunction(functionName);

        return function->ExtractKeyRange(functionExpr, keyColumns, rowBuffer);
    } else if (auto inExpr = expr->As<TInOpExpression>()) {
        int argsSize = inExpr->Arguments.size();

        std::vector<int> keyMapping(keyColumns.size(), -1);
        for (int index = 0; index < argsSize; ++index) {
            auto referenceExpr = inExpr->Arguments[index]->As<TReferenceExpression>();
            if (referenceExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0 && keyMapping[keyPartIndex] == -1) {
                    keyMapping[keyPartIndex] = index;
                }
            }
        }

        std::vector<TKeyTriePtr> keyTries;
        for (int rowIndex = 0; rowIndex < inExpr->Values.Size(); ++rowIndex) {
            auto literalTuple = inExpr->Values[rowIndex];

            auto rowConstraint = TKeyTrie::Universal();
            for (int keyIndex = keyMapping.size() - 1; keyIndex >= 0; --keyIndex) {
                auto index = keyMapping[keyIndex];
                if (index >= 0) {
                    auto valueConstraint = New<TKeyTrie>(keyIndex);
                    valueConstraint->Next.emplace_back(literalTuple[index], std::move(rowConstraint));
                    rowConstraint = std::move(valueConstraint);
                }
            }

            keyTries.push_back(rowConstraint);
        }

        return UniteKeyTrie(keyTries);
    }

    return TKeyTrie::Universal();
}

TConstExpressionPtr MakeAndExpression(TConstExpressionPtr lhs, TConstExpressionPtr rhs)
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
        InferBinaryExprType(
            EBinaryOp::And,
            lhs->Type,
            rhs->Type,
            "",
            InferName(lhs),
            InferName(rhs)),
        EBinaryOp::And,
        lhs,
        rhs);
}

TConstExpressionPtr MakeOrExpression(TConstExpressionPtr lhs, TConstExpressionPtr rhs)
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
        InferBinaryExprType(
            EBinaryOp::Or,
            lhs->Type,
            rhs->Type,
            "",
            InferName(lhs),
            InferName(rhs)),
        EBinaryOp::Or,
        lhs,
        rhs);
}

TConstExpressionPtr RefinePredicate(
    const TRowRange& keyRange,
    TConstExpressionPtr expr,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    TColumnEvaluatorPtr columnEvaluator)
{
    auto trueLiteral = New<TLiteralExpression>(
        EValueType::Boolean,
        MakeUnversionedBooleanValue(true));
    auto falseLiteral = New<TLiteralExpression>(
        EValueType::Boolean,
        MakeUnversionedBooleanValue(false));

    int rangeSize = std::min(keyRange.first.GetCount(), keyRange.second.GetCount());
    int commonPrefixSize = 0;
    while (commonPrefixSize < rangeSize) {
        commonPrefixSize++;
        if (keyRange.first[commonPrefixSize - 1] != keyRange.second[commonPrefixSize - 1]) {
            break;
        }
    }

    std::function<TConstExpressionPtr(TConstExpressionPtr expr)> refinePredicate =
        [&] (TConstExpressionPtr expr)->TConstExpressionPtr
    {
        if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
            auto opcode = binaryOpExpr->Opcode;
            auto lhsExpr = binaryOpExpr->Lhs;
            auto rhsExpr = binaryOpExpr->Rhs;

            if (opcode == EBinaryOp::And) {
                return MakeAndExpression( // eliminate constants
                    refinePredicate(lhsExpr),
                    refinePredicate(rhsExpr));
            } if (opcode == EBinaryOp::Or) {
                return MakeOrExpression(
                    refinePredicate(lhsExpr),
                    refinePredicate(rhsExpr));
            } else {
                if (rhsExpr->As<TReferenceExpression>()) {
                    // Ensure that references are on the left.
                    std::swap(lhsExpr, rhsExpr);
                    opcode = GetReversedBinaryOpcode(opcode);
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
            TNameTableToSchemaIdMapping idMapping;

            for (const auto& argument : inExpr->Arguments) {
                auto referenceExpr = argument->As<TReferenceExpression>();
                int index = referenceExpr
                    ? ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName)
                    : -1;
                idMapping.push_back(index);
            }

            TNameTableToSchemaIdMapping reverseIdMapping(keyColumns.size(), -1);
            for (int index = 0; index < idMapping.size(); ++index) {
                if (idMapping[index] != -1) {
                    reverseIdMapping[idMapping[index]] = index;
                }
            }

            int rowSize = keyColumns.size();
            for (int index = 0; index < rowSize; ++index) {
                if (reverseIdMapping[index] == -1 && !tableSchema.Columns()[index].Expression) {
                    rowSize = index;
                }
            }

            const auto areValidReferences = [&] (int index) {
                for (const auto& reference : columnEvaluator->GetReferenceIds(index)) {
                    if (reference >= rowSize) {
                        return false;
                    }
                }
                return true;
            };

            for (int index = 0; index < rowSize; ++index) {
                if (tableSchema.Columns()[index].Expression && !areValidReferences(index)) {
                    rowSize = index;
                }
            }

            auto rowBuffer = New<TRowBuffer>();
            auto tempRow = TUnversionedRow::Allocate(rowBuffer->GetPool(), keyColumns.size());

            auto inRange = [&, tempRow] (TRow literalTuple) mutable {
                for (int tupleIndex = 0; tupleIndex < idMapping.size(); ++tupleIndex) {
                    int schemaIndex = idMapping[tupleIndex];

                    if (schemaIndex >= 0 && schemaIndex < rowSize) {
                        tempRow[schemaIndex] = literalTuple[tupleIndex];
                    }
                }

                for (int index = 0; index < rowSize; ++index) {
                    if (reverseIdMapping[index] == -1) {
                        columnEvaluator->EvaluateKey(tempRow, rowBuffer, index);
                    }
                }

                auto cmpLower = CompareRows(
                    keyRange.first,
                    tempRow,
                    std::min(keyRange.first.GetCount(), rowSize));
                auto cmpUpper = CompareRows(
                    keyRange.second,
                    tempRow,
                    std::min(keyRange.second.GetCount(), rowSize));
                return cmpLower <= 0 && cmpUpper >= 0;
            };

            std::vector<TRow> filteredValues;
            for (auto value : inExpr->Values) {
                if (inRange(value)) {
                    filteredValues.push_back(value);
                }
            }

            if (filteredValues.empty()) {
                return falseLiteral;
            } else {
                return New<TInOpExpression>(
                    inExpr->Arguments,
                    MakeSharedRange(std::move(filteredValues), MakeHolder(inExpr->Values)));
            }
        }

        return expr;
    };

    return refinePredicate(expr);
}

TConstExpressionPtr RefinePredicate(
    const TRange<TRow>& lookupKeys,
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns)
{
    static auto trueLiteral = New<TLiteralExpression>(
        EValueType::Boolean,
        MakeUnversionedBooleanValue(true));
    static auto falseLiteral = New<TLiteralExpression>(
        EValueType::Boolean,
        MakeUnversionedBooleanValue(false));

    std::function<TConstExpressionPtr(TConstExpressionPtr expr)> refinePredicate =
        [&] (TConstExpressionPtr expr)->TConstExpressionPtr
    {
        if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
            auto opcode = binaryOpExpr->Opcode;
            auto lhsExpr = binaryOpExpr->Lhs;
            auto rhsExpr = binaryOpExpr->Rhs;

            // Eliminate constants.
            if (opcode == EBinaryOp::And) {
                return MakeAndExpression(
                    refinePredicate(lhsExpr),
                    refinePredicate(rhsExpr));
            } if (opcode == EBinaryOp::Or) {
                return MakeOrExpression(
                    refinePredicate(lhsExpr),
                    refinePredicate(rhsExpr));
            }
        } else if (auto inExpr = expr->As<TInOpExpression>()) {
            TNameTableToSchemaIdMapping idMapping;

            int maxIndex = 0;
            for (const auto& argument : inExpr->Arguments) {
                auto referenceExpr = argument->As<TReferenceExpression>();
                if (!referenceExpr) {
                    return inExpr;
                }

                int index = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (index == -1) {
                    return inExpr;
                }

                idMapping.push_back(index);
                maxIndex = std::max(maxIndex, index);
            }

            TNameTableToSchemaIdMapping reverseIdMapping(static_cast<size_t>(maxIndex + 1), -1);
            for (int index = 0; index < idMapping.size(); ++index) {
                reverseIdMapping[idMapping[index]] = index;
            }
            auto compareValues = [&] (const TRow& lhs, const TRow& rhs) {
                for (int index = 0; index < reverseIdMapping.size(); ++index) {
                    if (reverseIdMapping[index] != -1) {
                        int result = CompareRowValues(
                            lhs.Begin()[reverseIdMapping[index]],
                            rhs.Begin()[reverseIdMapping[index]]);

                        if (result != 0) {
                            return result < 0;
                        }
                    }
                }
                return false;
            };

            auto compareKeys = [&] (const TRow& lhs, const TRow& rhs) {
                for (int index = 0; index < reverseIdMapping.size(); ++index) {
                    if (reverseIdMapping[index] != -1) {
                        if (index >= lhs.GetCount() || index >= rhs.GetCount()) {
                            return lhs.GetCount() < rhs.GetCount();
                        }

                        int result = CompareRowValues(
                            lhs.Begin()[index],
                            rhs.Begin()[index]);

                        if (result != 0) {
                            return result < 0;
                        }
                    }
                }
                return false;
            };

            auto compareKeyAndValue = [&] (const TRow& lhs, const TRow& rhs) {
                for (int index = 0; index < reverseIdMapping.size(); ++index) {
                    if (reverseIdMapping[index] != -1) {
                        if (index >= lhs.GetCount()) {
                            return -1;
                        }

                        int result = CompareRowValues(
                            lhs.Begin()[index],
                            rhs.Begin()[reverseIdMapping[index]]);

                        if (result != 0) {
                            return result;
                        }
                    }
                }
                return 0;
            };

            std::vector<TRow> sortedValues(inExpr->Values.Begin(), inExpr->Values.End());
            std::sort(sortedValues.begin(), sortedValues.end(), compareValues);

            std::vector<TRow> sortedKeys(lookupKeys.Begin(), lookupKeys.End());
            std::sort(sortedKeys.begin(), sortedKeys.end(), compareKeys);

            auto canOmitInExpr = [&] () {
                int keyIndex = 0;
                int tupleIndex = 0;
                while (keyIndex < sortedKeys.size() && tupleIndex < sortedValues.size()) {
                    int result = compareKeyAndValue(sortedKeys[keyIndex], sortedValues[tupleIndex]);
                    if (result < 0) {
                        return false;
                    } else if (result == 0) {
                        ++keyIndex;
                    } else {
                        ++tupleIndex;
                    }
                }
                return keyIndex == sortedKeys.size();
            };

            if (canOmitInExpr()) {
                return trueLiteral;
            } else {
                return inExpr;
            }
        }

        return expr;
    };

    return refinePredicate(expr);
}

TKeyRange Unite(const TKeyRange& first, const TKeyRange& second)
{
    const auto& lower = ChooseMinKey(first.first, second.first);
    const auto& upper = ChooseMaxKey(first.second, second.second);
    return std::make_pair(lower, upper);
}

TRowRange Unite(const TRowRange& first, const TRowRange& second)
{
    const auto& lower = std::min(first.first, second.first);
    const auto& upper = std::max(first.second, second.second);
    return std::make_pair(lower, upper);
}

TKeyRange Intersect(const TKeyRange& first, const TKeyRange& second)
{
    const auto* leftmost = &first;
    const auto* rightmost = &second;

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

TRowRange Intersect(const TRowRange& first, const TRowRange& second)
{
    const auto* leftmost = &first;
    const auto* rightmost = &second;

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

bool IsEmpty(const TRowRange& keyRange)
{
    return keyRange.first >= keyRange.second;
}

bool AreAllReferencesInSchema(TConstExpressionPtr expr, const TTableSchema& tableSchema)
{
    if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        return tableSchema.FindColumn(referenceExpr->ColumnName);
    } else if (expr->As<TLiteralExpression>()) {
        return true;
    } else if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        return AreAllReferencesInSchema(binaryOpExpr->Lhs, tableSchema) && AreAllReferencesInSchema(binaryOpExpr->Rhs, tableSchema);
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        bool result = true;
        for (const auto& argument : functionExpr->Arguments) {
            result = result && AreAllReferencesInSchema(argument, tableSchema);
        }
        return result;
    } else if (auto inExpr = expr->As<TInOpExpression>()) {
        bool result = true;
        for (const auto& argument : inExpr->Arguments) {
            result = result && AreAllReferencesInSchema(argument, tableSchema);
        }
        return result;
    }

    return false;
}

TConstExpressionPtr ExtractPredicateForColumnSubset(
    TConstExpressionPtr expr,
    const TTableSchema& tableSchema)
{
    if (!expr) {
        return nullptr;
    }

    if (AreAllReferencesInSchema(expr, tableSchema)) {
        return expr;
    } else if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        auto opcode = binaryOpExpr->Opcode;
        if (opcode == EBinaryOp::And) {
            return MakeAndExpression(
                ExtractPredicateForColumnSubset(binaryOpExpr->Lhs, tableSchema),
                ExtractPredicateForColumnSubset(binaryOpExpr->Rhs, tableSchema));
        } if (opcode == EBinaryOp::Or) {
            return MakeOrExpression(
                ExtractPredicateForColumnSubset(binaryOpExpr->Lhs, tableSchema),
                ExtractPredicateForColumnSubset(binaryOpExpr->Rhs, tableSchema));
        }
    }

    return New<TLiteralExpression>(
        EValueType::Boolean,
        MakeUnversionedBooleanValue(true));
}

std::vector<TRowRange> MergeOverlappingRanges(
    std::vector<TRowRange> ranges)
{
    int lastIndex = ranges.empty() ? -1 : 0;
    std::sort(ranges.begin(), ranges.end());
    for (int index = 1; index < ranges.size(); ++index) {
        if (ranges[index].first <= ranges[lastIndex].second) {
            if (ranges[lastIndex].second < ranges[index].second) {
                ranges[lastIndex].second = std::move(ranges[index].second);
            }
        } else if (ranges[index].first == ranges[index].second) {
            continue;
        } else {
            ++lastIndex;
            if (lastIndex < index) {
                ranges[lastIndex] = std::move(ranges[index]);
            }
        }
    }
    ranges.resize(lastIndex + 1);
    return ranges;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

