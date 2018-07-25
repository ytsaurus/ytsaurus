#include "query_helpers.h"
#include "private.h"
#include "functions.h"
#include "helpers.h"
#include "key_trie.h"
#include "query.h"

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/unversioned_row.h>

namespace NYT {
namespace NQueryClient {

using namespace NTableClient;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

struct TPlanHelpersBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

TKeyTriePtr ExtractMultipleConstraints(
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer,
    const TConstRangeExtractorMapPtr& rangeExtractors)
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
                ExtractMultipleConstraints(lhsExpr, keyColumns, rowBuffer, rangeExtractors),
                ExtractMultipleConstraints(rhsExpr, keyColumns, rowBuffer, rangeExtractors));
        } if (opcode == EBinaryOp::Or) {
            return UniteKeyTrie(
                ExtractMultipleConstraints(lhsExpr, keyColumns, rowBuffer, rangeExtractors),
                ExtractMultipleConstraints(rhsExpr, keyColumns, rowBuffer, rangeExtractors));
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
        auto found = rangeExtractors->find(functionExpr->FunctionName);
        if (found == rangeExtractors->end()) {
            return TKeyTrie::Universal();
        }

        auto rangeExtractor = found->second;

        return rangeExtractor(
            functionExpr,
            keyColumns,
            rowBuffer);
    } else if (auto inExpr = expr->As<TInExpression>()) {
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
    } else if (auto betweenExpr = expr->As<TBetweenExpression>()) {
        int argsSize = betweenExpr->Arguments.size();

        std::vector<int> keyMapping(keyColumns.size(), -1);
        for (int index = 0; index < argsSize; ++index) {
            auto referenceExpr = betweenExpr->Arguments[index]->As<TReferenceExpression>();
            if (referenceExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0 && keyMapping[keyPartIndex] == -1) {
                    keyMapping[keyPartIndex] = index;
                }
            }
        }

        std::vector<TKeyTriePtr> keyTries;
        for (int rowIndex = 0; rowIndex < betweenExpr->Ranges.Size(); ++rowIndex) {
            auto literalRange = betweenExpr->Ranges[rowIndex];

            auto lower = literalRange.first;
            auto upper = literalRange.second;

            size_t prefix = 0;
            while (prefix < lower.GetCount() && prefix < upper.GetCount() && lower[prefix] == upper[prefix]) {
                ++prefix;
            }

            int rangeColumnIndex = -1;
            auto rowConstraint = TKeyTrie::Universal();
            for (int keyIndex = keyMapping.size() - 1; keyIndex >= 0; --keyIndex) {
                auto index = keyMapping[keyIndex];
                if (index >= 0 && index < prefix) {
                    auto valueConstraint = New<TKeyTrie>(keyIndex);
                    valueConstraint->Next.emplace_back(lower[index], std::move(rowConstraint));
                    rowConstraint = std::move(valueConstraint);
                }

                if (index == prefix) {
                    rangeColumnIndex = keyIndex;
                }
            }

            if (rangeColumnIndex != -1) {
                auto rangeConstraint = New<TKeyTrie>(rangeColumnIndex);
                auto& bounds = rangeConstraint->Bounds;

                bounds.emplace_back(
                    lower.GetCount() > prefix
                        ? lower[prefix]
                        : MakeUnversionedSentinelValue(EValueType::Min),
                    true);

                bounds.emplace_back(
                    upper.GetCount() > prefix
                        ? upper[prefix]
                        : MakeUnversionedSentinelValue(EValueType::Max),
                    true);

                rowConstraint = IntersectKeyTrie(rowConstraint, rangeConstraint);
            }

            keyTries.push_back(rowConstraint);
        }

        return UniteKeyTrie(keyTries);
    }

    return TKeyTrie::Universal();
}

bool IsTrue(TConstExpressionPtr expr)
{
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        TValue value = literalExpr->Value;
        if (value.Type == EValueType::Boolean) {
            return value.Data.Boolean;
        }
    }
    return false;
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
        EValueType::Boolean,
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
        EValueType::Boolean,
        EBinaryOp::Or,
        lhs,
        rhs);
}

namespace {

int CompareRow(TRow lhs, TRow rhs, const std::vector<size_t>& mapping)
{
    for (auto index : mapping) {
        int result = CompareRowValues(lhs.Begin()[index], rhs.Begin()[index]);

        if (result != 0) {
            return result;
        }
    }
    return 0;
}

void SortRows(
    std::vector<TRow>::iterator begin,
    std::vector<TRow>::iterator end,
    const std::vector<size_t>& mapping)
{
    std::sort(begin, end, [&] (TRow lhs, TRow rhs) {
        return CompareRow(lhs, rhs, mapping) < 0;
    });
};

void SortRows(
    std::vector<std::pair<TRow, size_t>>::iterator begin,
    std::vector<std::pair<TRow, size_t>>::iterator end,
    const std::vector<size_t>& mapping)
{
    std::sort(begin, end, [&] (const std::pair<TRow, size_t>& lhs, const std::pair<TRow, size_t>& rhs) {
        return CompareRow(lhs.first, rhs.first, mapping) < 0;
    });
};

} // namespace

TConstExpressionPtr EliminateInExpression(
    const TRange<TRow>& lookupKeys,
    const TInExpression* inExpr,
    const TKeyColumns& keyColumns,
    size_t keyPrefixSize,
    const std::vector<std::pair<TBound, TBound>>* bounds)
{
    static auto trueLiteral = New<TLiteralExpression>(
        EValueType::Boolean,
        MakeUnversionedBooleanValue(true));
    static auto falseLiteral = New<TLiteralExpression>(
        EValueType::Boolean,
        MakeUnversionedBooleanValue(false));

    std::vector<size_t> valueMapping;
    std::vector<size_t> keyMapping;
    TNullable<size_t> rangeArgIndex;

    bool allArgsAreKey = true;
    for (size_t argumentIndex = 0; argumentIndex < inExpr->Arguments.size(); ++argumentIndex) {
        const auto& argument = inExpr->Arguments[argumentIndex];
        auto referenceExpr = argument->As<TReferenceExpression>();
        int keyIndex = referenceExpr
            ? ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName)
            : -1;
        if (keyIndex == -1 || keyIndex >= keyPrefixSize) {
            allArgsAreKey = false;
        } else {
            valueMapping.push_back(argumentIndex);
            keyMapping.push_back(keyIndex);
        }

        if (bounds && keyIndex == keyPrefixSize) {
            rangeArgIndex.Assign(argumentIndex);
        }
    }

    auto compareKeyAndValue = [&] (TRow lhs, TRow rhs) {
        for (int index = 0; index < valueMapping.size(); ++index) {
            int result = CompareRowValues(lhs.Begin()[keyMapping[index]], rhs.Begin()[valueMapping[index]]);

            if (result != 0) {
                return result;
            }
        }
        return 0;
    };

    std::vector<TRow> sortedValues(inExpr->Values.Begin(), inExpr->Values.End());
    if (!allArgsAreKey) {
        SortRows(sortedValues.begin(), sortedValues.end(), valueMapping);
    }

    std::vector<std::pair<TRow, size_t>> sortedKeys(lookupKeys.Size());
    for (size_t index = 0; index < lookupKeys.Size(); ++index) {
        sortedKeys[index] = std::make_pair(lookupKeys[index], index);
    }
    SortRows(sortedKeys.begin(), sortedKeys.end(), keyMapping);

    std::vector<TRow> filteredValues;
    bool hasExtraLookupKeys = false;
    size_t keyIndex = 0;
    size_t tupleIndex = 0;
    while (keyIndex < sortedKeys.size() && tupleIndex < sortedValues.size()) {
        auto currentKey = sortedKeys[keyIndex];
        auto currentValue = sortedValues[tupleIndex];

        int result = compareKeyAndValue(currentKey.first, currentValue);
        if (result == 0) {
            auto keyIndexBegin = keyIndex;
            do {
                ++keyIndex;
            } while (keyIndex < sortedKeys.size()
                && CompareRow(currentKey.first, sortedKeys[keyIndex].first, keyMapping) == 0);

            // from keyIndexBegin to keyIndex
            std::vector<TBound> unitedBounds;
            if (bounds) {
                std::vector<std::vector<TBound>> allBounds;
                for (size_t index = keyIndexBegin; index < keyIndex; ++index) {
                    auto lowerAndUpper = (*bounds)[sortedKeys[index].second];

                    allBounds.push_back(std::vector<TBound>{lowerAndUpper.first, lowerAndUpper.second});
                }

                UniteBounds(&allBounds);

                YCHECK(!allBounds.empty());
                unitedBounds = std::move(allBounds.front());
            }

            do {
                if (!rangeArgIndex || Covers(unitedBounds, sortedValues[tupleIndex][*rangeArgIndex])) {
                    filteredValues.push_back(sortedValues[tupleIndex]);
                }
                ++tupleIndex;
            } while (tupleIndex < sortedValues.size() &&
                CompareRow(currentValue, sortedValues[tupleIndex], valueMapping) == 0);
        } else if (result < 0) {
            hasExtraLookupKeys = true;
            ++keyIndex;
        } else {
            ++tupleIndex;
        }
    }

    if (keyIndex != sortedKeys.size()) {
        hasExtraLookupKeys = true;
    }

    if (!hasExtraLookupKeys && allArgsAreKey) {
        return trueLiteral;
    } else {
        if (filteredValues.empty()) {
            return falseLiteral;
        } else {
            std::sort(filteredValues.begin(), filteredValues.end());
            return New<TInExpression>(
                inExpr->Arguments,
                MakeSharedRange(std::move(filteredValues), inExpr->Values));
        }
    }
}

TConstExpressionPtr EliminatePredicate(
    const TRange<TRowRange>& keyRanges,
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns)
{
    auto trueLiteral = New<TLiteralExpression>(
        EValueType::Boolean,
        MakeUnversionedBooleanValue(true));
    auto falseLiteral = New<TLiteralExpression>(
        EValueType::Boolean,
        MakeUnversionedBooleanValue(false));

    int minCommonPrefixSize = std::numeric_limits<int>::max();
    for (const auto& keyRange : keyRanges) {
        int commonPrefixSize = 0;
        while (commonPrefixSize < keyRange.first.GetCount()
            && commonPrefixSize + 1 < keyRange.second.GetCount()
            && keyRange.first[commonPrefixSize] == keyRange.second[commonPrefixSize])
        {
            commonPrefixSize++;
        }
        minCommonPrefixSize = std::min(minCommonPrefixSize, commonPrefixSize);
    }

    auto getBounds = [] (const TRowRange& keyRange, size_t keyPartIndex) -> std::pair<TBound, TBound> {
        auto lower = keyPartIndex < keyRange.first.GetCount()
            ? TBound(keyRange.first[keyPartIndex], true)
            : TBound(MakeUnversionedSentinelValue(EValueType::Min), false);

        YCHECK(keyPartIndex < keyRange.second.GetCount());
        auto upper = TBound(keyRange.second[keyPartIndex], keyPartIndex + 1 < keyRange.second.GetCount());

        return std::make_pair(lower, upper);
    };

    // Is it a good idea? Heavy, not always useful calculation.
    std::vector<std::vector<TBound>> unitedBoundsByColumn(minCommonPrefixSize + 1);
    for (size_t keyPartIndex = 0; keyPartIndex <= minCommonPrefixSize; ++keyPartIndex) {
        std::vector<std::vector<TBound>> allBounds;
        for (const auto& keyRange : keyRanges) {
            auto bounds = getBounds(keyRange, keyPartIndex);
            allBounds.push_back(std::vector<TBound>{bounds.first, bounds.second});
        }

        UniteBounds(&allBounds);
        YCHECK(!allBounds.empty());
        unitedBoundsByColumn[keyPartIndex] = std::move(allBounds.front());
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
                    if (keyPartIndex >= 0 && keyPartIndex <= minCommonPrefixSize) {
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
                            auto resultBounds = IntersectBounds(bounds, unitedBoundsByColumn[keyPartIndex]);

                            if (resultBounds.empty()) {
                                return falseLiteral;
                            } else if (resultBounds == unitedBoundsByColumn[keyPartIndex]) {
                                return trueLiteral;
                            }
                        }
                    }
                }
            }
        } else if (auto inExpr = expr->As<TInExpression>()) {
            std::vector<TRow> lookupKeys;
            std::vector<std::pair<TBound, TBound>> bounds;
            for (const auto& keyRange : keyRanges) {
                lookupKeys.push_back(keyRange.first);
                bounds.push_back(getBounds(keyRange, minCommonPrefixSize));
            }

            return EliminateInExpression(MakeRange(lookupKeys), inExpr, keyColumns, minCommonPrefixSize, &bounds);
        }

        return expr;
    };

    return refinePredicate(expr);
}

TConstExpressionPtr EliminatePredicate(
    const TRange<TRow>& lookupKeys,
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns)
{
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
        } else if (auto inExpr = expr->As<TInExpression>()) {
            return EliminateInExpression(lookupKeys, inExpr, keyColumns, keyColumns.size(), nullptr);
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
    } else if (auto unaryOpExpr = expr->As<TUnaryOpExpression>()) {
        return AreAllReferencesInSchema(unaryOpExpr->Operand, tableSchema);
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        bool result = true;
        for (const auto& argument : functionExpr->Arguments) {
            result = result && AreAllReferencesInSchema(argument, tableSchema);
        }
        return result;
    } else if (auto inExpr = expr->As<TInExpression>()) {
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

void CollectOperands(std::vector<TConstExpressionPtr>* operands, TConstExpressionPtr expr)
{
    if (!expr) {
        return;
    }

    if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        auto opcode = binaryOpExpr->Opcode;
        if (opcode == EBinaryOp::And) {
            CollectOperands(operands, binaryOpExpr->Lhs);
            CollectOperands(operands, binaryOpExpr->Rhs);
        } else {
            operands->push_back(expr);
        }
    } else {
        operands->push_back(expr);
    }
}

std::pair<TConstExpressionPtr, TConstExpressionPtr> SplitPredicateByColumnSubset(
    TConstExpressionPtr root,
    const TTableSchema& tableSchema)
{
    // collect AND operands
    std::vector<TConstExpressionPtr> operands;

    CollectOperands(&operands, root);
    TConstExpressionPtr projected = New<TLiteralExpression>(
        EValueType::Boolean,
        MakeUnversionedBooleanValue(true));
    TConstExpressionPtr remaining = New<TLiteralExpression>(
        EValueType::Boolean,
        MakeUnversionedBooleanValue(true));

    for (auto expr : operands) {
        auto& target = AreAllReferencesInSchema(expr, tableSchema) ? projected : remaining;
        target = MakeAndExpression(target, expr);
    }

    return std::make_pair(projected, remaining);
}

std::vector<TMutableRowRange> MergeOverlappingRanges(
    std::vector<TMutableRowRange> ranges)
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

