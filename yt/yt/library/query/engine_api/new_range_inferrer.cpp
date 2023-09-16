#include "new_range_inferrer.h"

#include <yt/yt/library/query/base/query_helpers.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

// Build mapping from schema key index to index of reference in tuple.
std::vector<int> BuildKeyMapping(const TKeyColumns& keyColumns, TRange<TConstExpressionPtr> expressions)
{
    std::vector<int> keyMapping(keyColumns.size(), -1);
    for (int index = 0; index < std::ssize(expressions); ++index) {
        const auto* referenceExpr = expressions[index]->As<TReferenceExpression>();
        if (referenceExpr) {
            int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
            if (keyPartIndex >= 0 && keyMapping[keyPartIndex] == -1) {
                keyMapping[keyPartIndex] = index;
            }
        }
    }
    return keyMapping;
}

int CompareRowUsingMapping(TRow lhs, TRow rhs, TRange<int> mapping)
{
    for (auto index : mapping) {
        if (index == -1) {
            continue;
        }

        int result = CompareRowValuesCheckingNan(lhs.Begin()[index], rhs.Begin()[index]);

        if (result != 0) {
            return result;
        }
    }
    return 0;
}

int GetEqualPrefixUsingMapping(TRow lhs, TRow rhs, TRange<int> mapping)
{
    int equalPrefix = 0;
    for (auto index : mapping) {
        if (index == -1) {
            ++equalPrefix;
            continue;
        }

        if (lhs.Begin()[index] != rhs.Begin()[index]) {
            break;
        }

        ++equalPrefix;
    }
    return equalPrefix;
}

TConstraintRef TConstraintsHolder::ExtractFromExpression(
    const TConstExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer,
    const TConstConstraintExtractorMapPtr& constraintExtractors)
{
    YT_VERIFY(!keyColumns.empty());

    if (!expr) {
        return TConstraintRef::Universal();
    }

    if (const auto* binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        auto opcode = binaryOpExpr->Opcode;
        auto lhsExpr = binaryOpExpr->Lhs;
        auto rhsExpr = binaryOpExpr->Rhs;

        if (opcode == EBinaryOp::And) {
            auto lhsConstraint = ExtractFromExpression(lhsExpr, keyColumns, rowBuffer, constraintExtractors);
            auto rhsConstraint = ExtractFromExpression(rhsExpr, keyColumns, rowBuffer, constraintExtractors);
            return TConstraintsHolder::Intersect(lhsConstraint, rhsConstraint);
        } else if (opcode == EBinaryOp::Or) {
            auto lhsConstraint = ExtractFromExpression(lhsExpr, keyColumns, rowBuffer, constraintExtractors);
            auto rhsConstraint = ExtractFromExpression(rhsExpr, keyColumns, rowBuffer, constraintExtractors);
            return TConstraintsHolder::Unite(lhsConstraint, rhsConstraint);
        } else {
            if (rhsExpr->As<TReferenceExpression>()) {
                // Ensure that references are on the left.
                std::swap(lhsExpr, rhsExpr);
                opcode = GetReversedBinaryOpcode(opcode);
            }

            const auto* referenceExpr = lhsExpr->As<TReferenceExpression>();
            const auto* constantExpr = rhsExpr->As<TLiteralExpression>();

            if (referenceExpr && constantExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0) {
                    auto value = TValue(constantExpr->Value);
                    switch (opcode) {
                        case EBinaryOp::Equal:
                            return Interval(
                                TValueBound{value, false},
                                TValueBound{value, true},
                                keyPartIndex);
                        case EBinaryOp::NotEqual:
                            return TConstraintsHolder::Append(
                                {
                                    TConstraint::Make(
                                        MinBound,
                                        TValueBound{value, false}),
                                    TConstraint::Make(
                                        TValueBound{value, true},
                                        MaxBound)
                                },
                                keyPartIndex);
                        case EBinaryOp::Less:
                            return TConstraintsHolder::Interval(
                                MinBound,
                                TValueBound{value, false},
                                keyPartIndex);
                        case EBinaryOp::LessOrEqual:
                            return TConstraintsHolder::Interval(
                                MinBound,
                                TValueBound{value, true},
                                keyPartIndex);
                        case EBinaryOp::Greater:
                            return TConstraintsHolder::Interval(
                                TValueBound{value, true},
                                MaxBound,
                                keyPartIndex);
                        case EBinaryOp::GreaterOrEqual:
                            return TConstraintsHolder::Interval(
                                TValueBound{value, false},
                                MaxBound,
                                keyPartIndex);
                        default:
                            break;
                    }
                }
            }

            return TConstraintRef::Universal();
        }
    } else if (const auto* functionExpr = expr->As<TFunctionExpression>()) {
        auto foundIt = constraintExtractors->find(functionExpr->FunctionName);
        if (foundIt == constraintExtractors->end()) {
            return TConstraintRef::Universal();
        }

        const auto& constraintExtractor = foundIt->second;

        return constraintExtractor(
            this,
            functionExpr,
            keyColumns,
            rowBuffer);
    } else if (const auto* inExpr = expr->As<TInExpression>()) {
        TRange<TRow> values = inExpr->Values;
        auto rowCount = std::ssize(values);

        auto keyMapping = BuildKeyMapping(keyColumns, inExpr->Arguments);

        bool orderedMapping = true;
        for (int index = 1; index < std::ssize(keyMapping); ++index) {
            if (keyMapping[index] <= keyMapping[index - 1]) {
                orderedMapping = false;
                break;
            }
        }

        std::vector<TRow> sortedValues;
        if (!orderedMapping) {
            sortedValues = values.ToVector();
            std::sort(sortedValues.begin(), sortedValues.end(), [&] (TRow lhs, TRow rhs) {
                return CompareRowUsingMapping(lhs, rhs, keyMapping) < 0;
            });
            values = sortedValues;
        }

        std::vector<int> commonKeyPrefixes(1, 0);

        for (int rowIndex = 1; rowIndex < rowCount; ++rowIndex) {
            commonKeyPrefixes.push_back(GetEqualPrefixUsingMapping(values[rowIndex - 1], values[rowIndex], keyMapping));
        }

        ui32 lastKeyStartOffset = 0;

        int lastKeyIndex = -1;
        for (int keyIndex = keyMapping.size() - 1; keyIndex >= 0; --keyIndex) {
            auto index = keyMapping[keyIndex];
            if (index >= 0) {
                auto& columnConstraints = (*this)[keyIndex];

                int currentNextOffset = lastKeyStartOffset;
                lastKeyStartOffset = columnConstraints.size();

                for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
                    const auto& value = values[rowIndex][index];

                    if (commonKeyPrefixes[rowIndex] > keyIndex) {
                        if (lastKeyIndex >= 0 && commonKeyPrefixes[rowIndex] <= lastKeyIndex) {
                            ++currentNextOffset;
                            columnConstraints.back().Next.EndIndex = currentNextOffset;
                        }
                    } else {
                        auto next = TConstraintRef::Universal();
                        if (lastKeyIndex >= 0) {
                            next.ColumnId = lastKeyIndex;
                            next.StartIndex = currentNextOffset;
                            ++currentNextOffset;
                            next.EndIndex = currentNextOffset;
                        }

                        columnConstraints.push_back(TConstraint::Make(
                            TValueBound{value, false},
                            TValueBound{value, true},
                            next));
                    }
                }

                lastKeyIndex = keyIndex;
            }
        }

        auto result = TConstraintRef::Universal();
        if (lastKeyIndex >= 0) {
            result.ColumnId = lastKeyIndex;
            result.StartIndex = lastKeyStartOffset;
            result.EndIndex = (*this)[lastKeyIndex].size();
        }

        return result;
    } else if (const auto* betweenExpr = expr->As<TBetweenExpression>()) {
        const auto& expressions = betweenExpr->Arguments;
        std::vector<int> keyColumnIds;

        for (int index = 0; index < std::ssize(expressions); ++index) {
            const auto* referenceExpr = expressions[index]->As<TReferenceExpression>();
            if (!referenceExpr) {
                break;
            }

            int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);

            if (keyPartIndex < 0 || !keyColumnIds.empty() && keyColumnIds.back() >= keyPartIndex) {
                break;
            }

            keyColumnIds.push_back(keyPartIndex);
        }

        if (keyColumnIds.empty()) {
            return TConstraintRef::Universal();
        }

        // BETWEEN (a, b, c) and (k, l, m) generates the following constraints:
        // [a-, a+]    [b-, b+]   [c-, c+]
        //                        [c+, +inf]
        //             [b+, +inf]
        // [a+, k-]
        // [k-, k+]    [-inf, l-]
        //             [l-, l+]   [-inf, m-]
        //                        [m-, m+]

        std::vector<TConstraintRef> rangeConstraints;

        for (int rowIndex = 0; rowIndex < std::ssize(betweenExpr->Ranges); ++rowIndex) {
            auto literalRange = betweenExpr->Ranges[rowIndex];

            auto lower = literalRange.first;
            auto upper = literalRange.second;

            size_t equalPrefix = 0;
            while (equalPrefix < lower.GetCount() &&
                equalPrefix < upper.GetCount() &&
                lower[equalPrefix] == upper[equalPrefix])
            {
                ++equalPrefix;
            }

             // Lower and upper bounds are included.
            auto currentLower = TConstraintRef::Universal();
            auto currentUpper = TConstraintRef::Universal();

            size_t expressionIndex = keyColumnIds.size();

            while (expressionIndex > equalPrefix + 1) {
                --expressionIndex;

                auto keyColumnIndex = keyColumnIds[expressionIndex];

                if (expressionIndex < lower.GetCount()) {
                    const auto& lowerValue = lower[expressionIndex];
                    currentLower = Append({
                            TConstraint::Make(
                                TValueBound{lowerValue, false},
                                TValueBound{lowerValue, true},
                                currentLower),
                            TConstraint::Make(TValueBound{lowerValue, true}, MaxBound)
                        },
                        keyColumnIndex);
                }

                if (expressionIndex < upper.GetCount()) {
                    const auto& upperValue = upper[expressionIndex];
                    currentUpper = Append({
                            TConstraint::Make(MinBound, TValueBound{upperValue, false}),
                            TConstraint::Make(
                                TValueBound{upperValue, false},
                                TValueBound{upperValue, true},
                                currentUpper)
                        },
                        keyColumnIndex);
                }
            }

            auto current = TConstraintRef::Universal();
            if (expressionIndex == equalPrefix + 1) {
                --expressionIndex;
                auto keyColumnIndex = keyColumnIds[expressionIndex];

                if (expressionIndex < lower.GetCount() && expressionIndex < upper.GetCount()) {
                    const auto& lowerValue = lower[expressionIndex];
                    const auto& upperValue = upper[expressionIndex];

                    current = Append({
                            TConstraint::Make(
                                TValueBound{lowerValue, false},
                                TValueBound{lowerValue, true},
                                currentLower),
                            TConstraint::Make(
                                TValueBound{lowerValue, true},
                                TValueBound{upperValue, false}
                            ),
                            TConstraint::Make(
                                TValueBound{upperValue, false},
                                TValueBound{upperValue, true},
                                currentUpper)
                        },
                        keyColumnIndex);
                } else if (expressionIndex < lower.GetCount()) {
                    const auto& lowerValue = lower[expressionIndex];
                    current = Append({
                            TConstraint::Make(
                                TValueBound{lowerValue, false},
                                TValueBound{lowerValue, true},
                                currentLower),
                            TConstraint::Make(TValueBound{lowerValue, true}, MaxBound)
                        },
                        keyColumnIndex);
                } else if (expressionIndex < upper.GetCount()) {
                    const auto& upperValue = upper[expressionIndex];
                    current = Append({
                            TConstraint::Make(MinBound, TValueBound{upperValue, false}),
                            TConstraint::Make(
                                TValueBound{upperValue, false},
                                TValueBound{upperValue, true},
                                currentUpper)
                        },
                        keyColumnIndex);
                }
            }

            while (expressionIndex > 0) {
                --expressionIndex;

                auto keyColumnIndex = keyColumnIds[expressionIndex];

                const auto& value = lower[expressionIndex];
                YT_VERIFY(value == upper[expressionIndex]);

                current = Append({
                        TConstraint::Make(
                            TValueBound{value, false},
                            TValueBound{value, true},
                            current)
                    },
                    keyColumnIndex);
            }

            rangeConstraints.push_back(current);
        }

        while (rangeConstraints.size() > 1) {
            int index = 0;

            while (2 * index + 1 < std::ssize(rangeConstraints)) {
                rangeConstraints[index] = TConstraintsHolder::Unite(rangeConstraints[2 * index], rangeConstraints[2 * index + 1]);
                ++index;
            }

            if (2 * index < std::ssize(rangeConstraints)) {
                rangeConstraints[index] = rangeConstraints[2 * index];
                ++index;
            }

            rangeConstraints.resize(index);
        }

        return rangeConstraints.front();
    } else if (const auto* literalExpr = expr->As<TLiteralExpression>()) {
        TValue value = literalExpr->Value;
        if (value.Type == EValueType::Boolean) {
            return value.Data.Boolean ? TConstraintRef::Universal() : TConstraintRef::Empty();
        }
    }

    return TConstraintRef::Universal();
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK TRangeInferrer CreateNewRangeInferrer(
    TConstExpressionPtr /*predicate*/,
    const TTableSchemaPtr& /*schema*/,
    const TKeyColumns& /*keyColumns*/,
    const IColumnEvaluatorCachePtr& /*evaluatorCache*/,
    const TConstConstraintExtractorMapPtr& /*constraintExtractors*/,
    const TQueryOptions& /*options*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/new_range_inferrer.cpp.
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
