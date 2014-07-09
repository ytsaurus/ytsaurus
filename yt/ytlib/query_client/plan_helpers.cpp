#include "stdafx.h"
#include "plan_helpers.h"
#include "plan_node.h"

#include "private.h"
#include "helpers.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/row_buffer.h>

namespace NYT {
namespace NQueryClient {

using namespace NVersionedTableClient;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

TKeyColumns InferKeyColumns(const TOperator* op)
{
    switch (op->GetKind()) {
        case EOperatorKind::Scan: {
            return GetKeyColumnsFromDataSplit(
                op->As<TScanOperator>()->DataSplits()[0]);
            // TODO(lukyan): assert that other splits hava the same key columns
        }
        case EOperatorKind::Filter: {
            return InferKeyColumns(op->As<TFilterOperator>()->GetSource());
        }
        case EOperatorKind::Group: {
            return TKeyColumns();
        }
        case EOperatorKind::Project: {
            return TKeyColumns();
        }
    }
    YUNREACHABLE();
}

TKeyRange InferKeyRange(const TOperator* op)
{
    switch (op->GetKind()) {
        case EOperatorKind::Scan: {
            auto* scanOp = op->As<TScanOperator>();
            auto unitedKeyRange = std::make_pair(MaxKey(), MinKey());
            for (const auto& dataSplit : scanOp->DataSplits()) {
                unitedKeyRange = Unite(unitedKeyRange, GetBothBoundsFromDataSplit(dataSplit));
            }
            return unitedKeyRange;
        }
        case EOperatorKind::Filter: {
            auto* filterOp = op->As<TFilterOperator>();
            auto* sourceOp = filterOp->GetSource();
            return RefineKeyRange(
                InferKeyColumns(sourceOp),
                InferKeyRange(sourceOp),
                filterOp->GetPredicate());
        }
        case EOperatorKind::Group: {
            return InferKeyRange(op->As<TGroupOperator>()->GetSource());
        }
        case EOperatorKind::Project: {
            return InferKeyRange(op->As<TProjectOperator>()->GetSource());
        }
    }
    YUNREACHABLE();
}

TKeyRange RefineKeyRange(
    const TKeyColumns& keyColumns,
    const TKeyRange& keyRange,
    const TExpression* predicate)
{
    typedef std::tuple<size_t, bool, TValue> TConstraint;
    typedef std::vector<TConstraint> TConstraints;

    TConstraints constraints;

    const int keySize = static_cast<int>(keyColumns.size());

    // Computes key index for a given column name.
    auto columnNameToKeyPartIndex =
    [&] (const Stroka& columnName) -> size_t {
        for (size_t index = 0; index < keySize; ++index) {
            if (keyColumns[index] == columnName) {
                return index;
            }
        }
        return -1;
    };
    
    TRowBuffer rowBuffer;

    // Extract primitive constraints, like "A > 5" or "A = 5", or "5 = A".
    std::function<void(const TBinaryOpExpression*)> extractSingleConstraint =
    [&] (const TBinaryOpExpression* binaryOpExpr) {
        auto opcode = binaryOpExpr->GetOpcode();
        YCHECK(GetBinaryOpcodeKind(opcode) == EBinaryOpKind::Relational);

        auto* lhs = binaryOpExpr->GetLhs();
        auto* rhs = binaryOpExpr->GetRhs();

        if (rhs->IsA<TReferenceExpression>()) {
            // Ensure that references are on the left.
            std::swap(lhs, rhs);
            switch (opcode) {
                case EBinaryOp::Equal:
                    opcode = EBinaryOp::Equal;
                case EBinaryOp::NotEqual:
                    opcode = EBinaryOp::NotEqual;
                case EBinaryOp::Less:
                    opcode = EBinaryOp::Greater;
                case EBinaryOp::LessOrEqual:
                    opcode = EBinaryOp::GreaterOrEqual;
                case EBinaryOp::Greater:
                    opcode = EBinaryOp::Less;
                case EBinaryOp::GreaterOrEqual:
                    opcode = EBinaryOp::LessOrEqual;
                default:
                    YUNREACHABLE();
            }
        }

        auto* referenceExpr = lhs->As<TReferenceExpression>();
        auto* constantExpr = rhs->IsConstant() ? rhs : nullptr;

        if (referenceExpr && constantExpr) {
            int keyPartIndex = columnNameToKeyPartIndex(referenceExpr->GetColumnName());
            auto value = constantExpr->GetConstantValue();
            switch (opcode) {
                case EBinaryOp::Equal:
                    constraints.emplace_back(keyPartIndex, false, value);
                    constraints.emplace_back(keyPartIndex, true, value);
                    break;
                case EBinaryOp::Less:
                    constraints.emplace_back(keyPartIndex, true, GetPrevValue(value, &rowBuffer));
                    break;
                case EBinaryOp::LessOrEqual:
                    constraints.emplace_back(keyPartIndex, true, value);
                    break;
                case EBinaryOp::Greater:
                    constraints.emplace_back(keyPartIndex, false, GetNextValue(value, &rowBuffer));
                    break;
                case EBinaryOp::GreaterOrEqual:
                    constraints.emplace_back(keyPartIndex, false, value);
                    break;
                default:
                    break;
            }            
        }
    };

    // Descend down to conjuncts and extract all constraints.
    std::function<void(const TExpression*)> extractMultipleConstraints =
    [&] (const TExpression* expr) {
        auto* binaryOpExpr = expr->As<TBinaryOpExpression>();
        if (!binaryOpExpr) {
            return;
        }

        auto opcode = binaryOpExpr->GetOpcode();

        if (opcode == EBinaryOp::And) {
            extractMultipleConstraints(binaryOpExpr->GetLhs());
            extractMultipleConstraints(binaryOpExpr->GetRhs());
            return;
        }

        if (GetBinaryOpcodeKind(opcode) == EBinaryOpKind::Relational) {
            extractSingleConstraint(binaryOpExpr);
            return;
        }
    };

    // Now, traverse expression tree and actually extract constraints.
    extractMultipleConstraints(predicate);

    auto leftBound = TRow::Allocate(rowBuffer.GetAlignedPool(), keySize);
    auto rightBound = TRow::Allocate(rowBuffer.GetAlignedPool(), keySize);

    for (int keyPartIndex = 0; keyPartIndex < keySize; ++keyPartIndex) {
        leftBound[keyPartIndex].Type = EValueType::Min;
        rightBound[keyPartIndex].Type = EValueType::Max;
    }


    for (int constraintIndex = 0; constraintIndex < constraints.size(); ++constraintIndex) {
        const auto& constraint = constraints[constraintIndex];

        auto keyPartIndex = std::get<0>(constraint);
        auto constraintDirection = std::get<1>(constraint);
        auto constraintBound = std::get<2>(constraint);

        if (constraintDirection) {
            if (CompareRowValues(rightBound[keyPartIndex], constraintBound) > 0) {
                rightBound[keyPartIndex] = constraintBound;
            }
        } else {
            if (CompareRowValues(leftBound[keyPartIndex], constraintBound) < 0) {
                leftBound[keyPartIndex] = constraintBound;
            }
        }
    }

    if (rightBound[keySize - 1].Type != EValueType::Max) {
        rightBound[keySize - 1] = GetNextValue(rightBound[keySize - 1], &rowBuffer);
    }

    // Increment rightBound
    //{
    //    int keyPartIndex = keySize - 1;

    //    while (keyPartIndex >= 0 && rightBound[keyPartIndex].Type == EValueType::Max) {
    //        rightBound[keyPartIndex].Type = EValueType::Min;
    //        --keyPartIndex;
    //    }

    //    if (keyPartIndex >= 0) {
    //        rightBound[keyPartIndex] = GetNextValue(rightBound[keyPartIndex], &rowBuffer);
    //    }
    //}

    for (int keyPartIndex = 0; keyPartIndex < keySize; ++keyPartIndex) {
        if (CompareRowValues(keyRange.first[keyPartIndex], leftBound[keyPartIndex]) < 0) {
            break;
        }
        leftBound[keyPartIndex] = keyRange.first[keyPartIndex];        
    }

    for (int keyPartIndex = 0; keyPartIndex < keySize; ++keyPartIndex) {
        if (CompareRowValues(keyRange.second[keyPartIndex], rightBound[keyPartIndex]) > 0) {
            break;
        }
        rightBound[keyPartIndex] = keyRange.second[keyPartIndex];        
    }

    return TKeyRange(TKey(leftBound), TKey(rightBound));
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

EValueType InferType(const TExpression* expr, const TTableSchema& sourceSchema)
{
    switch (expr->GetKind()) {
        case EExpressionKind::Literal:
            return EValueType(expr->As<TLiteralExpression>()->GetConstantValue().Type);
        case EExpressionKind::Reference:
            return sourceSchema.GetColumnOrThrow(expr->As<TReferenceExpression>()->GetColumnName()).Type;
        case EExpressionKind::Function: {
            auto* typedExpr = expr->As<TFunctionExpression>();

            Stroka functionName(typedExpr->GetFunctionName());
            functionName.to_lower();

            if (functionName == "if") {
                CHECK(typedExpr->GetArgumentCount() == 3);
                const TExpression* thenExpr = typedExpr->Arguments()[1];
                const TExpression* elseExpr = typedExpr->Arguments()[2];

                EValueType thenType = thenExpr->GetType(sourceSchema);
                EValueType elseType = elseExpr->GetType(sourceSchema);

                YCHECK(thenType == elseType);
                
                return thenType;
            } else if (functionName == "has_prefix") {
                CHECK(typedExpr->GetArgumentCount() == 2);
                const TExpression* lhsExpr = typedExpr->Arguments()[0];
                const TExpression* rhsExpr = typedExpr->Arguments()[1];

                YCHECK(lhsExpr->GetType(sourceSchema) == EValueType::String);
                YCHECK(rhsExpr->GetType(sourceSchema) == EValueType::String);

                return EValueType::Integer;
            }
            YUNIMPLEMENTED();
        }
            
        case EExpressionKind::BinaryOp: {
            auto* typedExpr = expr->As<TBinaryOpExpression>();
            auto lhsType = InferType(typedExpr->GetLhs(), sourceSchema);
            auto rhsType = InferType(typedExpr->GetRhs(), sourceSchema);
            if (lhsType != rhsType) {
                THROW_ERROR_EXCEPTION(
                    "Type mismatch between left- and right-hand sides in expression %s",
                    ~typedExpr->GetSource().Quote())
                    << TErrorAttribute("lhs_type", ToString(lhsType))
                    << TErrorAttribute("rhs_type", ToString(rhsType));
            }

            switch (lhsType) {
                case EValueType::Integer:
                    return EValueType::Integer;
                case EValueType::Double:
                    switch (typedExpr->GetOpcode()) {
                        case EBinaryOp::Plus:
                        case EBinaryOp::Minus:
                        case EBinaryOp::Multiply:
                        case EBinaryOp::Divide:
                            return EValueType::Double;
                        case EBinaryOp::Equal:
                        case EBinaryOp::NotEqual:
                        case EBinaryOp::Less:
                        case EBinaryOp::LessOrEqual:
                        case EBinaryOp::Greater:
                        case EBinaryOp::GreaterOrEqual:
                            return EValueType::Integer;
                        default:
                             THROW_ERROR_EXCEPTION(
                                "Expression %s is not supported",
                                ~typedExpr->GetSource().Quote())
                                << TErrorAttribute("lhs_type", ToString(lhsType))
                                << TErrorAttribute("rhs_type", ToString(rhsType));
                    }
                    break;
                case EValueType::String: {
                    switch (typedExpr->GetOpcode()) {
                        case EBinaryOp::Equal:
                        case EBinaryOp::NotEqual:
                        case EBinaryOp::Less:
                        case EBinaryOp::Greater:
                            return EValueType::Integer;
                        default:
                            THROW_ERROR_EXCEPTION(
                                "Expression %s is not supported",
                                ~typedExpr->GetSource().Quote())
                                << TErrorAttribute("lhs_type", ToString(lhsType))
                                << TErrorAttribute("rhs_type", ToString(rhsType));
                    }
                    break;   
                }                    
                default:
                    YUNREACHABLE();
            }
        }
    }
    YUNREACHABLE();
}

Stroka InferName(const TExpression* expr)
{
    switch (expr->GetKind()) {
        case EExpressionKind::Literal:
            return ToString(expr->As<TLiteralExpression>()->GetValue());
        case EExpressionKind::Reference:
            return expr->As<TReferenceExpression>()->GetColumnName();
        case EExpressionKind::Function: {
            auto* typedExpr = expr->As<TFunctionExpression>();
            Stroka result = typedExpr->GetFunctionName();
            result += "(";
            for (int i = 0; i < typedExpr->GetArgumentCount(); ++i) {
                if (i) {
                    result += ", ";
                }
                result += InferName(typedExpr->GetArgument(i));
            }
            result += ")";
            return result;
        }
        case EExpressionKind::BinaryOp: {
            auto* typedExpr = expr->As<TBinaryOpExpression>();
            auto canOmitParenthesis = [] (const TExpression* expr) {
                return
                    expr->GetKind() == EExpressionKind::Literal ||
                    expr->GetKind() == EExpressionKind::Reference ||
                    expr->GetKind() == EExpressionKind::Function;
            };
            auto lhsName = InferName(typedExpr->GetLhs());
            if (!canOmitParenthesis(typedExpr->GetLhs())) {
                lhsName = "(" + lhsName + ")";
            }
            auto rhsName = InferName(typedExpr->GetRhs());
            if (!canOmitParenthesis(typedExpr->GetRhs())) {
                rhsName = "(" + rhsName + ")";
            }
            return
                lhsName +
                " " + GetBinaryOpcodeLexeme(typedExpr->GetOpcode()) + " " +
                rhsName;
        }
    }
    YUNREACHABLE();
}

bool IsConstant(const TExpression* expr)
{
    switch (expr->GetKind()) {
        case EExpressionKind::Literal:
            return true;
        default:
            return false;
    }
}

TValue GetConstantValue(const TExpression* expr)
{
    switch (expr->GetKind()) {
        case EExpressionKind::Literal:
            return expr->As<TLiteralExpression>()->GetValue();
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

