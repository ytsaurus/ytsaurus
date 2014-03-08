#include "stdafx.h"
#include "plan_helpers.h"
#include "plan_node.h"

#include "private.h"
#include "helpers.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/name_table.h>

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
    typedef std::tuple<size_t, EBinaryOp, TValue> TConstraint;
    typedef SmallVector<TConstraint, 4> TConstraints;

    TKeyRange result = keyRange;
    TConstraints constraints;

    const size_t keySize = keyColumns.size();

    auto normalizeKey =
    [&] (TKey& key) {
        if (keySize == 0) {
            return;
        }
        if (key.GetCount() == 0) {
            TUnversionedOwningRowBuilder builder(keySize);
            for (size_t index = 0; index < keySize; ++index) {
                builder.AddValue(MakeUnversionedSentinelValue(EValueType::Min));
            }
            key = builder.GetRowAndReset();
        } else if (key.GetCount() == keySize + 1 && key[keySize].Type == EValueType::Min) {
            TUnversionedOwningRowBuilder builder(keySize);
            for (size_t index = 0; index < keySize; ++index) {
                builder.AddValue(key[index]);
            }
            key = builder.GetRowAndReset();
            AdvanceToValueSuccessor(key[keySize - 1]);
        }
    };

    normalizeKey(result.first);
    normalizeKey(result.second);

    // Computes key index for a given column name.
    auto columnNameToKeyPartIndex =
    [&] (const Stroka& columnName) -> size_t {
        for (size_t index = 0; index < keyColumns.size(); ++index) {
            if (keyColumns[index] == columnName) {
                return index;
            }
        }
        return -1;
    };

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
            constraints.push_back(std::make_tuple(
                columnNameToKeyPartIndex(referenceExpr->GetColumnName()),
                opcode,
                constantExpr->GetConstantValue()));
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

    // Sort all constraints according to the key columns.
    std::sort(
        constraints.begin(),
        constraints.end(),
        [&] (const TConstraint& lhs, const TConstraint& rhs) -> bool {
            return std::get<0>(lhs) < std::get<0>(rhs);
        });

    // Find a maximal equality prefix.
    size_t keyPartIndex = 0;
    size_t constraintIndex = 0;

    auto extendToRightWithMin = [] (TKey& key, int index) {
        for (++index; index < key.GetCount(); ++index) {
            key[index].Type = EValueType::Min;
        }
    };

    auto extendToRightWithMax = [] (TKey& key, int index) {
        for (++index; index < key.GetCount(); ++index) {
            key[index].Type = EValueType::Max;
        }
    };

    while (keyPartIndex < keyColumns.size() && constraintIndex < constraints.size()) {
        const auto& constraint = constraints[constraintIndex];

        auto& currentLeftBound = result.first[keyPartIndex];
        auto& currentRightBound = result.second[keyPartIndex];

        auto constraintOpcode = std::get<1>(constraint);
        auto constraintBound = std::get<2>(constraint);

        if (keyPartIndex < std::get<0>(constraint)) {
            // Lexicographical order makes it meaningful to consider only
            // key ranges (L1 ... Lk) -- (R1 ... Rk) that satisfy:
            // L1 == R1 && ... && Li == Ri && L(i+1) < R(i+1)
            // with all other components irrelevant.
            if (CompareRowValues(currentLeftBound, currentRightBound) == 0) {
                ++keyPartIndex;
                continue;
            } else {
                break;
            }
        }

        int leftTernaryCmp = CompareRowValues(currentLeftBound, constraintBound);
        int rightTernaryCmp = CompareRowValues(currentRightBound, constraintBound);

        switch (constraintOpcode) {

            case EBinaryOp::Equal:
                if (leftTernaryCmp < 0) {
                    currentLeftBound = constraintBound;
                    extendToRightWithMin(result.first, keyPartIndex);
                }
                if (rightTernaryCmp > 0) {
                    currentRightBound = constraintBound;
                    if (keyPartIndex + 1 < keyColumns.size()) {
                        extendToRightWithMax(result.second, keyPartIndex);
                    } else {
                        AdvanceToValueSuccessor(currentRightBound);
                    }
                }
                break;

            case EBinaryOp::NotEqual:
                if (leftTernaryCmp == 0) {
                    currentLeftBound = constraintBound;
                    AdvanceToValueSuccessor(currentLeftBound);
                    extendToRightWithMin(result.first, keyPartIndex);
                }
                if (rightTernaryCmp == 0) {
                    extendToRightWithMin(result.second, keyPartIndex);
                }
                break;

            case EBinaryOp::Less:
                if (rightTernaryCmp >= 0) {
                    currentRightBound = constraintBound;
                    extendToRightWithMin(result.second, keyPartIndex);
                }
                break;

            case EBinaryOp::LessOrEqual:
                if (rightTernaryCmp > 0) {
                    currentRightBound = constraintBound;
                    if (keyPartIndex + 1 < keyColumns.size()) {
                        extendToRightWithMax(result.second, keyPartIndex);
                    } else {
                        AdvanceToValueSuccessor(currentRightBound);
                    }
                }
                break;

            case EBinaryOp::Greater:
                if (leftTernaryCmp <= 0) {
                    currentLeftBound = constraintBound;
                    AdvanceToValueSuccessor(currentLeftBound);
                    extendToRightWithMin(result.first, keyPartIndex);
                }
                break;

            case EBinaryOp::GreaterOrEqual:
                if (leftTernaryCmp < 0) {
                    currentLeftBound = constraintBound;
                    extendToRightWithMin(result.first, keyPartIndex);
                }
                break;

            default:
                YUNREACHABLE();

        }

        ++constraintIndex;
    }

    return result;
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
        case EExpressionKind::IntegerLiteral:
            return EValueType::Integer;
        case EExpressionKind::DoubleLiteral:
            return EValueType::Double;
        case EExpressionKind::Reference:
            // For reference expression, always trust cached type.
            return sourceSchema.GetColumnOrThrow(expr->As<TReferenceExpression>()->GetName()).Type;
        case EExpressionKind::Function:
            YUNIMPLEMENTED();
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
            if (lhsType != EValueType::Integer && lhsType != EValueType::Double) {
                THROW_ERROR_EXCEPTION(
                    "Expression %s require either integral or floating-point operands",
                    ~typedExpr->GetSource().Quote())
                    << TErrorAttribute("lhs_type", ToString(lhsType))
                    << TErrorAttribute("rhs_type", ToString(rhsType));
            }
            switch (typedExpr->GetOpcode()) {
                // For arithmetic operations resulting type matches operands' type.
                case EBinaryOp::Plus:
                case EBinaryOp::Minus:
                case EBinaryOp::Multiply:
                case EBinaryOp::Divide:
                    return lhsType;
                // For integral and logical operations operands must be integral.
                case EBinaryOp::Modulo:
                case EBinaryOp::And:
                case EBinaryOp::Or:
                    if (lhsType != EValueType::Integer) {
                        THROW_ERROR_EXCEPTION(
                            "Operands must be integral in expression %s",
                            ~typedExpr->GetSource().Quote())
                            << TErrorAttribute("lhs_type", ToString(lhsType))
                            << TErrorAttribute("rhs_type", ToString(rhsType));
                    }
                    return EValueType::Integer;
                // For comparsion operations resulting type is integer type
                // because we do not have built-in boolean type, and thus
                // we represent comparsion result as 0/1.
                case EBinaryOp::Equal:
                case EBinaryOp::NotEqual:
                case EBinaryOp::Less:
                case EBinaryOp::LessOrEqual:
                case EBinaryOp::Greater:
                case EBinaryOp::GreaterOrEqual:
                    return EValueType::Integer;
            }
        }
    }
    YUNREACHABLE();
}

Stroka InferName(const TExpression* expr)
{
    switch (expr->GetKind()) {
        case EExpressionKind::IntegerLiteral:
            return ToString(expr->As<TIntegerLiteralExpression>()->GetValue());
        case EExpressionKind::DoubleLiteral:
            return ToString(expr->As<TDoubleLiteralExpression>()->GetValue());
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
                    expr->GetKind() == EExpressionKind::IntegerLiteral ||
                    expr->GetKind() == EExpressionKind::DoubleLiteral ||
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
        case EExpressionKind::IntegerLiteral:
        case EExpressionKind::DoubleLiteral:
            return true;
        default:
            return false;
    }
}

TValue GetConstantValue(const TExpression* expr)
{
    switch (expr->GetKind()) {
        case EExpressionKind::IntegerLiteral:
            return MakeUnversionedIntegerValue(
                expr->As<TIntegerLiteralExpression>()->GetValue(),
                NullTimestamp);
        case EExpressionKind::DoubleLiteral:
            return MakeUnversionedIntegerValue(
                expr->As<TDoubleLiteralExpression>()->GetValue(),
                NullTimestamp);
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

