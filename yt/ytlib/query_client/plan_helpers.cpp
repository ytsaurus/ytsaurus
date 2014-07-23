#include "stdafx.h"
#include "plan_helpers.h"
#include "plan_node.h"
#include "key_trie.h"

#include "private.h"
#include "helpers.h"

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/unversioned_row.h>

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
    const TExpression* expr,
    const TKeyColumns& keyColumns,
    TRowBuffer* rowBuffer)
{
    if (auto* binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        auto opcode = binaryOpExpr->GetOpcode();
        auto* lhsExpr = binaryOpExpr->GetLhs();
        auto* rhsExpr = binaryOpExpr->GetRhs();

        if (opcode == EBinaryOp::And) {
            return IntersectKeyTrie(
                ExtractMultipleConstraints(lhsExpr, keyColumns, rowBuffer),
                ExtractMultipleConstraints(rhsExpr, keyColumns, rowBuffer),
                rowBuffer);
        } if (opcode == EBinaryOp::Or) {
            return UniteKeyTrie(
                ExtractMultipleConstraints(lhsExpr, keyColumns, rowBuffer),
                ExtractMultipleConstraints(rhsExpr, keyColumns, rowBuffer),
                rowBuffer);
        } else {
            if (rhsExpr->IsA<TReferenceExpression>()) {
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

            auto* referenceExpr = lhsExpr->As<TReferenceExpression>();
            auto* constantExpr = rhsExpr->IsConstant() ? rhsExpr : nullptr;

            TKeyTrieNode result;

            if (referenceExpr && constantExpr) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->GetColumnName());
                if (keyPartIndex >= 0) {
                    auto value = constantExpr->GetConstantValue();
                    switch (opcode) {
                        case EBinaryOp::Equal:
                            result.Offset = keyPartIndex;
                            result.Next[value] = TKeyTrieNode();
                            break;
                        case EBinaryOp::NotEqual:
                            result.Offset = keyPartIndex;

                            result.Bounds.push_back(MakeUnversionedSentinelValue(EValueType::Min));
                            result.Bounds.push_back(value);

                            result.Bounds.push_back(GetValueSuccessor(value, rowBuffer));
                            result.Bounds.push_back(MakeUnversionedSentinelValue(EValueType::Max));
                            
                            break;
                        case EBinaryOp::Less:
                            result.Offset = keyPartIndex;
                            result.Bounds.push_back(MakeUnversionedSentinelValue(EValueType::Min));
                            result.Bounds.push_back(value);

                            break;
                        case EBinaryOp::LessOrEqual:
                            result.Offset = keyPartIndex;
                            result.Bounds.push_back(MakeUnversionedSentinelValue(EValueType::Min));
                            result.Bounds.push_back(GetValueSuccessor(value, rowBuffer));

                            break;
                        case EBinaryOp::Greater:
                            result.Offset = keyPartIndex;
                            result.Bounds.push_back(GetValueSuccessor(value, rowBuffer));
                            result.Bounds.push_back(MakeUnversionedSentinelValue(EValueType::Max));

                            break;
                        case EBinaryOp::GreaterOrEqual:
                            result.Offset = keyPartIndex;
                            result.Bounds.push_back(value);
                            result.Bounds.push_back(MakeUnversionedSentinelValue(EValueType::Max));
                            break;
                        default:
                            break;
                    }
                }
            }

            return result;
        }
    } else if (auto* functionExpr = expr->As<TFunctionExpression>()) {
        Stroka functionName(functionExpr->GetFunctionName());
        functionName.to_lower();

        auto* lhsExpr = functionExpr->Arguments()[0];
        auto* rhsExpr = functionExpr->Arguments()[1];

        auto* referenceExpr = rhsExpr->As<TReferenceExpression>();
        auto* constantExpr = lhsExpr->IsConstant() ? lhsExpr : nullptr;

        TKeyTrieNode result;

        if (functionName == "is_prefix" && referenceExpr && constantExpr) {
            int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->GetColumnName());
            if (keyPartIndex >= 0) {
                auto value = constantExpr->GetConstantValue();

                YCHECK(value.Type == EValueType::String);

                result.Offset = keyPartIndex;
                result.Bounds.push_back(value);

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
                result.Bounds.push_back(value);
            }
        }

        return result;
    }

    return TKeyTrieNode();
};

TKeyRange RefineKeyRange(
    const TKeyColumns& keyColumns,
    const TKeyRange& keyRange,
    const TExpression* predicate)
{
    TRowBuffer rowBuffer;

    auto keyTrie = ExtractMultipleConstraints(
        predicate,
        keyColumns,
        &rowBuffer);

    std::vector<TKeyRange> result = 
        GetRangesFromTrieWithinRange(keyRange, &rowBuffer, keyColumns.size(), keyTrie);

    if (result.empty()) {
        return std::make_pair(EmptyKey(), EmptyKey());
    } else if (result.size() == 1) {
        return result[0];
    } else {
        return keyRange;
    }
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
                const TExpression* conditionExpr = typedExpr->Arguments()[0];
                const TExpression* thenExpr = typedExpr->Arguments()[1];
                const TExpression* elseExpr = typedExpr->Arguments()[2];

                EValueType conditionType = conditionExpr->GetType(sourceSchema);
                EValueType thenType = thenExpr->GetType(sourceSchema);
                EValueType elseType = elseExpr->GetType(sourceSchema);

                YCHECK(conditionType == EValueType::Int64);
                YCHECK(thenType == elseType);
                
                return thenType;
            } else if (functionName == "is_prefix") {
                CHECK(typedExpr->GetArgumentCount() == 2);
                const TExpression* lhsExpr = typedExpr->Arguments()[0];
                const TExpression* rhsExpr = typedExpr->Arguments()[1];

                YCHECK(lhsExpr->GetType(sourceSchema) == EValueType::String);
                YCHECK(rhsExpr->GetType(sourceSchema) == EValueType::String);

                return EValueType::Int64;
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
                case EValueType::Int64:
                    return EValueType::Int64;
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
                            return EValueType::Int64;
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
                            return EValueType::Int64;
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

