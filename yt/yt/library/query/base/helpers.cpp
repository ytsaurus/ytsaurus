#include "helpers.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

#ifdef _asan_enabled_
static const int MinimumStackFreeSpace = 128_KB;
#else
static const int MinimumStackFreeSpace = 16_KB;
#endif

////////////////////////////////////////////////////////////////////////////////

void CheckStackDepth()
{
    if (!NConcurrency::CheckFreeStackSpace(MinimumStackFreeSpace)) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::QueryExpressionDepthLimitExceeded,
            "Expression depth causes stack overflow");
    }
}

////////////////////////////////////////////////////////////////////////////////

NAst::TExpressionPtr BuildAndExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs)
{
    if (lhs && !rhs) {
        return lhs;
    }
    if (rhs && !lhs) {
        return rhs;
    }
    if (!lhs && !rhs) {
        return holder->New<NAst::TLiteralExpression>(TSourceLocation(), NAst::TLiteralValue(true));
    }
    return holder->New<NAst::TBinaryOpExpression>(
        TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        NAst::TExpressionList{std::move(lhs)},
        NAst::TExpressionList{std::move(rhs)});
}

NAst::TExpressionPtr BuildOrExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs)
{
    if (lhs && !rhs) {
        return lhs;
    }
    if (rhs && !lhs) {
        return rhs;
    }
    if (!lhs && !rhs) {
        return holder->New<NAst::TLiteralExpression>(TSourceLocation(), NAst::TLiteralValue(false));
    }
    return holder->New<NAst::TBinaryOpExpression>(
        TSourceLocation(),
        NQueryClient::EBinaryOp::Or,
        NAst::TExpressionList{std::move(lhs)},
        NAst::TExpressionList{std::move(rhs)});
}

NAst::TExpressionPtr BuildConcatenationExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs,
    const TString& separator)
{
    if (lhs && !rhs) {
        return lhs;
    }
    if (rhs && !lhs) {
        return rhs;
    }
    if (!lhs && !rhs) {
        return holder->New<NAst::TLiteralExpression>(TSourceLocation(), NAst::TLiteralValue(""));
    }

    auto keySeparator = holder->New<NAst::TLiteralExpression>(TSourceLocation(), separator);
    lhs = holder->New<NAst::TBinaryOpExpression>(
        TSourceLocation(),
        NQueryClient::EBinaryOp::Concatenate,
        NAst::TExpressionList{std::move(lhs)},
        NAst::TExpressionList{std::move(keySeparator)});

    return holder->New<NAst::TBinaryOpExpression>(
        TSourceLocation(),
        NQueryClient::EBinaryOp::Concatenate,
        NAst::TExpressionList{std::move(lhs)},
        NAst::TExpressionList{std::move(rhs)});
}

NAst::TExpressionPtr BuildBinaryOperationTree(
    TObjectsHolder* context,
    std::vector<NAst::TExpressionPtr> leaves,
    EBinaryOp opCode)
{
    if (leaves.empty()) {
        return nullptr;
    }
    if (leaves.size() == 1) {
        return leaves[0];
    }
    std::deque<NAst::TExpressionPtr> expressionQueue(leaves.begin(), leaves.end());
    while (expressionQueue.size() != 1) {
        auto lhs = expressionQueue.front();
        expressionQueue.pop_front();
        auto rhs = expressionQueue.front();
        expressionQueue.pop_front();
        expressionQueue.emplace_back(context->New<NAst::TBinaryOpExpression>(
            NQueryClient::NullSourceLocation,
            opCode,
            NAst::TExpressionList{std::move(lhs)},
            NAst::TExpressionList{std::move(rhs)}));
    }
    return expressionQueue.front();
}

////////////////////////////////////////////////////////////////////////////////

void FillExpressionConstraintSignature(
    TMutableRange<EConstraintKind> signature,
    TConstExpressionPtr expression,
    const TKeyColumns& keyColumns)
{
    YT_ASSERT(signature);

    YT_ASSERT(signature.size() == keyColumns.size());

    if (const auto* binaryOpExpr = expression->As<TBinaryOpExpression>()) {
        auto opcode = binaryOpExpr->Opcode;
        auto lhsExpr = binaryOpExpr->Lhs;
        auto rhsExpr = binaryOpExpr->Rhs;

        if (opcode == EBinaryOp::And) {
            FillExpressionConstraintSignature(signature, lhsExpr, keyColumns);
            auto rhsSignature = GetExpressionConstraintSignature(rhsExpr, keyColumns);

            for (int index = 0; index < std::ssize(keyColumns); ++index) {
                signature[index] = std::max(signature[index], rhsSignature[index]);
            }
        } else if (opcode == EBinaryOp::Or) {
            FillExpressionConstraintSignature(signature, lhsExpr, keyColumns);
            auto rhsSignature = GetExpressionConstraintSignature(rhsExpr, keyColumns);

            for (int index = 0; index < std::ssize(keyColumns); ++index) {
                signature[index] = std::min(signature[index], rhsSignature[index]);
            }
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
                    switch (opcode) {
                        case EBinaryOp::Equal:
                            signature[keyPartIndex] = EConstraintKind::Exact;
                            break;
                        case EBinaryOp::Less:
                        case EBinaryOp::LessOrEqual:
                        case EBinaryOp::Greater:
                        case EBinaryOp::GreaterOrEqual:
                            signature[keyPartIndex] = EConstraintKind::Range;
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    } else if (const auto* inExpr = expression->As<TInExpression>()) {
        for (const auto& argument : inExpr->Arguments) {
            const auto* reference = argument->As<TReferenceExpression>();
            if (!reference) {
                continue;
            }

            auto keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, reference->ColumnName);
            if (keyPartIndex < 0) {
                continue;
            }

            signature[keyPartIndex] = EConstraintKind::Exact;
        }
    } else if (const auto* betweenExpr = expression->As<TBetweenExpression>()) {
        const auto& expressions = betweenExpr->Arguments;

        const auto* referenceExpr = expressions.front()->As<TReferenceExpression>();
        if (!referenceExpr) {
            return;
        }

        int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
        if (keyPartIndex < 0) {
            return;
        }

        signature[keyPartIndex] = EConstraintKind::Range;
    } else if (const auto* literalExpr = expression->As<TLiteralExpression>()) {
        const auto& value = TUnversionedValue(literalExpr->Value);
        if (value.Type == EValueType::Boolean) {
            if (value.Data.Boolean) {
                for (auto& signaturePart : signature) {
                    signaturePart = EConstraintKind::None;
                }
            } else {
                for (auto& signaturePart : signature) {
                    signaturePart = EConstraintKind::Exact;
                }
            }
        }
    }
}

std::vector<EConstraintKind> GetExpressionConstraintSignature(
    const TConstExpressionPtr& expression,
    const TKeyColumns& keyColumns)
{
    if (keyColumns.empty()) {
        return {};
    }

    auto signature = std::vector(keyColumns.size(), EConstraintKind::None);

    if (!expression) {
        return signature;
    }

    FillExpressionConstraintSignature(signature, expression, keyColumns);

    return signature;
}

int GetConstraintSignatureScore(const std::vector<EConstraintKind>& signature)
{
    int score = 0;
    for (const auto& kind : signature) {
        if (kind == EConstraintKind::Exact) {
            score += 2;
            continue;
        } else if (kind == EConstraintKind::Range) {
            score += 1;
        }
        break;
    }

    return score;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
