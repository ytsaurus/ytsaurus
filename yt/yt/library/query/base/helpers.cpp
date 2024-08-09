#include "helpers.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NQueryClient {

using namespace NYT;

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

} // namespace NYT::NQueryClient
