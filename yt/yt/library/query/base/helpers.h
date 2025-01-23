#pragma once

#include "ast.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void CheckStackDepth();

////////////////////////////////////////////////////////////////////////////////

NAst::TExpressionPtr BuildAndExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs);

NAst::TExpressionPtr BuildOrExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs);

NAst::TExpressionPtr BuildConcatenationExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs,
    const TString& separator);

//! For commutative operations only.
NAst::TExpressionPtr BuildBinaryOperationTree(
    TObjectsHolder* holder,
    std::vector<NAst::TExpressionPtr> leaves,
    EBinaryOp opCode);

////////////////////////////////////////////////////////////////////////////////

std::vector<EConstraintKind> GetExpressionConstraintSignature(
    const TConstExpressionPtr& expression,
    const NTableClient::TKeyColumns& keyColumns);

int GetConstraintSignatureScore(const std::vector<EConstraintKind>& signature);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
