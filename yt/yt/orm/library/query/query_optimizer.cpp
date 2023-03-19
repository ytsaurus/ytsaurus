#include "query_optimizer.h"

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/hash.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

class TJoinOptimizer
{
public:
    TJoinOptimizer(TQuery* query)
        : Query_(query)
    { }

    bool Run();

private:
    TQuery* Query_;
    THashMap<TReference, TReference> ColumnsMapping_;

    bool CanOptimize();

    bool Analyze(const TExpressionPtr& expr) const;
    bool Analyze(const TExpressionList& list) const;
    bool Analyze(const TNullableExpressionList& list) const;
    bool Analyze(const TOrderExpressionList& list) const;

    void Fix(TExpressionPtr& expr);
    void Fix(TExpressionList& list);
    void Fix(TNullableExpressionList& list);
    void Fix(TOrderExpressionList& list);
};

bool TJoinOptimizer::Run()
{
    if (!CanOptimize()) {
        return false;
    }

    if (Query_->GroupExprs) {
        Fix(Query_->GroupExprs->first);
    }
    Fix(Query_->SelectExprs);
    Fix(Query_->WherePredicate);
    Fix(Query_->HavingPredicate);
    Fix(Query_->OrderExpressions);
    Query_->Joins.clear();
    return true;
}

bool TJoinOptimizer::CanOptimize()
{
    if (1 != Query_->Joins.size() || Query_->Joins[0].IsLeft) {
        return false;
    }
    const auto& join = Query_->Joins[0];

    if (join.Predicate || !join.Fields.empty()) {
        return false;
    }
    YT_VERIFY(join.Lhs.size() == join.Rhs.size());

    for (size_t i = 0; i < join.Lhs.size(); ++i) {
        // TODO(bulatman) The right table keys may be replaced by transformed left table keys as well.
        auto* lhs = join.Lhs[i]->As<TReferenceExpression>();
        auto* rhs = join.Rhs[i]->As<TReferenceExpression>();
        if (!lhs || !rhs) {
            return false;
        }
        if (!ColumnsMapping_.try_emplace(rhs->Reference, lhs->Reference).second) {
            // Seems like syntax error in query, skip optimization.
            return false;
        }
    }
    if (Query_->GroupExprs) {
        if (!Analyze(Query_->GroupExprs->first)) {
            return false;
        }
    }
    if (Query_->SelectExprs) {
        for (const auto& expr : *Query_->SelectExprs) {
            if (!Analyze(Query_->SelectExprs)) {
                return false;
            }
            if (const auto* typeExpr = expr->As<TAliasExpression>()) {
                // Add virtual references to aliases in select expressions.
                TReference reference(typeExpr->Name);
                if (!ColumnsMapping_.try_emplace(reference, reference).second) {
                    // Seems like syntax error in query, skip optimization.
                    return false;
                }
            }
        }
    }
    return Analyze(Query_->WherePredicate)
        && Analyze(Query_->HavingPredicate)
        && Analyze(Query_->OrderExpressions);
}

bool TJoinOptimizer::Analyze(const TExpressionPtr& expr) const
{
    if (expr->As<TLiteralExpression>()) {
        return true;
    } else if (auto* typedExpr = expr->As<TReferenceExpression>()) {
        return typedExpr->Reference.TableName == Query_->Table.Alias || ColumnsMapping_.contains(typedExpr->Reference);
    } else if (auto* typedExpr = expr->As<TAliasExpression>()) {
        return Analyze(typedExpr->Expression);
    } else if (auto* typedExpr = expr->As<TFunctionExpression>()) {
        return Analyze(typedExpr->Arguments);
    } else if (auto* typedExpr = expr->As<TUnaryOpExpression>()) {
        return Analyze(typedExpr->Operand);
    } else if (auto* typedExpr = expr->As<TBinaryOpExpression>()) {
        return Analyze(typedExpr->Lhs) && Analyze(typedExpr->Rhs);
    } else if (auto* typedExpr = expr->As<TInExpression>()) {
        return Analyze(typedExpr->Expr);
    } else if (auto* typedExpr = expr->As<TBetweenExpression>()) {
        return Analyze(typedExpr->Expr);
    } else if (auto* typedExpr = expr->As<TTransformExpression>()) {
        return Analyze(typedExpr->Expr) && Analyze(typedExpr->DefaultExpr);
    } else {
        YT_ABORT();
    }
}

bool TJoinOptimizer::Analyze(const TNullableExpressionList& list) const
{
    return list ? Analyze(*list) : true;
}

bool TJoinOptimizer::Analyze(const TExpressionList& list) const
{
    for (const auto& expr : list) {
        if (!Analyze(expr)) {
            return false;
        }
    }
    return true;
}

bool TJoinOptimizer::Analyze(const TOrderExpressionList& list) const
{
    for (const auto& expr : list) {
        if (!Analyze(expr.first)) {
            return false;
        }
    }
    return true;
}

void TJoinOptimizer::Fix(TExpressionPtr& expr)
{
    if (expr->As<TLiteralExpression>()) {
        // Do nothing.
    } else if (auto* typedExpr = expr->As<TReferenceExpression>()) {
        if (typedExpr->Reference.TableName != Query_->Table.Alias) {
            typedExpr->Reference = GetOrCrash(ColumnsMapping_, typedExpr->Reference);
        }
    } else if (auto* typedExpr = expr->As<TAliasExpression>()) {
        Fix(typedExpr->Expression);
    } else if (auto* typedExpr = expr->As<TFunctionExpression>()) {
        Fix(typedExpr->Arguments);
    } else if (auto* typedExpr = expr->As<TUnaryOpExpression>()) {
        Fix(typedExpr->Operand);
    } else if (auto* typedExpr = expr->As<TBinaryOpExpression>()) {
        Fix(typedExpr->Lhs);
        Fix(typedExpr->Rhs);
    } else if (auto* typedExpr = expr->As<TInExpression>()) {
        Fix(typedExpr->Expr);
    } else if (auto* typedExpr = expr->As<TBetweenExpression>()) {
        Fix(typedExpr->Expr);
    } else if (auto* typedExpr = expr->As<TTransformExpression>()) {
        Fix(typedExpr->Expr);
        Fix(typedExpr->DefaultExpr);
    } else {
        YT_ABORT();
    }
}

void TJoinOptimizer::Fix(TExpressionList& list)
{
    for (auto& expr : list) {
        Fix(expr);
    }
}

void TJoinOptimizer::Fix(TNullableExpressionList& list)
{
    if (list) {
        Fix(*list);
    }
}

void TJoinOptimizer::Fix(TOrderExpressionList& list)
{
    for (auto& expr : list) {
        Fix(expr.first);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TGroupByOptimizer
{
public:
    TGroupByOptimizer(const std::vector<TString>& prefixReferences, const TString& tableName)
        : PrefixReferences_(prefixReferences)
        , TableName_(tableName)
    { }

    bool Run(TExpressionPtr expr)
    {
        // Composite prefix references are currently not supported.
        if (PrefixReferences_.size() != 1) {
            return false;
        }
        return InferPrefixRanges(expr) == 1;
    }

private:
    const std::vector<TString>& PrefixReferences_;
    const TString& TableName_;

    bool IsTargetReference(const TExpressionList& exprs)
    {
        if (exprs.size() != 1) {
            return false;
        }
        if (auto* refExpr = exprs[0]->As<TReferenceExpression>()) {
            return refExpr->Reference.ColumnName == PrefixReferences_[0] &&
                refExpr->Reference.TableName == TableName_;
        }
        return false;
    }

    // 0 -- unbound prefix.
    std::optional<size_t> InferPrefixRanges(TExpressionPtr expr)
    {
        if (auto* binaryExpr = expr->As<TBinaryOpExpression>()) {
            if (IsLogicalBinaryOp(binaryExpr->Opcode)) {
                YT_VERIFY(binaryExpr->Lhs.size() == 1);
                YT_VERIFY(binaryExpr->Rhs.size() == 1);
                auto prefixRangesLeft = InferPrefixRanges(binaryExpr->Lhs[0]);
                auto prefixRangesRight = InferPrefixRanges(binaryExpr->Rhs[0]);
                if (!prefixRangesLeft || !prefixRangesRight) {
                    return std::max(prefixRangesLeft, prefixRangesRight);
                }
                if (binaryExpr->Opcode == NQueryClient::EBinaryOp::Or) {
                    return (*prefixRangesLeft == 0 || *prefixRangesRight == 0)
                        ? 0
                        : *prefixRangesLeft + *prefixRangesRight;
                } else {
                    return (*prefixRangesLeft == 0 || *prefixRangesRight == 0)
                        ? std::max(prefixRangesLeft, prefixRangesRight)
                        : std::min(prefixRangesLeft, prefixRangesRight);
                }
            } else if (NQueryClient::IsRelationalBinaryOp(binaryExpr->Opcode)) {
                if (IsTargetReference(binaryExpr->Lhs) || IsTargetReference(binaryExpr->Rhs)) {
                    // 1 for equals operator, 0 for others.
                    return binaryExpr->Opcode == NQueryClient::EBinaryOp::Equal;
                }
            }
        } else if (auto* typedExpr = expr->As<TInExpression>()) {
            if (IsTargetReference(typedExpr->Expr)) {
                return typedExpr->Values.size();
            }
        } else if (auto* typedExpr = expr->As<TBetweenExpression>()) {
            if (IsTargetReference(typedExpr->Expr)) {
                return 0;
            }
        } else if (auto* typedExpr = expr->As<TUnaryOpExpression>()) {
            if (typedExpr->Opcode == NQueryClient::EUnaryOp::Not && IsTargetReference(typedExpr->Operand)) {
                YT_VERIFY(typedExpr->Operand.size() == 1);
                auto prefixRanges = InferPrefixRanges(typedExpr->Operand[0]);
                return std::min(prefixRanges, std::make_optional<size_t>(0));
            }
        }
        return std::nullopt;
    }
};

} // namespace

bool TryOptimizeJoin(TQuery* query)
{
    TJoinOptimizer optimizer(query);
    return optimizer.Run();
}

bool TryOptimizeGroupByWithUniquePrefix(
    TExpressionPtr filterExpression,
    const std::vector<TString>& prefixReferences,
    const TString& tableName)
{
    TGroupByOptimizer optimizer(prefixReferences, tableName);

    return optimizer.Run(filterExpression);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
