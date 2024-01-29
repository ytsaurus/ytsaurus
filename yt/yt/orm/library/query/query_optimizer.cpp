#include "query_optimizer.h"

#include <yt/yt/library/query/base/helpers.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/hash.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

class TOptimizeResultCollector
{
public:
    void operator() (bool optimizeResult)
    {
        OptimizedAnything_ = OptimizedAnything_ || optimizeResult;
    }

    bool OptimizedAnything() &&
    {
        return OptimizedAnything_;
    }

private:
    bool OptimizedAnything_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TBaseOptimizer
{
protected:
    bool OptimizeQuery(TQuery* query)
    {
        TOptimizeResultCollector collector;
        if (query->GroupExprs) {
            collector(Optimize(query->GroupExprs->first));
        }

        collector(Optimize(query->SelectExprs));
        collector(Optimize(query->WherePredicate));
        collector(Optimize(query->HavingPredicate));
        collector(Optimize(query->OrderExpressions));
        return std::move(collector).OptimizedAnything();
    }

    virtual bool Optimize(TExpressionPtr& expr)
    {
        NQueryClient::CheckStackDepth();

        if (auto* typedExpr = expr->As<TLiteralExpression>()) {
            return Optimize(*typedExpr);
        } else if (auto* typedExpr = expr->As<TReferenceExpression>()) {
            return Optimize(*typedExpr);
        } else if (auto* typedExpr = expr->As<TAliasExpression>()) {
            return Optimize(*typedExpr);
        } else if (auto* typedExpr = expr->As<TFunctionExpression>()) {
            return Optimize(*typedExpr);
        } else if (auto* typedExpr = expr->As<TUnaryOpExpression>()) {
            return Optimize(*typedExpr);
        } else if (auto* typedExpr = expr->As<TBinaryOpExpression>()) {
            return Optimize(*typedExpr);
        } else if (auto* typedExpr = expr->As<TInExpression>()) {
            return Optimize(*typedExpr);
        } else if (auto* typedExpr = expr->As<TBetweenExpression>()) {
            return Optimize(*typedExpr);
        } else if (auto* typedExpr = expr->As<TTransformExpression>()) {
            return Optimize(*typedExpr);
        } else if (auto* typedExpr = expr->As<TCaseExpression>()) {
            return Optimize(*typedExpr);
        } else if (auto* typedExpr = expr->As<TLikeExpression>()) {
            return Optimize(*typedExpr);
        } else {
            THROW_ERROR_EXCEPTION("Unsupported expression %Qv in user query",
                FormatExpression(*expr));
        }
    }

    virtual bool Optimize(TExpressionList& list)
    {
        TOptimizeResultCollector collector;
        for (auto& expr : list) {
            collector(Optimize(expr));
        }

        return std::move(collector).OptimizedAnything();
    }

    virtual bool Optimize(TNullableExpressionList& list)
    {
        if (list) {
            return Optimize(*list);
        }

        return false;
    }

    virtual bool Optimize(TOrderExpressionList& list)
    {
        TOptimizeResultCollector collector;
        for (auto& expr : list) {
            collector(Optimize(expr.first));
        }

        return std::move(collector).OptimizedAnything();
    }

    virtual bool Optimize(TReferenceExpression& /*expression*/)
    {
        return false;
    }

    virtual bool Optimize(TLiteralExpression& /*expression*/)
    {
        return false;
    }

    virtual bool Optimize(TFunctionExpression& expression)
    {
        return Optimize(expression.Arguments);
    }

    virtual bool Optimize(TAliasExpression& expression)
    {
        return Optimize(expression.Expression);
    }

    virtual bool Optimize(TBinaryOpExpression& expression)
    {
        TOptimizeResultCollector collector;
        collector(Optimize(expression.Lhs));
        collector(Optimize(expression.Rhs));
        return std::move(collector).OptimizedAnything();
    }

    virtual bool Optimize(TUnaryOpExpression& expression)
    {
        return Optimize(expression.Operand);
    }

    virtual bool Optimize(TInExpression& expression)
    {
        return Optimize(expression.Expr);
    }

    virtual bool Optimize(TTransformExpression& expression)
    {
        TOptimizeResultCollector collector;
        collector(Optimize(expression.Expr));
        collector(Optimize(expression.DefaultExpr));
        return std::move(collector).OptimizedAnything();
    }

    virtual bool Optimize(TBetweenExpression& expression)
    {
        return Optimize(expression.Expr);
    }

    virtual bool Optimize(TWhenThenExpressionList& list)
    {
        TOptimizeResultCollector collector;
        for (auto& expr : list) {
            collector(Optimize(expr.first));
            collector(Optimize(expr.second));
        }

        return std::move(collector).OptimizedAnything();
    }

    virtual bool Optimize(TCaseExpression& expression)
    {
        TOptimizeResultCollector collector;
        collector(Optimize(expression.DefaultExpression));
        collector(Optimize(expression.OptionalOperand));
        collector(Optimize(expression.WhenThenExpressions));
        return std::move(collector).OptimizedAnything();
    }

    virtual bool Optimize(TLikeExpression& expression)
    {
        TOptimizeResultCollector collector;
        collector(Optimize(expression.Pattern));
        collector(Optimize(expression.Text));
        collector(Optimize(expression.EscapeCharacter));
        return std::move(collector).OptimizedAnything();
    }

    virtual bool IsOptimizationAllowed(const TExpressionPtr& expr) const
    {
        NQueryClient::CheckStackDepth();

        if (expr->As<TLiteralExpression>()) {
            return true;
        } else if (auto* typedExpr = expr->As<TReferenceExpression>()) {
            return IsOptimizationAllowed(*typedExpr);
        } else if (auto* typedExpr = expr->As<TAliasExpression>()) {
            return IsOptimizationAllowed(typedExpr->Expression);
        } else if (auto* typedExpr = expr->As<TFunctionExpression>()) {
            return IsOptimizationAllowed(*typedExpr);
        } else if (auto* typedExpr = expr->As<TUnaryOpExpression>()) {
            return IsOptimizationAllowed(typedExpr->Operand);
        } else if (auto* typedExpr = expr->As<TBinaryOpExpression>()) {
            return IsOptimizationAllowed(typedExpr->Lhs) && IsOptimizationAllowed(typedExpr->Rhs);
        } else if (auto* typedExpr = expr->As<TInExpression>()) {
            return IsOptimizationAllowed(typedExpr->Expr);
        } else if (auto* typedExpr = expr->As<TBetweenExpression>()) {
            return IsOptimizationAllowed(typedExpr->Expr);
        } else if (auto* typedExpr = expr->As<TTransformExpression>()) {
            return IsOptimizationAllowed(typedExpr->Expr) && IsOptimizationAllowed(typedExpr->DefaultExpr);
        } else if (auto* typedExpr = expr->As<TCaseExpression>()) {
            return IsOptimizationAllowed(*typedExpr);
        } else if (auto* typedExpr = expr->As<TLikeExpression>()) {
            return IsOptimizationAllowed(*typedExpr);
        } else {
            THROW_ERROR_EXCEPTION("Unsupported expression %Qv in user query",
                FormatExpression(*expr));
        }
    }

    virtual bool IsOptimizationAllowed(const TNullableExpressionList& list) const
    {
        return list ? IsOptimizationAllowed(*list) : true;
    }

    virtual bool IsOptimizationAllowed(const TExpressionList& list) const
    {
        for (const auto& expr : list) {
            if (!IsOptimizationAllowed(expr)) {
                return false;
            }
        }
        return true;
    }

    virtual bool IsOptimizationAllowed(const TOrderExpressionList& list) const
    {
        for (const auto& expr : list) {
            if (!IsOptimizationAllowed(expr.first)) {
                return false;
            }
        }
        return true;
    }

    virtual bool IsOptimizationAllowed(const TReferenceExpression& /*expression*/) const
    {
        return true;
    }

    virtual bool IsOptimizationAllowed(const TFunctionExpression& expression) const
    {
        return IsOptimizationAllowed(expression.Arguments);
    }

    virtual bool IsOptimizationAllowed(const TWhenThenExpressionList& list) const
    {
        for (auto& expr : list) {
            if (!IsOptimizationAllowed(expr.first) || !IsOptimizationAllowed(expr.second)) {
                return false;
            }
        }

        return true;
    }

    virtual bool IsOptimizationAllowed(const TCaseExpression& expression) const
    {
        return IsOptimizationAllowed(expression.DefaultExpression) &&
            IsOptimizationAllowed(expression.WhenThenExpressions) &&
            IsOptimizationAllowed(expression.OptionalOperand);
    }

    virtual bool IsOptimizationAllowed(const TLikeExpression& expression) const
    {
        return IsOptimizationAllowed(expression.EscapeCharacter) &&
            IsOptimizationAllowed(expression.Pattern) &&
            IsOptimizationAllowed(expression.Text);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJoinOptimizer
    : public TBaseOptimizer
{
public:
    explicit TJoinOptimizer(TQuery* query)
        : Query_(query)
    { }

    bool Run()
    {
        if (!CanOptimize()) {
            return false;
        }

        OptimizeQuery(Query_);
        Query_->Joins.clear();
        return true;
    }

private:
    TQuery* const Query_;
    THashMap<TReference, TReference, TReferenceHasher, TReferenceEqComparer> ColumnsMapping_;

    using TBaseOptimizer::IsOptimizationAllowed;
    using TBaseOptimizer::Optimize;

    bool CanOptimize()
    {
        if (Query_->Joins.size() != 1) {
            return false;
        }
        const auto* join = std::get_if<TJoin>(&Query_->Joins[0]);
        if (!join || join->IsLeft) {
            return false;
        }

        if (join->Predicate || !join->Fields.empty()) {
            return false;
        }
        YT_VERIFY(join->Lhs.size() == join->Rhs.size());

        for (size_t i = 0; i < join->Lhs.size(); ++i) {
            // TODO(bulatman) The right table keys may be replaced by transformed left table keys as well.
            auto* lhs = join->Lhs[i]->As<TReferenceExpression>();
            auto* rhs = join->Rhs[i]->As<TReferenceExpression>();
            if (!lhs || !rhs) {
                return false;
            }
            if (!ColumnsMapping_.try_emplace(rhs->Reference, lhs->Reference).second) {
                // Seems like syntax error in query, skip optimization.
                return false;
            }
        }
        if (Query_->GroupExprs) {
            if (!IsOptimizationAllowed(Query_->GroupExprs->first)) {
                return false;
            }
        }
        if (Query_->SelectExprs) {
            for (const auto& expr : *Query_->SelectExprs) {
                if (!IsOptimizationAllowed(Query_->SelectExprs)) {
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
        return IsOptimizationAllowed(Query_->WherePredicate)
            && IsOptimizationAllowed(Query_->HavingPredicate)
            && IsOptimizationAllowed(Query_->OrderExpressions);
    }

    bool IsOptimizationAllowed(const TReferenceExpression& expression) const override
    {
        return expression.Reference.TableName == Query_->Table.Alias || ColumnsMapping_.contains(expression.Reference);
    }

    bool Optimize(TReferenceExpression& expression) override
    {
        if (expression.Reference.TableName != Query_->Table.Alias) {
            expression.Reference = GetOrCrash(ColumnsMapping_, expression.Reference);
            return true;
        }

        return false;
    }
};

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

////////////////////////////////////////////////////////////////////////////////

class TTryGetStringOptimizer
    : public TBaseOptimizer
{
public:
    bool Run(TQuery* query)
    {
        return OptimizeQuery(query);
    }

private:
    using TBaseOptimizer::Optimize;

    bool Optimize(TFunctionExpression& expression) override
    {
        if (expression.FunctionName == "string" && expression.Arguments.size() == 1) {
            auto argumentFunction = expression.Arguments[0]->As<TFunctionExpression>();
            if (argumentFunction && argumentFunction->FunctionName == "try_get_string") {
                expression.FunctionName = "try_get_string";
                expression.Arguments = argumentFunction->Arguments;
                return true;
            }
        }

        return false;
    }
};

} // namespace

bool TryOptimizeJoin(TQuery* query)
{
    THROW_ERROR_EXCEPTION_UNLESS(query, "TryOptimizeJoin() received a nullptr query");
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

bool TryOptimizeTryGetString(TQuery* query)
{
    TTryGetStringOptimizer optimizer;
    YT_VERIFY(query);
    return optimizer.Run(query);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
