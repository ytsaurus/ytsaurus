#include "query_optimizer.h"

#include <yt/yt/library/query/base/ast_visitors.h>
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
    : public TBaseAstVisitor<bool, TBaseOptimizer>
{
public:
    bool OptimizeQuery(TQuery* query)
    {
        TOptimizeResultCollector collector;
        if (query->GroupExprs) {
            collector(Visit(*query->GroupExprs));
        }

        collector(Visit(query->SelectExprs));
        collector(Visit(query->WherePredicate));
        collector(Visit(query->HavingPredicate));
        collector(Visit(query->OrderExpressions));
        return std::move(collector).OptimizedAnything();
    }

    virtual bool OnLiteral(TLiteralExpressionPtr /*literalExpr*/)
    {
        return false;
    }

    virtual bool OnReference(TReferenceExpressionPtr /*referenceExpr*/)
    {
        return false;
    }

    virtual bool OnAlias(TAliasExpressionPtr aliasExpr)
    {
        return Visit(aliasExpr->Expression);
    }

    virtual bool OnUnary(TUnaryOpExpressionPtr unaryExpr)
    {
        return Visit(unaryExpr->Operand);
    }

    virtual bool OnBinary(TBinaryOpExpressionPtr binaryExpr)
    {
        TOptimizeResultCollector collector;
        collector(Visit(binaryExpr->Lhs));
        collector(Visit(binaryExpr->Rhs));
        return std::move(collector).OptimizedAnything();
    }

    virtual bool OnFunction(TFunctionExpressionPtr functionExpr)
    {
        return Visit(functionExpr->Arguments);
    }

    virtual bool OnIn(TInExpressionPtr inExpr)
    {
        return Visit(inExpr->Expr);
    }

    virtual bool OnBetween(TBetweenExpressionPtr betweenExpr)
    {
        return Visit(betweenExpr->Expr);
    }

    virtual bool OnTransform(TTransformExpressionPtr transformExpr)
    {
        TOptimizeResultCollector collector;
        collector(Visit(transformExpr->Expr));
        collector(Visit(transformExpr->DefaultExpr));
        return std::move(collector).OptimizedAnything();
    }

    virtual bool OnCase(TCaseExpressionPtr caseExpr)
    {
        TOptimizeResultCollector collector;
        collector(Visit(caseExpr->DefaultExpression));
        collector(Visit(caseExpr->OptionalOperand));
        collector(Visit(caseExpr->WhenThenExpressions));
        return std::move(collector).OptimizedAnything();
    }

    virtual bool OnLike(TLikeExpressionPtr likeExpr)
    {
        TOptimizeResultCollector collector;
        collector(Visit(likeExpr->Pattern));
        collector(Visit(likeExpr->Text));
        collector(Visit(likeExpr->EscapeCharacter));
        return std::move(collector).OptimizedAnything();
    }

protected:
    using TBaseAstVisitor<bool, TBaseOptimizer>::Visit;

    bool Visit(TExpressionList& list)
    {
        TOptimizeResultCollector collector;
        for (auto& expr : list) {
            collector(Visit(expr));
        }

        return std::move(collector).OptimizedAnything();
    }

    bool Visit(TNullableExpressionList& list)
    {
        return list ? Visit(*list) : false;
    }

    bool Visit(TOrderExpressionList& list)
    {
        TOptimizeResultCollector collector;
        for (auto& expr : list) {
            collector(Visit(expr.first));
        }

        return std::move(collector).OptimizedAnything();
    }

    bool Visit(TWhenThenExpressionList& list)
    {
        TOptimizeResultCollector collector;
        for (auto& expr : list) {
            collector(Visit(expr.first));
            collector(Visit(expr.second));
        }

        return std::move(collector).OptimizedAnything();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBaseOptimizationChecker
    : public TBaseAstVisitor<bool, TBaseOptimizationChecker>
{
public:
    virtual bool OnLiteral(TLiteralExpressionPtr /*literalExpr*/)
    {
        return true;
    }

    virtual bool OnReference(TReferenceExpressionPtr /*referenceExpr*/)
    {
        return true;
    }

    virtual bool OnAlias(TAliasExpressionPtr aliasExpr)
    {
        return Visit(aliasExpr->Expression);
    }

    virtual bool OnUnary(TUnaryOpExpressionPtr unaryExpr)
    {
        return Visit(unaryExpr->Operand);
    }

    virtual bool OnBinary(TBinaryOpExpressionPtr binaryExpr)
    {
        return Visit(binaryExpr->Lhs) && Visit(binaryExpr->Rhs);
    }

    virtual bool OnFunction(TFunctionExpressionPtr functionExpr)
    {
        return Visit(functionExpr->Arguments);
    }

    virtual bool OnIn(TInExpressionPtr inExpr)
    {
        return Visit(inExpr->Expr);
    }

    virtual bool OnBetween(TBetweenExpressionPtr betweenExpr)
    {
        return Visit(betweenExpr->Expr);
    }

    virtual bool OnTransform(TTransformExpressionPtr transformExpr)
    {
        return Visit(transformExpr->Expr) && Visit(transformExpr->DefaultExpr);
    }

    virtual bool OnCase(TCaseExpressionPtr caseExpr)
    {
        return Visit(caseExpr->DefaultExpression) &&
            Visit(caseExpr->WhenThenExpressions) &&
            Visit(caseExpr->OptionalOperand);
    }

    virtual bool OnLike(TLikeExpressionPtr likeExpr)
    {
        return Visit(likeExpr->EscapeCharacter) &&
            Visit(likeExpr->Pattern) &&
            Visit(likeExpr->Text);
    }

protected:
    using TBaseAstVisitor<bool, TBaseOptimizationChecker>::Visit;

    bool Visit(const TNullableExpressionList& list)
    {
        return list ? Visit(*list) : true;
    }

    bool Visit(const TExpressionList& list)
    {
        for (const auto& expr : list) {
            if (!Visit(expr)) {
                return false;
            }
        }
        return true;
    }

    bool Visit(const TOrderExpressionList& list)
    {
        for (const auto& expr : list) {
            if (!Visit(expr.first)) {
                return false;
            }
        }
        return true;
    }

    bool Visit(const TWhenThenExpressionList& list)
    {
        for (auto& expr : list) {
            if (!Visit(expr.first) || !Visit(expr.second)) {
                return false;
            }
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJoinOptimizationChecker
    : public TBaseOptimizationChecker
{
public:
    explicit TJoinOptimizationChecker(
        TQuery* query,
        THashMap<TReference, TReference, TReferenceHasher, TReferenceEqComparer>& columnsMapping)
        : Query_(query)
        , ColumnsMapping_(columnsMapping)
    { }


    bool OnReference(TReferenceExpressionPtr expression) override
    {
        return expression->Reference.TableName == Query_->Table.Alias || ColumnsMapping_.contains(expression->Reference);
    }

    bool IsOptimizationAllowed(const TNullableExpressionList& list)
    {
        return Visit(list);
    }

    bool IsOptimizationAllowed(const TExpressionList& list)
    {
        return Visit(list);
    }

    bool IsOptimizationAllowed(const TOrderExpressionList& list)
    {
        return Visit(list);
    }

    bool IsOptimizationAllowed(const TWhenThenExpressionList& list)
    {
        return Visit(list);
    }

private:
    TQuery* const Query_;
    THashMap<TReference, TReference, TReferenceHasher, TReferenceEqComparer>& ColumnsMapping_;

    using TBaseOptimizationChecker::Visit;
};

////////////////////////////////////////////////////////////////////////////////

class TJoinOptimizer
    : public TBaseOptimizer
{
public:

    explicit TJoinOptimizer(TQuery* query)
        : Query_(query)
        , OptimizationChecker_(Query_, ColumnsMapping_)
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

    bool OnReference(TReferenceExpressionPtr expression) override
    {
        if (expression->Reference.TableName != Query_->Table.Alias) {
            expression->Reference = GetOrCrash(ColumnsMapping_, expression->Reference);
            return true;
        }

        return false;
    }

private:
    TQuery* const Query_;
    THashMap<TReference, TReference, TReferenceHasher, TReferenceEqComparer> ColumnsMapping_;
    TJoinOptimizationChecker OptimizationChecker_;

    using TBaseOptimizer::Visit;

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
            if (!OptimizationChecker_.IsOptimizationAllowed(*Query_->GroupExprs)) {
                return false;
            }
        }
        if (Query_->SelectExprs) {
            for (const auto& expr : *Query_->SelectExprs) {
                if (!OptimizationChecker_.IsOptimizationAllowed(Query_->SelectExprs)) {
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
        return OptimizationChecker_.IsOptimizationAllowed(Query_->WherePredicate) &&
            OptimizationChecker_.IsOptimizationAllowed(Query_->HavingPredicate) &&
            OptimizationChecker_.IsOptimizationAllowed(Query_->OrderExpressions);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGroupByOptimizer
    : public TBaseAstVisitor<std::optional<size_t>, TGroupByOptimizer>
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
        return Visit(expr) == 1;
    }

    std::optional<size_t> OnLiteral(TLiteralExpressionPtr /*literalExpr*/)
    {
        return std::nullopt;
    }

    std::optional<size_t> OnReference(TReferenceExpressionPtr /*referenceExpr*/)
    {
        return std::nullopt;
    }

    std::optional<size_t> OnAlias(TAliasExpressionPtr /*aliasExpr*/)
    {
        return std::nullopt;
    }

    std::optional<size_t> OnUnary(TUnaryOpExpressionPtr unaryExpr)
    {
        if (unaryExpr->Opcode == NQueryClient::EUnaryOp::Not && IsTargetReference(unaryExpr->Operand)) {
            YT_VERIFY(unaryExpr->Operand.size() == 1);
            auto prefixRanges = Visit(unaryExpr->Operand[0]);
            return std::min(prefixRanges, std::make_optional<size_t>(0));
        }
        return std::nullopt;
    }

    std::optional<size_t> OnBinary(TBinaryOpExpressionPtr binaryExpr)
    {
        if (IsLogicalBinaryOp(binaryExpr->Opcode)) {
            YT_VERIFY(binaryExpr->Lhs.size() == 1);
            YT_VERIFY(binaryExpr->Rhs.size() == 1);
            auto prefixRangesLeft = Visit(binaryExpr->Lhs[0]);
            auto prefixRangesRight = Visit(binaryExpr->Rhs[0]);
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
        return std::nullopt;
    }

    std::optional<size_t> OnFunction(TFunctionExpressionPtr /*functionExpr*/)
    {
        return std::nullopt;
    }

    std::optional<size_t> OnIn(TInExpressionPtr inExpr)
    {
        if (IsTargetReference(inExpr->Expr)) {
            return inExpr->Values.size();
        }
        return std::nullopt;
    }

    std::optional<size_t> OnBetween(TBetweenExpressionPtr betweenExpr)
    {
        if (IsTargetReference(betweenExpr->Expr)) {
            return 0;
        }
        return std::nullopt;
    }

    std::optional<size_t> OnTransform(TTransformExpressionPtr /*transformExpr*/)
    {
        return std::nullopt;
    }

    std::optional<size_t> OnCase(TCaseExpressionPtr /*caseExpr*/)
    {
        return std::nullopt;
    }

    std::optional<size_t> OnLike(TLikeExpressionPtr /*likeExpr*/)
    {
        return std::nullopt;
    }

private:
    const std::vector<TString>& PrefixReferences_;
    const TString& TableName_;

    using TBaseAstVisitor<std::optional<size_t>, TGroupByOptimizer>::Visit;

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
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
