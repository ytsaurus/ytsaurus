#include "query_optimizer.h"

#include "misc.h"

#include <yt/yt/orm/library/attributes/helpers.h>

#include <yt/yt/library/query/base/ast_visitors.h>
#include <yt/yt/library/query/base/helpers.h>

#include <library/cpp/yt/assert/assert.h>
#include <library/cpp/yt/misc/variant.h>

#include <util/generic/hash.h>

#include <algorithm>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

class TBaseOptimizer
    : public TBaseAstVisitor<bool, TBaseOptimizer>
{
public:
    bool OptimizeQuery(TQuery* query)
    {
        NAttributes::TBooleanOrCollector collector;
        if (query->GroupExprs) {
            collector(Visit(*query->GroupExprs));
        }

        collector(Visit(query->SelectExprs));
        collector(Visit(query->WherePredicate));
        collector(Visit(query->HavingPredicate));
        collector(Visit(query->OrderExpressions));
        return std::move(collector).Result();
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
        NAttributes::TBooleanOrCollector collector;
        collector(Visit(binaryExpr->Lhs));
        collector(Visit(binaryExpr->Rhs));
        return std::move(collector).Result();
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
        NAttributes::TBooleanOrCollector collector;
        collector(Visit(transformExpr->Expr));
        collector(Visit(transformExpr->DefaultExpr));
        return std::move(collector).Result();
    }

    virtual bool OnCase(TCaseExpressionPtr caseExpr)
    {
        NAttributes::TBooleanOrCollector collector;
        collector(Visit(caseExpr->DefaultExpression));
        collector(Visit(caseExpr->OptionalOperand));
        collector(Visit(caseExpr->WhenThenExpressions));
        return std::move(collector).Result();
    }

    virtual bool OnLike(TLikeExpressionPtr likeExpr)
    {
        NAttributes::TBooleanOrCollector collector;
        collector(Visit(likeExpr->Pattern));
        collector(Visit(likeExpr->Text));
        collector(Visit(likeExpr->EscapeCharacter));
        return std::move(collector).Result();
    }

protected:
    using TBaseAstVisitor<bool, TBaseOptimizer>::Visit;

    bool Visit(TExpressionList& list)
    {
        NAttributes::TBooleanOrCollector collector;
        for (auto& expr : list) {
            collector(Visit(expr));
        }

        return std::move(collector).Result();
    }

    bool Visit(TNullableExpressionList& list)
    {
        return list ? Visit(*list) : false;
    }

    bool Visit(TOrderExpressionList& list)
    {
        NAttributes::TBooleanOrCollector collector;
        for (auto& expr : list) {
            collector(Visit(expr.Expressions));
        }

        return std::move(collector).Result();
    }

    bool Visit(TWhenThenExpressionList& list)
    {
        NAttributes::TBooleanOrCollector collector;
        for (auto& expr : list) {
            collector(Visit(expr.Condition));
            collector(Visit(expr.Result));
        }

        return std::move(collector).Result();
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
            if (!Visit(expr.Expressions)) {
                return false;
            }
        }
        return true;
    }

    bool Visit(const TWhenThenExpressionList& list)
    {
        for (auto& expr : list) {
            if (!Visit(expr.Condition) || !Visit(expr.Result)) {
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
        return NYT::Visit(Query_->FromClause,
            [&] (const TTableDescriptor& table) {
                return expression->Reference.TableName == table.Alias ||
                    ColumnsMapping_.contains(expression->Reference);
            },
            [&] (const TQueryAstHeadPtr& /*subquery*/) -> bool {
                THROW_ERROR_EXCEPTION("Subqueries are not supported yet");
            });
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
        if (expression->Reference.TableName != std::get<TTableDescriptor>(Query_->FromClause).Alias) {
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

        for (int index = 0; index < std::ssize(join->Lhs); ++index) {
            // TODO(bulatman) The right table keys may be replaced by transformed left table keys as well.
            auto* lhs = join->Lhs[index]->As<TReferenceExpression>();
            auto* rhs = join->Rhs[index]->As<TReferenceExpression>();
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
    : public TBaseAstVisitor<std::optional<int>, TGroupByOptimizer>
{
public:
    TGroupByOptimizer(const TReference& reference)
        : Reference_(reference)
    { }

    bool Run(TExpressionPtr expr)
    {
        return Visit(expr) == 1;
    }

    std::optional<int> OnLiteral(TLiteralExpressionPtr /*literalExpr*/)
    {
        return std::nullopt;
    }

    std::optional<int> OnReference(TReferenceExpressionPtr /*referenceExpr*/)
    {
        return std::nullopt;
    }

    std::optional<int> OnAlias(TAliasExpressionPtr /*aliasExpr*/)
    {
        return std::nullopt;
    }

    std::optional<int> OnUnary(TUnaryOpExpressionPtr /*unaryExpr*/)
    {
        return std::nullopt;
    }

    std::optional<int> OnBinary(TBinaryOpExpressionPtr binaryExpr)
    {
        if (IsLogicalBinaryOp(binaryExpr->Opcode)) {
            YT_VERIFY(binaryExpr->Lhs.size() == 1);
            YT_VERIFY(binaryExpr->Rhs.size() == 1);
            auto prefixRangesLeft = Visit(binaryExpr->Lhs[0]);
            auto prefixRangesRight = Visit(binaryExpr->Rhs[0]);
            if (!prefixRangesLeft || !prefixRangesRight) {
                return binaryExpr->Opcode == NQueryClient::EBinaryOp::And
                    ? std::max(prefixRangesLeft, prefixRangesRight)
                    : std::nullopt;
            }
            if (binaryExpr->Opcode == NQueryClient::EBinaryOp::Or) {
                return std::clamp<i64>(static_cast<i64>(*prefixRangesLeft) + *prefixRangesRight, 0, UnboundSentinel);
            } else {
                return std::min(prefixRangesLeft, prefixRangesRight);
            }
        } else if (NQueryClient::IsRelationalBinaryOp(binaryExpr->Opcode)) {
            if (IsAnyExprATargetReference(binaryExpr->Lhs, Reference_) ||
                IsAnyExprATargetReference(binaryExpr->Rhs, Reference_))
            {
                // 1 for equals operator, 0 for others.
                return binaryExpr->Opcode == NQueryClient::EBinaryOp::Equal
                    ? 1
                    : UnboundSentinel;
            }
        }
        return std::nullopt;
    }

    std::optional<int> OnFunction(TFunctionExpressionPtr /*functionExpr*/)
    {
        return std::nullopt;
    }

    std::optional<int> OnIn(TInExpressionPtr inExpr)
    {
        if (IsAnyExprATargetReference(inExpr->Expr, Reference_)) {
            return inExpr->Values.size();
        }
        return std::nullopt;
    }

    std::optional<int> OnBetween(TBetweenExpressionPtr betweenExpr)
    {
        if (IsAnyExprATargetReference(betweenExpr->Expr, Reference_)) {
            return UnboundSentinel;
        }
        return std::nullopt;
    }

    std::optional<int> OnTransform(TTransformExpressionPtr /*transformExpr*/)
    {
        return std::nullopt;
    }

    std::optional<int> OnCase(TCaseExpressionPtr /*caseExpr*/)
    {
        return std::nullopt;
    }

    std::optional<int> OnLike(TLikeExpressionPtr /*likeExpr*/)
    {
        return std::nullopt;
    }

private:
    static constexpr i64 UnboundSentinel = std::numeric_limits<int>::max();

    const NQueryClient::NAst::TReference Reference_;

    using TBaseAstVisitor<std::optional<int>, TGroupByOptimizer>::Visit;
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
    const std::vector<std::string>& prefixReferences,
    const TString& tableName)
{
    YT_VERIFY(!prefixReferences.empty());
    if (prefixReferences.size() != 1) {
        return false;
    }
    return TGroupByOptimizer(TReference(prefixReferences[0], tableName))
        .Run(filterExpression);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
