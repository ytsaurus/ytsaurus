#include "computed_fields_filter.h"

#include <yt/yt/library/query/base/ast_visitors.h>
#include <yt/yt/library/query/base/helpers.h>
#include <yt/yt/library/query/base/query_common.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

//! Visitor returns true if expression is solid relative to the division by AND and it needs to be added to the result.
class TSubTreesSplitter
    : public TBaseAstVisitor<bool, TSubTreesSplitter>
{
public:
    explicit TSubTreesSplitter(TObjectsHolder* holder)
        : Holder_(holder)
    { }

    std::vector<TExpressionPtr> Run(const TExpressionPtr& expression)
    {
        TExpressionPtr expr(expression);
        if (Visit(expr)) {
            SubTrees_.push_back(expression);
        }
        return std::move(SubTrees_);
    }

    bool OnLiteral(TLiteralExpressionPtr /*literalExpr*/)
    {
        return true;
    }

    bool OnReference(TReferenceExpressionPtr /*referenceExpr*/)
    {
        return true;
    }

    bool OnAlias(TAliasExpressionPtr /*aliasExpr*/)
    {
        return true;
    }

    bool OnUnary(TUnaryOpExpressionPtr unaryExpr)
    {
        if (unaryExpr->Opcode != NQueryClient::EUnaryOp::Not) {
            return true;
        }
        bool beforeVisit = ReverseSign_;
        ReverseSign_ = !ReverseSign_;
        if (Visit(unaryExpr->Operand)) {
            if (ReverseSign_) {
                SubTrees_.push_back(unaryExpr);
            } else {
                SubTrees_.push_back(unaryExpr->Operand[0]);
            }
        }
        ReverseSign_ = beforeVisit;
        return false;
    }

    bool OnBinary(TBinaryOpExpressionPtr binaryExpr)
    {
        auto correctOp = ReverseSign_ ? NQueryClient::EBinaryOp::Or : NQueryClient::EBinaryOp::And;
        if (binaryExpr->Opcode != correctOp) {
            return true;
        }
        OnBinaryPart(binaryExpr->Lhs);
        OnBinaryPart(binaryExpr->Rhs);
        return false;
    }

    bool OnFunction(TFunctionExpressionPtr /*functionExpr*/)
    {
        return true;
    }

    bool OnIn(TInExpressionPtr /*inExpr*/)
    {
        return true;
    }

    bool OnBetween(TBetweenExpressionPtr /*betweenExpr*/)
    {
        return true;
    }

    bool OnTransform(TTransformExpressionPtr /*transformExpr*/)
    {
        return true;
    }

    bool OnCase(TCaseExpressionPtr /*caseExpr*/)
    {
        return true;
    }

    bool OnLike(TLikeExpressionPtr /*likeExpr*/)
    {
        return true;
    }

private:
    using TBaseAstVisitor<bool, TSubTreesSplitter>::Visit;

    std::vector<TExpressionPtr> SubTrees_;
    TObjectsHolder* Holder_;
    bool ReverseSign_ = false;

    bool Visit(const TExpressionList& exprList)
    {
        if (exprList.size() == 1) {
            return Visit(exprList[0]);
        }
        return false;
    }

    bool Visit(TNullableExpressionList& list)
    {
        return list ? Visit(*list) : false;
    }

    void OnBinaryPart(const TExpressionList& expression)
    {
        if (Visit(expression)) {
            if (ReverseSign_) {
                SubTrees_.push_back(Holder_->New<TUnaryOpExpression>(
                    NQueryClient::TSourceLocation(),
                    NQueryClient::EUnaryOp::Not,
                    expression));
            } else {
                SubTrees_.push_back(expression[0]);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TComputedFieldsChecker
    : public TBaseAstVisitor<bool, TComputedFieldsChecker>
{
public:
    explicit TComputedFieldsChecker(std::function<bool(TReferenceExpressionPtr)> detector)
        : Detector_(std::move(detector))
    { }

    bool ContainsComputedField(TExpressionPtr expression)
    {
        return Visit(expression);
    }

    bool OnLiteral(TLiteralExpressionPtr /*literalExpr*/)
    {
        return false;
    }

    bool OnReference(TReferenceExpressionPtr referenceExpr)
    {
        return Detector_(referenceExpr);
    }

    bool OnAlias(TAliasExpressionPtr aliasExpr)
    {
        return Visit(aliasExpr->Expression);
    }

    bool OnUnary(TUnaryOpExpressionPtr unaryExpr)
    {
        return Visit(unaryExpr->Operand);
    }

    bool OnBinary(TBinaryOpExpressionPtr binaryExpr)
    {
        return Visit(binaryExpr->Lhs) || Visit(binaryExpr->Rhs);
    }

    bool OnFunction(TFunctionExpressionPtr functionExpr)
    {
        return Visit(functionExpr->Arguments);
    }

    bool OnIn(TInExpressionPtr inExpr)
    {
        return Visit(inExpr->Expr);
    }

    bool OnBetween(TBetweenExpressionPtr betweenExpr)
    {
        return Visit(betweenExpr->Expr);
    }

    bool OnTransform(TTransformExpressionPtr transformExpr)
    {
        return Visit(transformExpr->Expr) || Visit(transformExpr->DefaultExpr);
    }

    bool OnCase(TCaseExpressionPtr caseExpr)
    {
        return Visit(caseExpr->DefaultExpression) ||
            Visit(caseExpr->WhenThenExpressions) ||
            Visit(caseExpr->OptionalOperand);
    }

    bool OnLike(TLikeExpressionPtr likeExpr)
    {
        return Visit(likeExpr->EscapeCharacter) ||
            Visit(likeExpr->Pattern) ||
            Visit(likeExpr->Text);
    }

private:
    using TBaseAstVisitor<bool, TComputedFieldsChecker>::Visit;

    TComputedFieldsDetector Detector_;

    bool Visit(const TNullableExpressionList& list)
    {
        return list ? Visit(*list) : false;
    }

    bool Visit(const TExpressionList& list)
    {
        for (const auto& expr : list) {
            if (Visit(expr)) {
                return true;
            }
        }
        return false;
    }

    bool Visit(const TOrderExpressionList& list)
    {
        for (const auto& expr : list) {
            if (Visit(expr.Expressions)) {
                return true;
            }
        }
        return false;
    }

    bool Visit(const TWhenThenExpressionList& list)
    {
        for (const auto& expr : list) {
            if (Visit(expr.Condition) || Visit(expr.Result)) {
                return true;
            }
        }
        return false;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::vector<TExpressionPtr> SplitIntoSubTrees(TObjectsHolder* context, TExpressionPtr expression)
{
    return TSubTreesSplitter(context).Run(expression);
}

bool ContainsComputedFields(TExpressionPtr expression, std::function<bool(TReferenceExpressionPtr)> detector)
{
    return TComputedFieldsChecker(std::move(detector)).ContainsComputedField(expression);
}

std::pair<TExpressionPtr, TExpressionPtr> SplitFilter(
    TObjectsHolder* context,
    std::function<bool(TReferenceExpressionPtr)> detector,
    TExpressionPtr filter)
{
    if (!filter) {
        return {nullptr, nullptr};
    }

    auto subTrees = SplitIntoSubTrees(context, filter);

    TComputedFieldsChecker checker(std::move(detector));
    std::vector<TExpressionPtr> nonComputedPart;
    std::vector<TExpressionPtr> computedPart;
    for (auto subTree: subTrees) {
        if (checker.ContainsComputedField(subTree)) {
            computedPart.push_back(std::move(subTree));
        } else {
            nonComputedPart.push_back(std::move(subTree));
        }
    }
    return {
        BuildBinaryOperationTree(
            context,
            std::move(nonComputedPart),
            NQueryClient::EBinaryOp::And),
        BuildBinaryOperationTree(
            context,
            std::move(computedPart),
            NQueryClient::EBinaryOp::And)
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
