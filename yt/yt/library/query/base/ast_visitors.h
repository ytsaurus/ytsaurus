#pragma once

#include "ast.h"

namespace NYT::NQueryClient::NAst {

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class TDerived, class TNode>
struct TAbstractAstVisitor
{
    TDerived* Derived()
    {
        return static_cast<TDerived*>(this);
    }

    TResult Visit(TNode node)
    {
        auto* expr = Derived()->GetExpression(node);

        if (auto* literalExpr = expr->template As<TLiteralExpression>()) {
            return Derived()->OnLiteral(literalExpr);
        } else if (auto* referenceExpr = expr->template As<TReferenceExpression>()) {
            return Derived()->OnReference(referenceExpr);
        } else if (auto* aliasExpr = expr->template As<TAliasExpression>()) {
            return Derived()->OnAlias(aliasExpr);
        } else if (auto* unaryOp = expr->template As<TUnaryOpExpression>()) {
            return Derived()->OnUnary(unaryOp);
        } else if (auto* binaryOp = expr->template As<TBinaryOpExpression>()) {
            return Derived()->OnBinary(binaryOp);
        } else if (auto* functionExpr = expr->template As<TFunctionExpression>()) {
            return Derived()->OnFunction(functionExpr);
        } else if (auto* inExpr = expr->template As<TInExpression>()) {
            return Derived()->OnIn(inExpr);
        } else if (auto* betweenExpr = expr->template As<TBetweenExpression>()) {
            return Derived()->OnBetween(betweenExpr);
        } else if (auto* transformExpr = expr->template As<TTransformExpression>()) {
            return Derived()->OnTransform(transformExpr);
        } else if (auto* caseExpr = expr->template As<TCaseExpression>()) {
            return Derived()->OnCase(caseExpr);
        } else if (auto* likeExpr = expr->template As<TLikeExpression>()) {
            return Derived()->OnLike(likeExpr);
        }
        YT_ABORT();
    }
};

template <class TDerived, class TNode>
struct TAbstractAstVisitor<void, TDerived, TNode>
{
    TDerived* Derived()
    {
        return static_cast<TDerived*>(this);
    }

    void Visit(TNode node)
    {
        auto* expr = Derived()->GetExpression(node);

        if (auto* literalExpr = expr->template As<TLiteralExpression>()) {
            return Derived()->OnLiteral(literalExpr);
        } else if (auto* referenceExpr = expr->template As<TReferenceExpression>()) {
            return Derived()->OnReference(referenceExpr);
        } else if (auto* aliasExpr = expr->template As<TAliasExpression>()) {
            return Derived()->OnAlias(aliasExpr);
        } else if (auto* unaryOp = expr->template As<TUnaryOpExpression>()) {
            return Derived()->OnUnary(unaryOp);
        } else if (auto* binaryOp = expr->template As<TBinaryOpExpression>()) {
            return Derived()->OnBinary(binaryOp);
        } else if (auto* functionExpr = expr->template As<TFunctionExpression>()) {
            return Derived()->OnFunction(functionExpr);
        } else if (auto* inExpr = expr->template As<TInExpression>()) {
            return Derived()->OnIn(inExpr);
        } else if (auto* betweenExpr = expr->template As<TBetweenExpression>()) {
            return Derived()->OnBetween(betweenExpr);
        } else if (auto* transformExpr = expr->template As<TTransformExpression>()) {
            return Derived()->OnTransform(transformExpr);
        } else if (auto* caseExpr = expr->template As<TCaseExpression>()) {
            return Derived()->OnCase(caseExpr);
        } else if (auto* likeExpr = expr->template As<TLikeExpression>()) {
            return Derived()->OnLike(likeExpr);
        }
        YT_ABORT();
    }

    void Visit(const std::vector<TNode>& tuple)
    {
        for (const auto& element : tuple) {
            Visit(element);
        }
    }

    void Visit(const std::optional<std::vector<TNode>>& nullableTuple)
    {
        if (nullableTuple) {
            Visit(*nullableTuple);
        }
    }
};

template <class TResult, class TDerived>
struct TBaseAstVisitor
    : TAbstractAstVisitor<TResult, TDerived, TExpressionPtr>
{
    TExpressionPtr GetExpression(TExpressionPtr expr)
    {
        return expr;
    }
};

template <class TDerived>
struct TAstVisitor
    : public TBaseAstVisitor<void, TDerived>
{
    using TBase = TBaseAstVisitor<void, TDerived>;
    using TBase::Visit;

    void OnLiteral(const TLiteralExpressionPtr /*literalExpr*/)
    { }

    void OnReference(const TReferenceExpressionPtr /*referenceExpr*/)
    { }

    void OnAlias(const TAliasExpressionPtr /*referenceExpr*/)
    { }

    void OnUnary(const TUnaryOpExpressionPtr unaryExpr)
    {
        Visit(unaryExpr->Operand);
    }

    void OnBinary(const TBinaryOpExpressionPtr binaryExpr)
    {
        Visit(binaryExpr->Lhs);
        Visit(binaryExpr->Rhs);
    }

    void OnFunction(const TFunctionExpressionPtr functionExpr)
    {
        Visit(functionExpr->Arguments);
    }

    void OnIn(const TInExpressionPtr inExpr)
    {
        Visit(inExpr->Expr);
    }

    void OnBetween(const TBetweenExpressionPtr betweenExpr)
    {
        Visit(betweenExpr->Expr);
    }

    void OnTransform(const TTransformExpressionPtr transformExpr)
    {
        Visit(transformExpr->Expr);
        Visit(transformExpr->DefaultExpr);
    }

    void OnCase(const TCaseExpressionPtr caseExpr)
    {
        Visit(caseExpr->OptionalOperand);
        for (const auto& whenThenExpression : caseExpr->WhenThenExpressions) {
            Visit(whenThenExpression.first);
            Visit(whenThenExpression.second);
        }
        Visit(caseExpr->DefaultExpression);
    }

    void OnLike(const TLikeExpressionPtr likeExpr)
    {
        Visit(likeExpr->Text);
        Visit(likeExpr->Pattern);
        Visit(likeExpr->EscapeCharacter);
    }

    void Visit(const std::vector<TExpressionPtr>& tuple)
    {
        for (auto expr : tuple) {
            Visit(expr);
        }
    }

    void Visit(const std::optional<std::vector<TExpressionPtr>>& nullableTuple)
    {
        if (nullableTuple) {
            return Visit(*nullableTuple);
        }
    }
};

template <class TDerived>
struct TRewriter
    : public TBaseAstVisitor<TExpressionPtr, TDerived>
{
    using TBaseAstVisitor<TExpressionPtr, TDerived>::Visit;

    TAstHead* Head;

    explicit TRewriter(TAstHead* head)
        : Head(head)
    { }

    TExpressionPtr OnLiteral(TLiteralExpressionPtr literal)
    {
        return literal;
    }

    TExpressionPtr OnReference(TReferenceExpressionPtr reference)
    {
        return reference;
    }

    TExpressionPtr OnAlias(TAliasExpressionPtr alias)
    {

        auto* newExpression = Visit(alias->Expression);
        if (newExpression == alias->Expression) {
            return alias;
        } else {
            return Head->New<TAliasExpression>(NullSourceLocation, newExpression, alias->Name);
        }
    }

    TExpressionPtr OnUnary(TUnaryOpExpressionPtr unary)
    {
        auto newOperand = Visit(unary->Operand);
        if (newOperand == unary->Operand) {
            return unary;
        } else {
            return Head->New<TUnaryOpExpression>(NullSourceLocation, unary->Opcode, std::move(newOperand));
        }
    }

    TExpressionPtr OnBinary(TBinaryOpExpressionPtr binary)
    {
        auto newLhs = Visit(binary->Lhs);
        auto newRhs = Visit(binary->Rhs);

        if (newLhs == binary->Lhs && newRhs == binary->Rhs) {
            return binary;
        } else {
            return Head->New<TBinaryOpExpression>(
                NullSourceLocation,
                binary->Opcode,
                std::move(newLhs),
                std::move(newRhs));
        }
    }

    TExpressionPtr OnFunction(TFunctionExpressionPtr function)
    {
        auto newArguments = Visit(function->Arguments);

        if (newArguments == function->Arguments) {
            return function;
        } else {
            return Head->New<TFunctionExpression>(
                NullSourceLocation,
                function->FunctionName,
                std::move(newArguments));
        }
    }

    TExpressionPtr OnIn(TInExpressionPtr in)
    {
        auto expression = Visit(in->Expr);

        if (expression == in->Expr) {
            return in;
        } else {
            return Head->New<TInExpression>(NullSourceLocation, std::move(expression), in->Values);
        }
    }

    TExpressionPtr OnBetween(TBetweenExpressionPtr between)
    {
        auto expression = Visit(between->Expr);

        if (expression == between->Expr) {
            return between;
        } else {
            return Head->New<TBetweenExpression>(NullSourceLocation, std::move(expression), between->Values);
        }
    }

    TExpressionPtr OnTransform(TTransformExpressionPtr transform)
    {
        auto expression = Visit(transform->Expr);
        auto newDefault = Visit(transform->DefaultExpr);

        if (newDefault == transform->DefaultExpr && expression == transform->Expr) {
            return transform;
        } else {
            return Head->New<TTransformExpression>(
                NullSourceLocation,
                std::move(expression),
                transform->From,
                transform->To,
                std::move(newDefault));
        }
    }

    TExpressionPtr OnCase(TCaseExpressionPtr caseExpr)
    {
        auto newOptionalOperand = Visit(caseExpr->OptionalOperand);
        auto newDefaultExpression = Visit(caseExpr->DefaultExpression);

        int expressionCount = caseExpr->WhenThenExpressions.size();
        std::vector<std::pair<TExpressionList, TExpressionList>> newWhenThenExpressions(expressionCount);
        for (int index = 0; index < expressionCount; ++index) {
            newWhenThenExpressions[index] = {
                Visit(caseExpr->WhenThenExpressions[index].first),
                Visit(caseExpr->WhenThenExpressions[index].second)
            };
        }

        if (newWhenThenExpressions == caseExpr->WhenThenExpressions &&
            newOptionalOperand == caseExpr->OptionalOperand &&
            newDefaultExpression == caseExpr->DefaultExpression)
        {
            return caseExpr;
        }

        return Head->New<TCaseExpression>(
            NullSourceLocation,
            std::move(newOptionalOperand),
            std::move(newWhenThenExpressions),
            std::move(newDefaultExpression));
    }

    TExpressionPtr OnLike(TLikeExpressionPtr like)
    {
        auto newText = Visit(like->Text);
        auto newPattern = Visit(like->Pattern);
        auto newEscape = Visit(like->EscapeCharacter);

        if (newText == like->Text && newPattern == like->Pattern && newEscape == like->EscapeCharacter) {
            return like;
        } else {
            return Head->New<TLikeExpression>(
                NullSourceLocation,
                std::move(newText),
                like->Opcode,
                std::move(newPattern),
                std::move(newEscape));
        }
    }

    std::vector<TExpressionPtr> Visit(const std::vector<TExpressionPtr>& tuple)
    {
        int size = tuple.size();
        std::vector<TExpressionPtr> newTuple(size);
        for (int index = 0; index < size; ++index) {
            newTuple[index] = Visit(tuple[index]);
        }
        return newTuple;
    }

    std::optional<std::vector<TExpressionPtr>> Visit(const std::optional<std::vector<TExpressionPtr>>& nullableTuple)
    {
        if (!nullableTuple) {
            return std::nullopt;
        } else {
            return Visit(*nullableTuple);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TListContainsTransformer
    : public TRewriter<TListContainsTransformer>
{
    using TBase = TRewriter<TListContainsTransformer>;

    const TReference& RepeatedIndexedColumn;
    const TReference& UnfoldedIndexerColumn;

    TListContainsTransformer(
        TAstHead* head,
        const TReference& repeatedIndexedColumn,
        const TReference& unfoldedIndexerColumn);

    TExpressionPtr OnFunction(TFunctionExpressionPtr function);
};

////////////////////////////////////////////////////////////////////////////////

struct TInTransformer
    : public TRewriter<TInTransformer>
{
    using TBase = TRewriter<TInTransformer>;

    const TReference& RepeatedIndexedColumn;
    const TReference& UnfoldedIndexerColumn;

    TInTransformer(
        TAstHead* head,
        const TReference& repeatedIndexedColumn,
        const TReference& unfoldedIndexerColumn);

    TExpressionPtr OnIn(TInExpressionPtr inExpr);
};

////////////////////////////////////////////////////////////////////////////////

struct TTableReferenceReplacer
    : public TRewriter<TTableReferenceReplacer>
{
    using TBase = TRewriter<TTableReferenceReplacer>;

    const THashSet<TString> ReplacedColumns;
    const std::optional<TString>& OldAlias;
    const std::optional<TString>& NewAlias;

    TTableReferenceReplacer(
        TAstHead* head,
        THashSet<TString> replacedColumns,
        const std::optional<TString>& oldAlias,
        const std::optional<TString>& newAlias);

    TExpressionPtr OnReference(TReferenceExpressionPtr reference);
};

////////////////////////////////////////////////////////////////////////////////

class TReferenceHarvester
    : public TAstVisitor<TReferenceHarvester>
{
public:
    explicit TReferenceHarvester(TColumnSet* storage);

    void OnReference(const TReferenceExpression* referenceExpr);

private:
    TColumnSet* const Storage_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NAst
