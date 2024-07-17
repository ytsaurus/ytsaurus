#pragma once

#include "ast.h"
#include "helpers.h"

namespace NYT::NQueryClient::NAst {

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class TDerived, class TNode>
struct TAbstractAstVisitor
{
    TDerived* Derived();

    TResult Visit(TNode node);
};

template <class TDerived, class TNode>
struct TAbstractAstVisitor<void, TDerived, TNode>
{
    TDerived* Derived();

    void Visit(TNode node);
    void Visit(const std::vector<TNode>& tuple);
    void Visit(const std::optional<std::vector<TNode>>& nullableTuple);
};

template <class TResult, class TDerived>
struct TBaseAstVisitor
    : TAbstractAstVisitor<TResult, TDerived, TExpressionPtr>
{
    TExpressionPtr GetExpression(TExpressionPtr expr);
};

template <class TDerived>
struct TAstVisitor
    : public TBaseAstVisitor<void, TDerived>
{
    using TBase = TBaseAstVisitor<void, TDerived>;
    using TBase::Visit;

    void OnLiteral(const TLiteralExpressionPtr literalExpr);
    void OnReference(const TReferenceExpressionPtr referenceExpr);
    void OnAlias(const TAliasExpressionPtr aliasExpr);
    void OnUnary(const TUnaryOpExpressionPtr unaryExpr);
    void OnBinary(const TBinaryOpExpressionPtr binaryExpr);
    void OnFunction(const TFunctionExpressionPtr functionExpr);
    void OnIn(const TInExpressionPtr inExpr);
    void OnBetween(const TBetweenExpressionPtr betweenExpr);
    void OnTransform(const TTransformExpressionPtr transformExpr);
    void OnCase(const TCaseExpressionPtr caseExpr);
    void OnLike(const TLikeExpressionPtr likeExpr);
    void Visit(const std::vector<TExpressionPtr>& tuple);
    void Visit(const std::optional<std::vector<TExpressionPtr>>& nullableTuple);
};

template <class TDerived>
struct TRewriter
    : public TBaseAstVisitor<TExpressionPtr, TDerived>
{
    using TBaseAstVisitor<TExpressionPtr, TDerived>::Visit;

    TObjectsHolder* Head;

    explicit TRewriter(TObjectsHolder* head);

    TExpressionPtr OnLiteral(TLiteralExpressionPtr literalExpr);
    TExpressionPtr OnReference(TReferenceExpressionPtr referenceExpr);
    TExpressionPtr OnAlias(TAliasExpressionPtr aliasExpr);
    TExpressionPtr OnUnary(TUnaryOpExpressionPtr unaryExpr);
    TExpressionPtr OnBinary(TBinaryOpExpressionPtr binaryExpr);
    TExpressionPtr OnFunction(TFunctionExpressionPtr functionExpr);
    TExpressionPtr OnIn(TInExpressionPtr inExpr);
    TExpressionPtr OnBetween(TBetweenExpressionPtr betweenExpr);
    TExpressionPtr OnTransform(TTransformExpressionPtr transformExpr);
    TExpressionPtr OnCase(TCaseExpressionPtr caseExprExpr);
    TExpressionPtr OnLike(TLikeExpressionPtr likeExpr);

    std::vector<TExpressionPtr> Visit(const std::vector<TExpressionPtr>& tuple);
    std::optional<std::vector<TExpressionPtr>> Visit(const std::optional<std::vector<TExpressionPtr>>& nullableTuple);
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

#define AST_VISITORS_INL_H_
#include "ast_visitors-inl.h"
#undef AST_VISITORS_INL_H_
