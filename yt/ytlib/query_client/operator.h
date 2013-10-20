#pragma once

#ifndef AST_H_
#ifdef YCM
#include "ast.h"
#else
#error "Direct inclusion of this file is not allowed, include ast.h"
#endif
#endif

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const int TypicalOperatorChildCount = 2;
const int TypicalProjectExpressionCount = 4;

////////////////////////////////////////////////////////////////////////////////

namespace NProto { class TOperator; }
void ToProto(NProto::TOperator* serialized, TOperator* original);
TOperator* FromProto(const NProto::TOperator& serialized, TQueryContext* context);

////////////////////////////////////////////////////////////////////////////////

class TOperator
    : public TAstNodeBase<TOperator, TypicalOperatorChildCount>
{
public:
    explicit TOperator(TQueryContext* context)
        : TAstNodeBase(context)
    { }

};

////////////////////////////////////////////////////////////////////////////////

class TScanOperator
    : public TOperator
{
public:
    TScanOperator(TQueryContext* context, int tableIndex)
        : TOperator(context)
        , TableIndex_(tableIndex)
    { }

    virtual bool Accept(IAstVisitor*) override;

    virtual void Check() const override
    {
        YCHECK(Children_.empty());
        TOperator::Check();
    }

    TScanOperator* Clone()
    {
        auto clone = new (Context_) TScanOperator(Context_, TableIndex_);
        clone->DataSplit().CopyFrom(DataSplit_);
        return clone;
    }

    DEFINE_BYVAL_RO_PROPERTY(int, TableIndex);

    DEFINE_BYREF_RW_PROPERTY(TDataSplit, DataSplit);

};

class TUnionOperator
    : public TOperator
{
public:
    TUnionOperator(TQueryContext* context)
        : TOperator(context)
    { }

    virtual bool Accept(IAstVisitor*) override;

    TUnionOperator* Clone()
    {
        auto clone = new (Context_) TUnionOperator(Context_);
        return clone;
    }

};

class TFilterOperator
    : public TOperator
{
public:
    TFilterOperator(TQueryContext* context, TExpression* predicate)
        : TOperator(context)
        , Predicate_(predicate)
    { }

    virtual bool Accept(IAstVisitor*) override;

    virtual void Check() const override
    {
        Predicate_->Check();
        YCHECK(Children_.size() == 1);
        TOperator::Check();
    }

    TFilterOperator* Clone()
    {
        auto clone = new (Context_) TFilterOperator(Context_, Predicate_);
        return clone;
    }

    DEFINE_BYVAL_RO_PROPERTY(TExpression*, Predicate);

};

class TProjectOperator
    : public TOperator
{
public:
    explicit TProjectOperator(TQueryContext* context)
        : TOperator(context)
    { }

    template <typename TIterator>
    TProjectOperator(TQueryContext* context, TIterator begin, TIterator end)
        : TOperator(context)
        , Expressions_(begin, end)
    { }

    virtual bool Accept(IAstVisitor*) override;

    virtual void Check() const override
    {
        FOREACH (const auto& expr, Expressions_) {
            expr->Check();
        }
        YCHECK(Children_.size() == 1);
        TOperator::Check();
    }

    TProjectOperator* Clone()
    {
        auto clone = new (Context_) TProjectOperator(
            Context_,
            Expressions_.begin(),
            Expressions_.end());
        return clone;
    }

    SmallVectorImpl<TExpression*>& Expressions()
    {
        return Expressions_;
    }

    const SmallVectorImpl<TExpression*>& Expressions() const
    {
        return Expressions_;
    }

    TExpression* GetExpression(int i)
    {
        return Expressions_[i];
    }

    const TExpression* GetExpression(int i) const
    {
        return Expressions_[i];
    }

protected:
    TSmallVector<TExpression*, TypicalProjectExpressionCount> Expressions_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

