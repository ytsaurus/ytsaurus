#pragma once

#ifndef PLAN_NODE_H_
#ifdef YCM
#include "plan_node.h"
#else
#error "Direct inclusion of this file is not allowed, include plan_node.h"
#endif
#endif

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

//typedef std::pair<const TExpression*, Stroka> TNamedExpression;

//typedef SmallVector<TNamedExpression, TypicalNamedExpressionsCount> TNamedExpressionList;

class TNamedExpression
{
public:
    TNamedExpression()
    { }

    TNamedExpression(const TExpression* expression, Stroka name)
        : Expression(expression)
        , Name(name)
    { }

    const TExpression* Expression;
    Stroka Name;
};

typedef SmallVector<TNamedExpression, TypicalNamedExpressionsCount> TNamedExpressionList;

DECLARE_ENUM(EOperatorKind,
    (Scan)
    (Union)
    (Filter)
    (GroupBy)
    (Project)
);

DECLARE_ENUM(EAggregateFunctions,
    (Sum)
    (Min)
    (Max)
    (Average)
    (Count)
);

class TAggregateItem
{
public:
    TAggregateItem()
    { }

    TAggregateItem(const TExpression* expression, EAggregateFunctions aggregateFunction, Stroka name)
        : Expression(expression)
        , AggregateFunction(aggregateFunction)
        , Name(name)
    { }

    const TExpression* Expression;
    EAggregateFunctions AggregateFunction;
    Stroka Name;
};

class TOperator
    : public TPlanNodeBase<TOperator, EOperatorKind>
{
public:
    TOperator(TPlanContext* context, EOperatorKind kind)
        : TPlanNodeBase(context, kind)
    { }

    TOperator* CloneImpl(TPlanContext* context) const;

    //! Piggy-backed method |InferTableSchema|.
    TTableSchema GetTableSchema() const;

    //! Piggy-backed method |InferNameTable|.
    NVersionedTableClient::TNameTablePtr GetNameTable() const;

    //! Piggy-backed method |InferKeyColumns|.
    TKeyColumns GetKeyColumns() const;

};

////////////////////////////////////////////////////////////////////////////////

class TScanOperator
    : public TOperator
{
public:
    TScanOperator(TPlanContext* context, int tableIndex)
        : TOperator(context, EOperatorKind::Scan)
        , TableIndex_(tableIndex)
    { }

    TScanOperator(TPlanContext* context, const TScanOperator& other)
        : TOperator(context, EOperatorKind::Scan)
        , TableIndex_(other.TableIndex_)
    {
        DataSplit_ = other.DataSplit_;
    }

    static inline bool IsClassOf(const TOperator* op)
    {
        return op->GetKind() == EOperatorKind::Scan;
    }

    virtual TArrayRef<const TOperator*> Children() const override
    {
        return Null;
    }

    DEFINE_BYVAL_RO_PROPERTY(int, TableIndex);

    DEFINE_BYREF_RW_PROPERTY(TDataSplit, DataSplit);

};

class TUnionOperator
    : public TOperator
{
public:
    typedef SmallVector<const TOperator*, TypicalUnionArity> TSources;

    TUnionOperator(TPlanContext* context)
        : TOperator(context, EOperatorKind::Union)
    { }

    TUnionOperator(TPlanContext* context, const TUnionOperator& other)
        : TOperator(context, EOperatorKind::Union)
        , Sources_(other.Sources_)
    { }

    static inline bool IsClassOf(const TOperator* op)
    {
        return op->GetKind() == EOperatorKind::Union;
    }

    virtual TArrayRef<const TOperator*> Children() const override
    {
        return Sources_;
    }

    TSources& Sources()
    {
        return Sources_;
    }

    const TSources& Sources() const
    {
        return Sources_;
    }

    const TOperator* GetSource(int i) const
    {
        return Sources_[i];
    }

private:
    TSources Sources_;

};

class TFilterOperator
    : public TOperator
{
public:
    TFilterOperator(TPlanContext* context, const TOperator* source)
        : TOperator(context, EOperatorKind::Filter)
        , Source_(source)
    { }

    TFilterOperator(TPlanContext* context, const TFilterOperator& other)
        : TOperator(context, EOperatorKind::Filter)
        , Source_(other.Source_)
        , Predicate_(other.Predicate_)
    { }

    static inline bool IsClassOf(const TOperator* op)
    {
        return op->GetKind() == EOperatorKind::Filter;
    }

    virtual TArrayRef<const TOperator*> Children() const override
    {
        return Source_;
    }

    DEFINE_BYVAL_RW_PROPERTY(const TOperator*, Source);

    DEFINE_BYVAL_RW_PROPERTY(const TExpression*, Predicate);

};

class TGroupByOperator
    : public TOperator
{
public:
    typedef SmallVector<TAggregateItem, 3> TAggregateItemList;

    TGroupByOperator(TPlanContext* context, const TOperator* source)
        : TOperator(context, EOperatorKind::GroupBy)
        , Source_(source)
    { }

    static inline bool IsClassOf(const TOperator* op)
    {
        return op->GetKind() == EOperatorKind::GroupBy;
    }

    virtual TArrayRef<const TOperator*> Children() const override
    {
        return Source_;
    }

    TNamedExpressionList& GroupItems()
    {
        return GroupItems_;
    }

    const TNamedExpressionList& GroupItems() const
    {
        return GroupItems_;
    }

    int GetGroupItemsCount() const
    {
        return GroupItems_.size();
    }

    const TNamedExpression& GetGroupItem(int i) const
    {
        return GroupItems_[i];
    }

    DEFINE_BYVAL_RW_PROPERTY(const TOperator*, Source);
    DEFINE_BYREF_RW_PROPERTY(TAggregateItemList, AggregateItems);

private:
    TNamedExpressionList GroupItems_;

};

class TProjectOperator
    : public TOperator
{
public:
    TProjectOperator(TPlanContext* context, const TOperator* source)
        : TOperator(context, EOperatorKind::Project)
        , Source_(source)
    { }

    TProjectOperator(TPlanContext* context, const TProjectOperator& other)
        : TOperator(context, EOperatorKind::Project)
        , Source_(other.Source_)
        , Projections_(other.Projections_)
    { }

    static inline bool IsClassOf(const TOperator* op)
    {
        return op->GetKind() == EOperatorKind::Project;
    }

    virtual TArrayRef<const TOperator*> Children() const override
    {
        return Source_;
    }

    TNamedExpressionList& Projections()
    {
        return Projections_;
    }

    const TNamedExpressionList& Projections() const
    {
        return Projections_;
    }

    int GetProjectionCount() const
    {
        return Projections_.size();
    }

    const TNamedExpression& GetProjection(int i) const
    {
        return Projections_[i];
    }

    DEFINE_BYVAL_RW_PROPERTY(const TOperator*, Source);

private:
    TNamedExpressionList Projections_;

};

////////////////////////////////////////////////////////////////////////////////

namespace NProto { class TOperator; }
void ToProto(NProto::TOperator* serialized, const TOperator* original);
const TOperator* FromProto(const NProto::TOperator& serialized, TPlanContext* context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

