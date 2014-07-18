#pragma once

#ifndef PLAN_NODE_H_
#ifdef YCM
#include "plan_node.h"
#else
#error "Direct inclusion of this file is not allowed, include plan_node.h"
#endif
#endif

#include <ytlib/new_table_client/schema.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EOperatorKind,
    (Scan)
    (Filter)
    (Group)
    (Project)
);

DECLARE_ENUM(EAggregateFunctions,
    (Sum)
    (Min)
    (Max)
    (Average)
    (Count)
);

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

typedef SmallVector<TAggregateItem, 3> TAggregateItemList;

class TOperator
    : public TPlanNodeBase<TOperator, EOperatorKind>
{
public:
    TOperator(TPlanContext* context, EOperatorKind kind)
        : TPlanNodeBase(context, kind)
    { }

    //! Infers table schema of the query result.
    virtual const TTableSchema& GetTableSchema(bool ignoreCache = false) const = 0;

    //! Piggy-backed method |InferKeyColumns|.
    TKeyColumns GetKeyColumns() const;

    //! Constructs a name table filled with operator's schema.
    NVersionedTableClient::TNameTablePtr GetNameTable() const;

protected:
    friend class TPlanNodeBase<TOperator, EOperatorKind>;
    TOperator* CloneImpl(TPlanContext* context) const;

};

////////////////////////////////////////////////////////////////////////////////

class TScanOperator
    : public TOperator
{
public:
    TScanOperator(TPlanContext* context)
        : TOperator(context, EOperatorKind::Scan)
    { }

    TScanOperator(TPlanContext* context, const TScanOperator& other)
        : TOperator(context, EOperatorKind::Scan)
        , DataSplits_(other.DataSplits_)
    { }

    static inline bool IsClassOf(const TOperator* op)
    {
        return op->GetKind() == EOperatorKind::Scan;
    }

    virtual const TTableSchema& GetTableSchema(bool ignoreCache = false) const override;

    DEFINE_BYREF_RW_PROPERTY(TDataSplits, DataSplits);

private:
    mutable std::unique_ptr<TTableSchema> TableSchema_;

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

    virtual const TTableSchema& GetTableSchema(bool ignoreCache = false) const override;

    DEFINE_BYVAL_RW_PROPERTY(const TOperator*, Source);
    DEFINE_BYVAL_RW_PROPERTY(const TExpression*, Predicate);

};

class TGroupOperator
    : public TOperator
{
public:
    TGroupOperator(TPlanContext* context, const TOperator* source)
        : TOperator(context, EOperatorKind::Group)
        , Source_(source)
    { }

    TGroupOperator(TPlanContext* context, const TGroupOperator& other)
        : TOperator(context, EOperatorKind::Group)
        , Source_(other.Source_)
        , GroupItems_(other.GroupItems_)
        , AggregateItems_(other.AggregateItems_)
    { }

    static inline bool IsClassOf(const TOperator* op)
    {
        return op->GetKind() == EOperatorKind::Group;
    }

    virtual const TTableSchema& GetTableSchema(bool ignoreCache = false) const override;

    int GetGroupItemCount() const
    {
        return GroupItems_.size();
    }

    const TNamedExpression& GetGroupItem(int i) const
    {
        return GroupItems_[i];
    }

    int GetAggregateItemCount() const
    {
        return AggregateItems_.size();
    }

    const TAggregateItem& GetAggregateItem(int i) const
    {
        return AggregateItems_[i];
    }

    DEFINE_BYVAL_RW_PROPERTY(const TOperator*, Source);
    DEFINE_BYREF_RW_PROPERTY(TNamedExpressionList, GroupItems);
    DEFINE_BYREF_RW_PROPERTY(TAggregateItemList, AggregateItems);

private:
    mutable std::unique_ptr<TTableSchema> TableSchema_;

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

    virtual const TTableSchema& GetTableSchema(bool ignoreCache = false) const override;

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
    mutable std::unique_ptr<TTableSchema> TableSchema_;

};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TOperator* serialized, const TOperator* original);
const TOperator* FromProto(const NProto::TOperator& serialized, TPlanContext* context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

