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

DECLARE_ENUM(EOperatorKind,
    (Scan)
    (Union)
    (Filter)
    (Project)
);

class TOperator
    : public TPlanNodeBase<TOperator, EOperatorKind>
{
public:
    TOperator(TPlanContext* context, EOperatorKind kind)
        : TPlanNodeBase(context, kind)
    { }

    //! Piggy-backed method |InferTableSchema|.
    TTableSchema GetTableSchema() const;

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
    typedef TSmallVector<const TOperator*, TypicalUnionArity> TSources;

    TUnionOperator(TPlanContext* context)
        : TOperator(context, EOperatorKind::Union)
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

class TProjectOperator
    : public TOperator
{
public:
    typedef TSmallVector<const TExpression*, TypicalProjectionCount> TProjections;

    TProjectOperator(TPlanContext* context, const TOperator* source)
        : TOperator(context, EOperatorKind::Project)
        , Source_(source)
    { }

    static inline bool IsClassOf(const TOperator* op)
    {
        return op->GetKind() == EOperatorKind::Project;
    }

    virtual TArrayRef<const TOperator*> Children() const override
    {
        return Source_;
    }

    TProjections& Projections()
    {
        return Projections_;
    }

    const TProjections& Projections() const
    {
        return Projections_;
    }

    int GetProjectionCount() const
    {
        return Projections_.size();
    }

    const TExpression* GetProjection(int i) const
    {
        return Projections_[i];
    }

    DEFINE_BYVAL_RW_PROPERTY(const TOperator*, Source);

private:
    TProjections Projections_;

};

////////////////////////////////////////////////////////////////////////////////

namespace NProto { class TOperator; }
void ToProto(NProto::TOperator* serialized, const TOperator* original);
const TOperator* FromProto(const NProto::TOperator& serialized, TPlanContext* context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

