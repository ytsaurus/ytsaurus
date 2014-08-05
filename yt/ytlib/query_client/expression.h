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

DECLARE_ENUM(EExpressionKind,
    (Literal)
    (Reference)
    (Function)
    (BinaryOp)
);

DECLARE_ENUM(EBinaryOp,
    // Arithmetical operations.
    (Plus)
    (Minus)
    (Multiply)
    (Divide)
    // Integral operations.
    (Modulo)
    // Logical operations.
    (And)
    (Or)
    // Relational operations.
    (Equal)
    (NotEqual)
    (Less)
    (LessOrEqual)
    (Greater)
    (GreaterOrEqual)
);

const char* GetBinaryOpcodeLexeme(EBinaryOp opcode);

////////////////////////////////////////////////////////////////////////////////

class TExpression
    : public TPlanNodeBase<TExpression, EExpressionKind>
{
public:
    TExpression(
        TPlanContext* context,
        EExpressionKind kind,
        const TSourceLocation& sourceLocation)
        : TPlanNodeBase(context, kind)
        , SourceLocation_(sourceLocation)
    { }

    //! Returns expression source (as it was written by the user) if possible.
    Stroka GetSource() const;

    //! Piggy-backed method |InferType|.
    EValueType GetType(const TTableSchema& sourceSchema) const;

    //! Piggy-backed method |InferName|.
    Stroka GetName() const;

    //! Piggy-backed method |IsConstant|.
    bool IsConstant() const;

    //! Piggy-backed method |GetConstantValue|.
    TValue GetConstantValue() const;

protected:
    friend class TPlanNodeBase<TExpression, EExpressionKind>;
    TExpression* CloneImpl(TPlanContext* context) const;

    TSourceLocation SourceLocation_;

};

////////////////////////////////////////////////////////////////////////////////

//! Represents a constant value.
class TLiteralExpression
    : public TExpression
{
public:
    TLiteralExpression(
        TPlanContext* context,
        const TSourceLocation& sourceLocation,
        NVersionedTableClient::TUnversionedValue value)
        : TExpression(context, EExpressionKind::Literal, sourceLocation)
        , Value_(value)
    { }

    TLiteralExpression(
        TPlanContext* context,
        const TLiteralExpression& other)
        : TExpression(context, EExpressionKind::Literal, other.SourceLocation_)
        , Value_(other.Value_)
    { }

    static inline bool IsClassOf(const TExpression* expr)
    {
        return expr->GetKind() == EExpressionKind::Literal;
    }

    DEFINE_BYVAL_RO_PROPERTY(NVersionedTableClient::TUnversionedValue, Value);

};

//! Represents a reference to a column in a row.
class TReferenceExpression
    : public TExpression
{
public:
    TReferenceExpression(
        TPlanContext* context,
        const TSourceLocation& sourceLocation,
        const TStringBuf& columnName)
        : TExpression(context, EExpressionKind::Reference, sourceLocation)
        , ColumnName_(columnName)
    { }

    TReferenceExpression(
        TPlanContext* context,
        const TReferenceExpression& other)
        : TExpression(context, EExpressionKind::Reference, other.SourceLocation_)
        , ColumnName_(other.ColumnName_)
    { }

    static inline bool IsClassOf(const TExpression* expr)
    {
        return expr->GetKind() == EExpressionKind::Reference;
    }

    DEFINE_BYVAL_RO_PROPERTY(Stroka, ColumnName);

};

//! Represent a function applied to its arguments.
class TFunctionExpression
    : public TExpression
{
public:
    typedef SmallVector<const TExpression*, TypicalFunctionArity> TArguments;

    template <class... TArgs>
    TFunctionExpression(
        TPlanContext* context,
        const TSourceLocation& sourceLocation,
        const TStringBuf& functionName,
        TArgs&&... args)
        : TExpression(context, EExpressionKind::Function, sourceLocation)
    {
        std::initializer_list<const TExpression*> arguments = {args...};
        Arguments_.assign(arguments.begin(), arguments.end());
        SetFunctionName(Stroka(functionName));
    }

    TFunctionExpression(
        TPlanContext* context,
        const TFunctionExpression& other)
        : TExpression(context, EExpressionKind::Function, other.SourceLocation_)
        , Arguments_(other.Arguments_)
        , FunctionName_(other.FunctionName_)
    { }

    static inline bool IsClassOf(const TExpression* expr)
    {
        return expr->GetKind() == EExpressionKind::Function;
    }

    TArguments& Arguments()
    {
        return Arguments_;
    }

    const TArguments& Arguments() const
    {
        return Arguments_;
    }

    int GetArgumentCount() const
    {
        return Arguments_.size();
    }

    const TExpression* GetArgument(int i) const
    {
        return Arguments_[i];
    }

    void SetArgument(int i, TExpression* argument)
    {
        Arguments_[i] = argument;
    }

    Stroka GetFunctionName() const
    {
        return FunctionName_;
    }

    void SetFunctionName(const Stroka& functionName)
    {
        FunctionName_ = functionName;
        FunctionName_.to_upper(0, Stroka::npos);
    }

private:
    TArguments Arguments_;
    Stroka FunctionName_;

};

//! Represents a binary operator applied to two sub-expressions.
class TBinaryOpExpression
    : public TExpression
{
public:
    TBinaryOpExpression(
        TPlanContext* context,
        const TSourceLocation& sourceLocation,
        EBinaryOp opcode,
        const TExpression* lhs,
        const TExpression* rhs)
        : TExpression(context, EExpressionKind::BinaryOp, sourceLocation)
        , Opcode_(opcode)
        , Lhs_(lhs)
        , Rhs_(rhs)
    { }

    TBinaryOpExpression(
        TPlanContext* context,
        const TBinaryOpExpression& other)
        : TExpression(context, EExpressionKind::BinaryOp, other.SourceLocation_)
        , Opcode_(other.Opcode_)
        , Lhs_(other.Lhs_)
        , Rhs_(other.Rhs_)
    { }

    static inline bool IsClassOf(const TExpression* expr)
    {
        return expr->GetKind() == EExpressionKind::BinaryOp;
    }

    const TExpression* GetLhs() const
    {
        return Lhs_;
    }

    void SetLhs(const TExpression* lhs)
    {
        Lhs_ = lhs;
    }

    const TExpression* GetRhs() const
    {
        return Rhs_;
    }

    void SetRhs(const TExpression* rhs)
    {
        Rhs_ = rhs;
    }

    DEFINE_BYVAL_RO_PROPERTY(EBinaryOp, Opcode);

protected:
    const TExpression* Lhs_;
    const TExpression* Rhs_;

};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExpression* serialized, const TExpression* original);
const TExpression* FromProto(const NProto::TExpression& serialized, TPlanContext* context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

