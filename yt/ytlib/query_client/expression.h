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
    (IntegerLiteral)
    (DoubleLiteral)
    (Reference)
    (Function)
    (BinaryOp)
);

DECLARE_ENUM(EBinaryOpKind,
    (Arithmetical)
    (Integral)
    (Logical)
    (Relational)
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
EBinaryOpKind GetBinaryOpcodeKind(EBinaryOp opcode);

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

    virtual TArrayRef<const TExpression*> Children() const override
    {
        return Null;
    }

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

private:
    TSourceLocation SourceLocation_;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents a constant integral value.
class TIntegerLiteralExpression
    : public TExpression
{
public:
    TIntegerLiteralExpression(
        TPlanContext* context,
        const TSourceLocation& sourceLocation,
        i64 value)
        : TExpression(context, EExpressionKind::IntegerLiteral, sourceLocation)
        , Value_(value)
    { }

    static inline bool IsClassOf(const TExpression* expr)
    {
        return expr->GetKind() == EExpressionKind::IntegerLiteral;
    }

    DEFINE_BYVAL_RO_PROPERTY(i64, Value);

};

//! Represents a constant floating-point value.
class TDoubleLiteralExpression
    : public TExpression
{
public:
    TDoubleLiteralExpression(
        TPlanContext* context,
        const TSourceLocation& sourceLocation,
        double value)
        : TExpression(context, EExpressionKind::DoubleLiteral, sourceLocation)
        , Value_(value)
    { }

    static inline bool IsClassOf(const TExpression* expr)
    {
        return expr->GetKind() == EExpressionKind::DoubleLiteral;
    }

    DEFINE_BYVAL_RO_PROPERTY(double, Value);

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

    TFunctionExpression(
        TPlanContext* context,
        const TSourceLocation& sourceLocation,
        const TStringBuf& functionName)
        : TExpression(context, EExpressionKind::Function, sourceLocation)
    {
        SetFunctionName(Stroka(functionName));
    }

    static inline bool IsClassOf(const TExpression* expr)
    {
        return expr->GetKind() == EExpressionKind::Function;
    }

    virtual TArrayRef<const TExpression*> Children() const override
    {
        return Arguments_;
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
    {
        Subexpressions_[Lhs_] = lhs;
        Subexpressions_[Rhs_] = rhs;
    }

    static inline bool IsClassOf(const TExpression* expr)
    {
        return expr->GetKind() == EExpressionKind::BinaryOp;
    }

    virtual TArrayRef<const TExpression*> Children() const override
    {
        // XXX(sandello): Construct explicitly to enable C-array overload.
        return MakeArrayRef(Subexpressions_);
    }

    const TExpression* GetLhs() const
    {
        return Subexpressions_[Lhs_];
    }

    const TExpression* GetRhs() const
    {
        return Subexpressions_[Rhs_];
    }

    DEFINE_BYVAL_RO_PROPERTY(EBinaryOp, Opcode);

protected:
    enum { Lhs_, Rhs_, NumberOfSubexpressions_ };
    const TExpression* Subexpressions_[NumberOfSubexpressions_];

};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExpression* serialized, const TExpression* original);
const TExpression* FromProto(const NProto::TExpression& serialized, TPlanContext* context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

