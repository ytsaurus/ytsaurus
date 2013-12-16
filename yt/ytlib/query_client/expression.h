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
    // Comparsion operations.
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
        , CachedType_(EValueType::Null)
        , CachedName_()
    { }

    virtual TArrayRef<const TExpression*> Children() const override
    {
        return Null;
    }

    //! Returns expression source (as it was written by the user) if possible.
    Stroka GetSource() const;

    //! Piggy-backed method |InferType|.
    EValueType GetType() const;

    //! Piggy-backed method |InferName|.
    Stroka GetName() const;

    //! Returns cached expression type.
    //! Null type must be interpreted as a cache miss.
    EValueType GetCachedType() const
    {
        return CachedType_;
    }

    //! Caches an expression type.
    void SetCachedType(EValueType type) const
    {
        CachedType_ = std::move(type);
    }

    //! Returns cached expression name.
    //! Empty string must be interpreted as a cache miss.
    Stroka GetCachedName() const
    {
        return CachedName_;
    }

    //! Caches an expression name.
    void SetCachedName(Stroka name) const
    {
        CachedName_ = std::move(name);
    }

private:
    TSourceLocation SourceLocation_;
    mutable EValueType CachedType_;
    mutable Stroka CachedName_;

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
        int tableIndex,
        const TStringBuf& columnName)
        : TExpression(context, EExpressionKind::Reference, sourceLocation)
        , TableIndex_(tableIndex)
        , ColumnName_(columnName)
        , IndexInRow_(-1)
        , IndexInKey_(-1)
    { }

    static inline bool IsClassOf(const TExpression* expr)
    {
        return expr->GetKind() == EExpressionKind::Reference;
    }

    DEFINE_BYVAL_RO_PROPERTY(int, TableIndex);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, ColumnName);

    DEFINE_BYVAL_RW_PROPERTY(int, IndexInRow);
    DEFINE_BYVAL_RW_PROPERTY(int, IndexInKey);

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
        , FunctionName_(functionName)
    { }

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

    DEFINE_BYVAL_RO_PROPERTY(Stroka, FunctionName);

private:
    TArguments Arguments_;

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

namespace NProto { class TExpression; }
void ToProto(NProto::TExpression* serialized, const TExpression* original);
const TExpression* FromProto(const NProto::TExpression& serialized, TPlanContext* context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

