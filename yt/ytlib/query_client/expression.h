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
    (Equal)
    (NotEqual)

    (Less)
    (LessOrEqual)
    (Greater)
    (GreaterOrEqual)

    (Plus)
    (Minus)

    (Multiply)
    (Divide)
    (Modulo)

    (And)
    (Or)
);

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

    virtual ERowValueType Typecheck() const = 0;

    virtual Stroka InferName() const = 0;

    Stroka GetSource() const;

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

    virtual ERowValueType Typecheck() const override
    {
        return ERowValueType::Integer;
    }

    virtual Stroka InferName() const override
    {
        return ToString(Value_);
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

    virtual ERowValueType Typecheck() const override
    {
        return ERowValueType::Double;
    }

    virtual Stroka InferName() const override
    {
        return ToString(Value_);
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
        const TStringBuf& name)
        : TExpression(context, EExpressionKind::Reference, sourceLocation)
        , TableIndex_(tableIndex)
        , Name_(name)
        , CachedType_(ERowValueType::TheBottom)
        , CachedKeyIndex_(-1)
    { }

    static inline bool IsClassOf(const TExpression* expr)
    {
        return expr->GetKind() == EExpressionKind::Reference;
    }

    virtual ERowValueType Typecheck() const override
    {
        return CachedType_;
    }

    virtual Stroka InferName() const override
    {
        return Name_;
    }

    DEFINE_BYVAL_RO_PROPERTY(int, TableIndex);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Name);

    DEFINE_BYVAL_RW_PROPERTY(ERowValueType, CachedType);
    DEFINE_BYVAL_RW_PROPERTY(int, CachedKeyIndex);

};

//! Represent a function applied to its arguments.
class TFunctionExpression
    : public TExpression
{
public:
    typedef TSmallVector<const TExpression*, TypicalFunctionArity> TArguments;

    TFunctionExpression(
        TPlanContext* context,
        const TSourceLocation& sourceLocation,
        const TStringBuf& name)
        : TExpression(context, EExpressionKind::Function, sourceLocation)
        , Name_(name)
    { }

    static inline bool IsClassOf(const TExpression* expr)
    {
        return expr->GetKind() == EExpressionKind::Function;
    }

    virtual TArrayRef<const TExpression*> Children() const override
    {
        return Arguments_;
    }

    virtual ERowValueType Typecheck() const override
    {
        // TODO(sandello): We should register functions with their signatures.
        YUNIMPLEMENTED();
    }

    virtual Stroka InferName() const override
    {
        Stroka result;
        result += Name_;
        result += "(";
        for (const auto& argument : Arguments_) {
            if (!result.empty()) {
                result += ", ";
            }
            result += argument->InferName();
        }
        result += ")";
        return result;
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

    DEFINE_BYVAL_RO_PROPERTY(Stroka, Name);

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

    virtual ERowValueType Typecheck() const override
    {
        auto lhsType = GetLhs()->Typecheck();
        auto rhsType = GetRhs()->Typecheck();

        if (lhsType != rhsType) {
            THROW_ERROR_EXCEPTION(
                "Type mismatch between left-hand side and right-hand side in expression %s",
                ~GetSource().Quote())
                << TErrorAttribute("lhs_type", lhsType.ToString())
                << TErrorAttribute("rhs_type", rhsType.ToString());
        }

        // XXX(sandello): As we do not have boolean type, we cast cmps to int.
        // TODO(sandello): For arithmetic exprs we have to return different value.
        return ERowValueType::Integer;
    }

    virtual Stroka InferName() const override
    {
        Stroka result;
        result += GetLhs()->InferName();
        switch (Opcode_) {
        case EBinaryOp::Less:           result = " < "; break;
        case EBinaryOp::LessOrEqual:    result = " <= "; break;
        case EBinaryOp::Equal:          result = " = "; break;
        case EBinaryOp::NotEqual:       result = " != "; break;
        case EBinaryOp::Greater:        result = " > "; break;
        case EBinaryOp::GreaterOrEqual: result = " >= "; break;
        default: YUNREACHABLE();
        }
        result += GetRhs()->InferName();
        return result;
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

