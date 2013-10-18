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

DECLARE_ENUM(EBinaryOp,
    (Less)
    (LessOrEqual)
    (Equal)
    (NotEqual)
    (Greater)
    (GreaterOrEqual)
);

const int TypicalExpressionChildCount = 3;

////////////////////////////////////////////////////////////////////////////////

namespace NProto { class TExpression; }
void ToProto(NProto::TExpression* serialized, TExpression* original);
TExpression* FromProto(const NProto::TExpression& serialized, TQueryContext* context);

////////////////////////////////////////////////////////////////////////////////

class TExpression
    : public TAstNodeBase<TExpression, TypicalExpressionChildCount>
{
public:
    explicit TExpression(TQueryContext* context, const TSourceLocation& sourceLocation)
        : TAstNodeBase(context)
        , SourceLocation_(sourceLocation)
    { }

    Stroka GetSource() const;

    virtual bool Accept(IAstVisitor*) = 0;

    virtual const char* GetDebugName() const
    {
        return typeid(*this).name();
    }

    virtual void Check() const
    {
        FOREACH (const auto& child, Children_) {
            YCHECK(this == child->Parent_);
        }
    }

    virtual EColumnType Typecheck() const = 0;

    DEFINE_BYREF_RW_PROPERTY(TSourceLocation, SourceLocation);

};

////////////////////////////////////////////////////////////////////////////////

//! Represents a constant integral value.
class TIntegerLiteralExpression
    : public TExpression
{
public:
    TIntegerLiteralExpression(
        TQueryContext* context,
        const TSourceLocation& sourceLocation,
        i64 value)
        : TExpression(context, sourceLocation)
        , Value_(value)
    { }

    virtual bool Accept(IAstVisitor*) override;

    virtual void Check() const override
    {
        YCHECK(Children_.empty());
    }

    virtual EColumnType Typecheck() const override
    {
        return EColumnType::Integer;
    }

    DEFINE_BYVAL_RO_PROPERTY(i64, Value);

};

//! Represents a constant floating-point value.
class TDoubleLiteralExpression
    : public TExpression
{
public:
    TDoubleLiteralExpression(
        TQueryContext* context,
        const TSourceLocation& sourceLocation,
        double value)
        : TExpression(context, sourceLocation)
        , Value_(value)
    { }

    virtual bool Accept(IAstVisitor*) override;

    virtual void Check() const override
    {
        YCHECK(Children_.empty());
    }

    virtual EColumnType Typecheck() const override
    {
        return EColumnType::Double;
    }

    DEFINE_BYVAL_RO_PROPERTY(double, Value);

};

//! Represents a reference to a column in a row.
class TReferenceExpression
    : public TExpression
{
public:
    TReferenceExpression(
        TQueryContext* context,
        const TSourceLocation& sourceLocation,
        int tableIndex,
        const TStringBuf& name)
        : TExpression(context, sourceLocation)
        , TableIndex_(tableIndex)
        , Name_(name)
        , Type_(EColumnType::TheBottom)
        , KeyIndex_(-1)
    { }

    virtual bool Accept(IAstVisitor*) override;

    virtual void Check() const override
    {
        YCHECK(Children_.empty());
    }

    virtual EColumnType Typecheck() const override
    {
        return Type_;
    }

    DEFINE_BYVAL_RO_PROPERTY(int, TableIndex);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Name);
    DEFINE_BYVAL_RW_PROPERTY(EColumnType, Type);
    DEFINE_BYVAL_RW_PROPERTY(int, KeyIndex);

};

//! Represent a function applied to its arguments.
class TFunctionExpression
    : public TExpression
{
public:
    TFunctionExpression(
        TQueryContext* context,
        const TSourceLocation& sourceLocation,
        const TStringBuf& name)
        : TExpression(context, sourceLocation)
        , Name_(name)
    { }

    template <typename TIterator>
    TFunctionExpression(
        TQueryContext* context,
        const TSourceLocation& sourceLocation,
        const TStringBuf& name,
        TIterator begin,
        TIterator end)
        : TExpression(context, sourceLocation)
        , Name_(name)
    {
        Children_.reserve(std::distance(begin, end));
        for (TIterator it = begin; it != end; ++it) {
            AttachChild(*it);
        }
    }

    virtual bool Accept(IAstVisitor*) override;

    virtual EColumnType Typecheck() const override
    {
        THROW_ERROR_EXCEPTION("Function calls are not supported yet")
            << TErrorAttribute("expression", GetSource());

        // TODO(sandello): We should register functions with their signatures.
        return EColumnType::TheBottom;
    }

    int GetArity() const
    {
        return Children_.size();
    }

    TExpression* GetArgument(int i) const
    {
        return Children_[i];
    }

    DEFINE_BYVAL_RO_PROPERTY(Stroka, Name);

};

//! Represents a binary operator applied to two sub-expressions.
class TBinaryOpExpression
    : public TExpression
{
public:
    TBinaryOpExpression(
        TQueryContext* context,
        const TSourceLocation& sourceLocation,
        EBinaryOp opcode,
        TExpression* lhs = nullptr,
        TExpression* rhs = nullptr)
        : TExpression(context, sourceLocation)
        , Opcode_(opcode)
    {
        Children_.reserve(NumberOfSubexpressions_);
        // TODO(sandello): Remove this.
        if (lhs) {
            AttachChild(lhs);
        }
        if (rhs) {
            AttachChild(rhs);
        }
    }

    virtual bool Accept(IAstVisitor*) override;

    virtual void Check() const
    {
        YCHECK(Children_.size() == 1);
        TExpression::Check();
    }

    virtual EColumnType Typecheck() const override
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
        return EColumnType::Integer;
    }

    TExpression* GetLhs() const
    {
        return Children_[Lhs_];
    }

    TExpression* GetRhs() const
    {
        return Children_[Rhs_];
    }

    DEFINE_BYVAL_RO_PROPERTY(EBinaryOp, Opcode);

protected:
    enum { Lhs_, Rhs_, NumberOfSubexpressions_ };

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

