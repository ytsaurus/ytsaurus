#pragma once

#include "functions.h"

#include <core/misc/variant.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef int TTypeArgument;
typedef std::vector<EValueType> TUnionType;
typedef TVariant<EValueType, TTypeArgument, TUnionType> TType;

class TTypedFunction
    : public virtual IFunctionDescriptor
{
public:
    TTypedFunction(
        const Stroka& functionName,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType);

    TTypedFunction(
        const Stroka& functionName,
        std::vector<TType> argumentTypes,
        TType resultType);

    virtual Stroka GetName() const override;

    virtual EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source) const override;

private:
    EValueType TypingFunction(
        const std::vector<TType>& expectedArgTypes,
        TType repeatedArgType,
        TType resultType,
        const Stroka& functionName,
        const std::vector<EValueType>& argTypes,
        const TStringBuf& source) const;

    Stroka FunctionName_;
    std::vector<TType> ArgumentTypes_;
    TType RepeatedArgumentType_;
    TType ResultType_;
};

class TCodegenFunction
    : public virtual IFunctionDescriptor
{
    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name,
        TCGContext& builder,
        Value* row) const = 0;

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name) const override;
};

class TUniversalRangeFunction
    : public virtual IFunctionDescriptor
{
    virtual TKeyTriePtr ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
        const TKeyColumns& keyColumns,
        const TRowBufferPtr& rowBuffer) const override;
};

////////////////////////////////////////////////////////////////////////////////

class TIfFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    TIfFunction();

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name,
        TCGContext& builder,
        Value* row) const override;
};

class TIsPrefixFunction
    : public TTypedFunction
    , public TCodegenFunction
{
public:
    TIsPrefixFunction();

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name,
        TCGContext& builder,
        Value* row) const override;

    virtual TKeyTriePtr ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
        const TKeyColumns& keyColumns,
        const TRowBufferPtr& rowBuffer) const override;
};

class TCastFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    TCastFunction(
        EValueType resultType,
        const Stroka& functionName);

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name,
        TCGContext& builder,
        Value* row) const override;

private:
    static const TUnionType CastTypes_;
};

////////////////////////////////////////////////////////////////////////////////

class TAggregateFunction
    : public IAggregateFunctionDescriptor
{
public:
    TAggregateFunction(Stroka name);

    virtual Stroka GetName() const override;

private:
    const Stroka Name_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
