#pragma once

#include "functions.h"

#include <core/misc/variant.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef int TTypeArgument;
typedef std::vector<EValueType> TUnionType;
typedef TVariant<EValueType, TTypeArgument, TUnionType> TType;

EValueType TypingFunction(
    const std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    const std::vector<TType>& expectedArgTypes,
    TType repeatedArgType,
    TType resultType,
    const Stroka& functionName,
    const std::vector<EValueType>& argTypes,
    const TStringBuf& source);

class TTypedFunction
    : public virtual IFunctionDescriptor
{
public:
    TTypedFunction(
        const Stroka& functionName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType);

    TTypedFunction(
        const Stroka& functionName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType resultType);

    virtual Stroka GetName() const override;

    virtual EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source) const override;

private:
    Stroka FunctionName_;
    std::unordered_map<TTypeArgument, TUnionType> TypeArgumentConstraints_;
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
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const Stroka& name) const override;
};

class TUniversalRangeFunction
    : public virtual IFunctionDescriptor
{
    virtual TKeyTriePtr ExtractKeyRange(
        const TConstFunctionExpressionPtr& expr,
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
        const TConstFunctionExpressionPtr& expr,
        const TKeyColumns& keyColumns,
        const TRowBufferPtr& rowBuffer) const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
