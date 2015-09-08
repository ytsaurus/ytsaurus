#pragma once

#include "cg_fragment_compiler.h"
#include "key_trie.h"

#include <core/misc/variant.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IFunctionDescriptor
    : public virtual TRefCounted
{
    virtual Stroka GetName() const = 0;

    virtual EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source) const = 0;

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const Stroka& name) const = 0;

    virtual TKeyTriePtr ExtractKeyRange(
        const TConstFunctionExpressionPtr& expr,
        const TKeyColumns& keyColumns,
        const TRowBufferPtr& rowBuffer) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFunctionDescriptor)

////////////////////////////////////////////////////////////////////////////////

struct IAggregateFunctionDescriptor
    : public virtual TRefCounted
{
    virtual Stroka GetName() const = 0;

    virtual const TCodegenAggregate MakeCodegenAggregate(
        EValueType argumentType,
        EValueType stateType,
        EValueType resultType,
        const Stroka& name) const = 0;

    virtual EValueType GetStateType(
        EValueType type) const = 0;

    virtual EValueType InferResultType(
        EValueType argumentType,
        const TStringBuf& source) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IAggregateFunctionDescriptor)

////////////////////////////////////////////////////////////////////////////////

class TUniversalRangeFunction
    : public virtual IFunctionDescriptor
{
    virtual TKeyTriePtr ExtractKeyRange(
        const TConstFunctionExpressionPtr& expr,
        const TKeyColumns& keyColumns,
        const TRowBufferPtr& rowBuffer) const override;
};

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
