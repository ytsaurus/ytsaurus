#pragma once

#include "cg_fragment_compiler.h"
#include "key_trie.h"

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
        EValueType type,
        const Stroka& name) const = 0;

    virtual TKeyTriePtr ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
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
        EValueType type,
        const Stroka& name) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IAggregateFunctionDescriptor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
