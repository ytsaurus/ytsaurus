#pragma once

#include "functions.h"
#include "user_defined_functions.h"

#include <core/misc/variant.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TCodegenFunction
    : public virtual IFunctionDescriptor
{
    virtual TCGValue CodegenValue(
        TCGContext& builder,
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name,
        Value* row) const = 0;

    virtual TCodegenExpression MakeCodegenExpr(
        TCodegenValue codegenFunctionContext,
        std::vector<TCodegenExpression> codegenArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const Stroka& name) const override;
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
        TCGContext& builder,
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name,
        Value* row) const override;
};

class TIsPrefixFunction
    : public TTypedFunction
    , public TExternallyDefinedFunction
{
public:
    TIsPrefixFunction();

    virtual TKeyTriePtr ExtractKeyRange(
        const TConstFunctionExpressionPtr& expr,
        const TKeyColumns& keyColumns,
        const TRowBufferPtr& rowBuffer) const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
