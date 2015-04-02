#pragma once

#include "functions.h"
#include "builtin_functions.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TSimpleCallingConvention
    : public virtual IFunctionDescriptor
{
    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name) const override;

    virtual Value* LLVMValue(
        std::vector<Value*> argValues,
        TCGContext& builder) const = 0;
};

class TUserDefinedFunction
    : public TTypedFunction
    , public TUniversalRangeFunction
    , public TSimpleCallingConvention
{
public:
    TUserDefinedFunction(
        const Stroka& functionName,
        std::vector<EValueType> argumentTypes,
        EValueType resultType,
        TSharedRef implementationFile);

    virtual Value* LLVMValue(
        std::vector<Value*> argValues,
        TCGContext& builder) const;

private:
    Stroka FunctionName_;
    TSharedRef ImplementationFile_;
    EValueType ResultType_;
    std::vector<EValueType> ArgumentTypes_;

    Function* GetLLVMFunction(TCGContext& builder) const;
    void CheckCallee(Function* callee, TCGContext& builder) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
