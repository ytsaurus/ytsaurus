#pragma once

#include "functions.h"
#include "builtin_functions.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct ICallingConvention
    : public TRefCounted
{
    virtual TCodegenExpression MakeCodegenFunctionCall(
        std::vector<TCodegenExpression> codegenArgs,
        std::function<Value*(std::vector<Value*>, TCGContext&)> codegenBody,
        EValueType type,
        const Stroka& name) const = 0;

    virtual void CheckResultType(
        Type* llvmType,
        EValueType resultType,
        TCGContext& builder) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ICallingConvention);

class TUnversionedValueCallingConvention
    : public ICallingConvention
{
public:
    virtual TCodegenExpression MakeCodegenFunctionCall(
        std::vector<TCodegenExpression> codegenArgs,
        std::function<Value*(std::vector<Value*>, TCGContext&)> codegenBody,
        EValueType type,
        const Stroka& name) const override;

    void CheckResultType(
        Type* llvmType,
        EValueType resultType,
        TCGContext& builder) const override;
};

class TSimpleCallingConvention
    : public ICallingConvention
{
public:
    virtual TCodegenExpression MakeCodegenFunctionCall(
        std::vector<TCodegenExpression> codegenArgs,
        std::function<Value*(std::vector<Value*>, TCGContext&)> codegenBody,
        EValueType type,
        const Stroka& name) const override;

    void CheckResultType(
        Type* llvmType,
        EValueType resultType,
        TCGContext& builder) const override;
};

class TUserDefinedFunction
    : public TTypedFunction
    , public TUniversalRangeFunction
{
public:
    TUserDefinedFunction(
        const Stroka& functionName,
        std::vector<EValueType> argumentTypes,
        EValueType resultType,
        TSharedRef implementationFile,
        ICallingConventionPtr callingConvention = New<TSimpleCallingConvention>());

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name) const override;

private:
    Stroka FunctionName_;
    TSharedRef ImplementationFile_;
    EValueType ResultType_;
    std::vector<EValueType> ArgumentTypes_;
    ICallingConventionPtr CallingConvention_;

    Function* GetLlvmFunction(TCGContext& builder) const;
    void CheckCallee(
        Function* callee,
        TCGContext& builder,
        std::vector<Value*> argumentValues) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
