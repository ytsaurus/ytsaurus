#pragma once

#include "public.h"

#include "functions_common.h"
#include "cg_fragment_compiler.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IFunctionCodegen
    : public TRefCounted
{
    virtual TCodegenExpression Profile(
        TCGVariables* variables,
        std::vector<size_t> argIds,
        std::unique_ptr<bool[]> literalArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const TString& name,
        llvm::FoldingSetNodeID* id = nullptr) const = 0;

    virtual bool IsNullable(const std::vector<bool>& nullableArgs) const {
        return true;
    }

};

DEFINE_REFCOUNTED_TYPE(IFunctionCodegen)

struct IAggregateCodegen
    : public TRefCounted
{
    virtual TCodegenAggregate Profile(
        EValueType argumentType,
        EValueType stateType,
        EValueType resultType,
        const TString& name,
        llvm::FoldingSetNodeID* id = nullptr) const = 0;

};

DEFINE_REFCOUNTED_TYPE(IAggregateCodegen)

////////////////////////////////////////////////////////////////////////////////

struct ICallingConvention
    : public TRefCounted
{
    virtual TCGValue MakeCodegenFunctionCall(
        TCGBaseContext& baseBuilder,
        std::vector<TCodegenValue> codegenArguments,
        std::function<Value*(TCGBaseContext&, std::vector<Value*>)> codegenBody,
        EValueType type,
        const TString& name) const = 0;

    virtual llvm::FunctionType* GetCalleeType(
        TCGBaseContext& builder,
        std::vector<EValueType> argumentTypes,
        EValueType resultType,
        bool useFunctionContext) const = 0;

};

DEFINE_REFCOUNTED_TYPE(ICallingConvention)

class TUnversionedValueCallingConvention
    : public ICallingConvention
{
public:
    TUnversionedValueCallingConvention(int repeatedArgIndex);

    virtual TCGValue MakeCodegenFunctionCall(
        TCGBaseContext& baseBuilder,
        std::vector<TCodegenValue> codegenArguments,
        std::function<Value*(TCGBaseContext&, std::vector<Value*>)> codegenBody,
        EValueType type,
        const TString& name) const override;

    virtual llvm::FunctionType* GetCalleeType(
        TCGBaseContext& builder,
        std::vector<EValueType> argumentTypes,
        EValueType resultType,
        bool useFunctionContext) const override;

private:
    int RepeatedArgIndex_;
};

class TSimpleCallingConvention
    : public ICallingConvention
{
public:
    virtual TCGValue MakeCodegenFunctionCall(
        TCGBaseContext& baseBuilder,
        std::vector<TCodegenValue> codegenArguments,
        std::function<Value*(TCGBaseContext&, std::vector<Value*>)> codegenBody,
        EValueType type,
        const TString& name) const override;

    virtual llvm::FunctionType* GetCalleeType(
        TCGBaseContext& builder,
        std::vector<EValueType> argumentTypes,
        EValueType resultType,
        bool useFunctionContext) const override;
};

ICallingConventionPtr GetCallingConvention(
    ECallingConvention callingConvention,
    int repeatedArgIndex,
    TType repeatedArgType);

ICallingConventionPtr GetCallingConvention(ECallingConvention callingConvention);

////////////////////////////////////////////////////////////////////////////////

struct TExternalFunctionCodegen
    : public IFunctionCodegen
{
public:
    TExternalFunctionCodegen(
        const TString& functionName,
        const TString& symbolName,
        TSharedRef implementationFile,
        ICallingConventionPtr callingConvention,
        TSharedRef fingerprint,
        bool useFunctionContext = false)
        : FunctionName_(functionName)
        , SymbolName_(symbolName)
        , ImplementationFile_(implementationFile)
        , CallingConvention_(callingConvention)
        , Fingerprint_(fingerprint)
        , UseFunctionContext_(useFunctionContext)
    { }

    TExternalFunctionCodegen(
        const TString& functionName,
        const TString& symbolName,
        TSharedRef implementationFile,
        ECallingConvention callingConvention,
        TType repeatedArgType,
        int repeatedArgIndex,
        bool useFunctionContext,
        TSharedRef fingerprint)
        : TExternalFunctionCodegen(
            functionName,
            symbolName,
            implementationFile,
            GetCallingConvention(callingConvention, repeatedArgIndex, repeatedArgType),
            fingerprint,
            useFunctionContext)
    { }

    virtual TCodegenExpression Profile(
        TCGVariables* variables,
        std::vector<size_t> argIds,
        std::unique_ptr<bool[]> literalArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const TString& name,
        llvm::FoldingSetNodeID* id) const override;

private:
    const TString FunctionName_;
    const TString SymbolName_;
    const TSharedRef ImplementationFile_;
    const ICallingConventionPtr CallingConvention_;
    const TSharedRef Fingerprint_;
    const bool UseFunctionContext_;

};

struct TExternalAggregateCodegen
    : public IAggregateCodegen
{
public:
    TExternalAggregateCodegen(
        const TString& aggregateName,
        TSharedRef implementationFile,
        ECallingConvention callingConvention,
        TSharedRef fingerprint)
        : AggregateName_(aggregateName)
        , ImplementationFile_(implementationFile)
        , CallingConvention_(GetCallingConvention(callingConvention))
        , Fingerprint_(fingerprint)
    { }

    virtual TCodegenAggregate Profile(
        EValueType argumentType,
        EValueType stateType,
        EValueType resultType,
        const TString& name,
        llvm::FoldingSetNodeID* id) const override;

private:
    const TString AggregateName_;
    const TSharedRef ImplementationFile_;
    const ICallingConventionPtr CallingConvention_;
    const TSharedRef Fingerprint_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
