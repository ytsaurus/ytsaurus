#pragma once

#include "cg_fragment_compiler.h"

#include <yt/yt/library/query/base/functions_common.h>

#include <yt/yt/library/query/engine_api/public.h>

namespace NYT::NQueryClient {

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
        NCodegen::EExecutionBackend executionBackend,
        llvm::FoldingSetNodeID* id = nullptr) const = 0;

    virtual bool IsNullable(const std::vector<bool>& /*nullableArgs*/) const {
        return true;
    }
};

DEFINE_REFCOUNTED_TYPE(IFunctionCodegen)

struct IAggregateCodegen
    : public TRefCounted
{
    virtual TCodegenAggregate Profile(
        std::vector<EValueType> argumentTypes,
        EValueType stateType,
        EValueType resultType,
        const TString& name,
        NCodegen::EExecutionBackend executionBackend,
        llvm::FoldingSetNodeID* id = nullptr) const = 0;

    virtual bool IsFirst() const = 0;
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
        bool aggregate,
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

    TCGValue MakeCodegenFunctionCall(
        TCGBaseContext& baseBuilder,
        std::vector<TCodegenValue> codegenArguments,
        std::function<Value*(TCGBaseContext&, std::vector<Value*>)> codegenBody,
        EValueType type,
        bool aggregate,
        const TString& name) const override;

    llvm::FunctionType* GetCalleeType(
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
    TCGValue MakeCodegenFunctionCall(
        TCGBaseContext& baseBuilder,
        std::vector<TCodegenValue> codegenArguments,
        std::function<Value*(TCGBaseContext&, std::vector<Value*>)> codegenBody,
        EValueType type,
        bool aggregate,
        const TString& name) const override;

    llvm::FunctionType* GetCalleeType(
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

    TCodegenExpression Profile(
        TCGVariables* variables,
        std::vector<size_t> argIds,
        std::unique_ptr<bool[]> literalArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const TString& name,
        NCodegen::EExecutionBackend executionBackend,
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
        bool isFirst,
        TSharedRef fingerprint)
        : AggregateName_(aggregateName)
        , ImplementationFile_(implementationFile)
        , CallingConvention_(GetCallingConvention(callingConvention))
        , IsFirst_(isFirst)
        , Fingerprint_(fingerprint)
    { }

    TCodegenAggregate Profile(
        std::vector<EValueType> argumentTypes,
        EValueType stateType,
        EValueType resultType,
        const TString& name,
        NCodegen::EExecutionBackend executionBackend,
        llvm::FoldingSetNodeID* id) const override;

    bool IsFirst() const override;

private:
    const TString AggregateName_;
    const TSharedRef ImplementationFile_;
    const ICallingConventionPtr CallingConvention_;
    const bool IsFirst_;
    const TSharedRef Fingerprint_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
