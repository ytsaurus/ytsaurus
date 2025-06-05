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
        const std::vector<TLogicalTypePtr>& argumentTypes,
        const TLogicalTypePtr& type,
        const std::string& name,
        NCodegen::EExecutionBackend executionBackend,
        llvm::FoldingSetNodeID* id = nullptr) const = 0;

    virtual bool IsNullable(const std::vector<bool>& /*nullableArgs*/) const
    {
        return true;
    }
};

DEFINE_REFCOUNTED_TYPE(IFunctionCodegen)

struct IAggregateCodegen
    : public TRefCounted
{
    virtual TCodegenAggregate Profile(
        const std::vector<TLogicalTypePtr>& argumentTypes,
        const TLogicalTypePtr& stateType,
        const TLogicalTypePtr& resultType,
        const std::string& name,
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
        EValueType wireType,
        bool aggregate,
        const std::string& name) const = 0;

    virtual llvm::FunctionType* GetCalleeType(
        TCGBaseContext& builder,
        const std::vector<TLogicalTypePtr>& argumentTypes,
        const TLogicalTypePtr& resultType,
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
        EValueType wireType,
        bool aggregate,
        const std::string& name) const override;

    llvm::FunctionType* GetCalleeType(
        TCGBaseContext& builder,
        const std::vector<TLogicalTypePtr>& argumentTypes,
        const TLogicalTypePtr& resultType,
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
        EValueType wireType,
        bool aggregate,
        const std::string& name) const override;

    llvm::FunctionType* GetCalleeType(
        TCGBaseContext& builder,
        const std::vector<TLogicalTypePtr>& argumentTypes,
        const TLogicalTypePtr& resultType,
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
        const std::string& functionName,
        const std::string& symbolName,
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
        const std::string& functionName,
        const std::string& symbolName,
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
        const std::vector<TLogicalTypePtr>& argumentTypes,
        const TLogicalTypePtr& type,
        const std::string& name,
        NCodegen::EExecutionBackend executionBackend,
        llvm::FoldingSetNodeID* id) const override;

private:
    const std::string FunctionName_;
    const std::string SymbolName_;
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
        const std::string& aggregateName,
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
        const std::vector<TLogicalTypePtr>& argumentTypes,
        const TLogicalTypePtr& stateType,
        const TLogicalTypePtr& resultType,
        const std::string& name,
        NCodegen::EExecutionBackend executionBackend,
        llvm::FoldingSetNodeID* id) const override;

    bool IsFirst() const override;

private:
    const std::string AggregateName_;
    const TSharedRef ImplementationFile_;
    const ICallingConventionPtr CallingConvention_;
    const bool IsFirst_;
    const TSharedRef Fingerprint_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
