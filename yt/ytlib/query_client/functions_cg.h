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
        TCodegenValue codegenFunctionContext,
        std::vector<TCodegenExpression> codegenArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const TString& name,
        llvm::FoldingSetNodeID* id = nullptr) const = 0;

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
    virtual TCodegenExpression MakeCodegenFunctionCall(
        TCodegenValue codegenFunctionContext,
        std::vector<TCodegenExpression> codegenArgs,
        std::function<Value*(std::vector<Value*>, TCGExprContext&)> codegenBody,
        EValueType type,
        const TString& name) const = 0;

    virtual llvm::FunctionType* GetCalleeType(
        TCGExprContext& builder,
        std::vector<EValueType> argumentTypes,
        EValueType resultType) const = 0;

};

DEFINE_REFCOUNTED_TYPE(ICallingConvention)

class TUnversionedValueCallingConvention
    : public ICallingConvention
{
public:
    TUnversionedValueCallingConvention(int repeatedArgIndex, bool useFunctionContext = false);

    virtual TCodegenExpression MakeCodegenFunctionCall(
        TCodegenValue codegenFunctionContext,
        std::vector<TCodegenExpression> codegenArgs,
        std::function<Value*(std::vector<Value*>, TCGExprContext&)> codegenBody,
        EValueType type,
        const TString& name) const override;

    virtual llvm::FunctionType* GetCalleeType(
        TCGExprContext& builder,
        std::vector<EValueType> argumentTypes,
        EValueType resultType) const override;

private:
    int RepeatedArgIndex_;
    bool UseFunctionContext_;
};

class TSimpleCallingConvention
    : public ICallingConvention
{
public:
    virtual TCodegenExpression MakeCodegenFunctionCall(
        TCodegenValue codegenFunctionContext,
        std::vector<TCodegenExpression> codegenArgs,
        std::function<Value*(std::vector<Value*>, TCGExprContext&)> codegenBody,
        EValueType type,
        const TString& name) const override;

    virtual llvm::FunctionType* GetCalleeType(
        TCGExprContext& builder,
        std::vector<EValueType> argumentTypes,
        EValueType resultType) const override;
};

ICallingConventionPtr GetCallingConvention(
    ECallingConvention callingConvention,
    int repeatedArgIndex,
    TType repeatedArgType,
    bool useFunctionContext);

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
        TSharedRef fingerprint)
        : FunctionName_(functionName)
        , SymbolName_(symbolName)
        , ImplementationFile_(implementationFile)
        , CallingConvention_(callingConvention)
        , Fingerprint_(fingerprint)
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
            GetCallingConvention(callingConvention, repeatedArgIndex, repeatedArgType, useFunctionContext),
            fingerprint)
    { }

    virtual TCodegenExpression Profile(
        TCodegenValue codegenFunctionContext,
        std::vector<TCodegenExpression> codegenArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const TString& name,
        llvm::FoldingSetNodeID* id) const override;

private:
    TString FunctionName_;
    TString SymbolName_;
    TSharedRef ImplementationFile_;
    ICallingConventionPtr CallingConvention_;
    TSharedRef Fingerprint_;

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
    TString AggregateName_;
    TSharedRef ImplementationFile_;
    ICallingConventionPtr CallingConvention_;
    TSharedRef Fingerprint_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
