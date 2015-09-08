#pragma once

#include "functions.h"
#include "udf_descriptor.h"

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

    virtual llvm::FunctionType* GetCalleeType(
        TCGContext& builder,
        std::vector<EValueType> argumentTypes,
        EValueType resultType) const = 0;

    static void CheckCallee(
        TCGContext& builder,
        const Stroka& functionName,
        llvm::Function* callee,
        llvm::FunctionType* functionType);
};

DEFINE_REFCOUNTED_TYPE(ICallingConvention);

class TUnversionedValueCallingConvention
    : public ICallingConvention
{
public:
    TUnversionedValueCallingConvention(int repeatedArgIndex);

    virtual TCodegenExpression MakeCodegenFunctionCall(
        std::vector<TCodegenExpression> codegenArgs,
        std::function<Value*(std::vector<Value*>, TCGContext&)> codegenBody,
        EValueType type,
        const Stroka& name) const override;

    virtual llvm::FunctionType* GetCalleeType(
        TCGContext& builder,
        std::vector<EValueType> argumentTypes,
        EValueType resultType) const override;

private:
    int RepeatedArgIndex_;
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

    virtual llvm::FunctionType* GetCalleeType(
        TCGContext& builder,
        std::vector<EValueType> argumentTypes,
        EValueType resultType) const override;
};

ICallingConventionPtr GetCallingConvention(
    ECallingConvention callingConvention,
    int repeatedArgIndex,
    TType repeatedArgType);

////////////////////////////////////////////////////////////////////////////////

class TExternallyDefinedFunction
    : public virtual IFunctionDescriptor
{
public:
    TExternallyDefinedFunction(
        const Stroka& functionName,
        const Stroka& symbolName,
        TSharedRef implementationFile,
        ICallingConventionPtr callingConvention)
        : FunctionName_(functionName)
        , SymbolName_(symbolName)
        , ImplementationFile_(implementationFile)
        , CallingConvention_(callingConvention)
    { }

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const Stroka& name) const override;

private:
    Stroka FunctionName_;
    Stroka SymbolName_;
    TSharedRef ImplementationFile_;
    ICallingConventionPtr CallingConvention_;

};

////////////////////////////////////////////////////////////////////////////////

class TUserDefinedFunction
    : public TTypedFunction
    , public TUniversalRangeFunction
    , public TExternallyDefinedFunction
{
public:
    TUserDefinedFunction(
        const Stroka& functionName,
        std::vector<TType> argumentTypes,
        TType resultType,
        TSharedRef implementationFile,
        ECallingConvention callingConvention);

    TUserDefinedFunction(
        const Stroka& functionName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TSharedRef implementationFile);

    TUserDefinedFunction(
        const Stroka& functionName,
        const Stroka& symbolName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TSharedRef implementationFile);

private:
    TUserDefinedFunction(
        const Stroka& functionName,
        const Stroka& symbolName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TSharedRef implementationFile,
        ICallingConventionPtr callingConvention);
};

////////////////////////////////////////////////////////////////////////////////

class TUserDefinedAggregateFunction
    : public IAggregateFunctionDescriptor
{
public:
    TUserDefinedAggregateFunction(
        const Stroka& aggregateName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        TType argumentType,
        TType resultType,
        TType stateType,
        TSharedRef implementationFile,
        ECallingConvention callingConvention);

    virtual Stroka GetName() const override;

    virtual const TCodegenAggregate MakeCodegenAggregate(
        EValueType argumentType,
        EValueType stateType,
        EValueType resultType,
        const Stroka& name) const override;

    virtual EValueType GetStateType(
        EValueType type) const override;

    virtual EValueType InferResultType(
        EValueType argumentType,
        const TStringBuf& source) const override;

private:
    Stroka AggregateName_;
    std::unordered_map<TTypeArgument, TUnionType> TypeArgumentConstraints_;
    TType ArgumentType_;
    TType ResultType_;
    TType StateType_;
    TSharedRef ImplementationFile_;
    ICallingConventionPtr CallingConvention_;

    TUserDefinedAggregateFunction(
        const Stroka& aggregateName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        TType argumentType,
        TType resultType,
        TType stateType,
        TSharedRef implementationFile,
        ICallingConventionPtr callingConvention);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
