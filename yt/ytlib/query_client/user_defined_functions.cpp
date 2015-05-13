#include "user_defined_functions.h"

#include "cg_fragment_compiler.h"
#include "plan_helpers.h"

#include <new_table_client/row_base.h>
#include <new_table_client/llvm_types.h>

#include <llvm/Object/ObjectFile.h>

#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/raw_ostream.h>

#include <llvm/IRReader/IRReader.h>

#include <llvm/Linker/Linker.h>

using namespace llvm;

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

static const char* ExecutionContextStructName = "struct.TExecutionContext";

static const char* UnversionedValueStructName = "struct.TUnversionedValue";

Stroka ToString(llvm::Type* tp)
{
    std::string str;
    llvm::raw_string_ostream stream(str);
    tp->print(stream);
    return Stroka(stream.str());
}

Type* GetOpaqueType(
    TCGContext& builder,
    const char* name)
{
    auto existingType = builder.Module
        ->GetModule()
        ->getTypeByName(name);

    if (existingType) {
        return existingType;
    }

    return StructType::create(
        builder.getContext(),
        name);
}

void PushExecutionContext(
    TCGContext& builder,
    std::vector<Value*>& argumentValues)
{
    auto fullContext = builder.GetExecutionContextPtr();
    auto contextType = GetOpaqueType(builder, ExecutionContextStructName);
    auto contextStruct = builder.CreateBitCast(
        fullContext,
        PointerType::getUnqual(contextType));
    argumentValues.push_back(contextStruct);
}

////////////////////////////////////////////////////////////////////////////////

void PushArgument(
    TCGContext& builder,
    std::vector<Value*>& argumentValues,
    TCGValue argumentValue)
{
    argumentValues.push_back(argumentValue.GetData());
    if (IsStringLikeType(argumentValue.GetStaticType())) {
        argumentValues.push_back(argumentValue.GetLength());
    }
}

TCGValue PropagateNullArguments(
    std::vector<TCodegenExpression>& codegenArgs,
    std::vector<Value*>& argumentValues,
    std::function<Value*(std::vector<Value*>)> codegenBody,
    std::function<TCGValue(Value*)> codegenReturn,
    EValueType type,
    const Stroka& name,
    TCGContext& builder,
    Value* row)
{
    if (codegenArgs.empty()) {
        auto llvmResult = codegenBody(argumentValues);
        return codegenReturn(llvmResult);
    } else {
        auto currentArgValue = codegenArgs.back()(builder, row);
        codegenArgs.pop_back();

        PushArgument(builder, argumentValues, currentArgValue);

        return CodegenIf<TCGContext, TCGValue>(
            builder,
            currentArgValue.IsNull(),
            [&] (TCGContext& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGContext& builder) {
                return PropagateNullArguments(
                    codegenArgs,
                    argumentValues,
                    std::move(codegenBody),
                    std::move(codegenReturn),
                    type,
                    name,
                    builder,
                    row);
            },
            Twine(name.c_str()));
    }
}

TCodegenExpression TSimpleCallingConvention::MakeCodegenFunctionCall(
    std::vector<TCodegenExpression> codegenArgs,
    std::function<Value*(std::vector<Value*>, TCGContext&)> codegenBody,
    EValueType type,
    const Stroka& name) const
{
    return [
        this_ = MakeStrong(this),
        type,
        name,
        MOVE(codegenArgs),
        MOVE(codegenBody)
    ] (TCGContext& builder, Value* row) mutable {
        std::reverse(
            codegenArgs.begin(),
            codegenArgs.end());

        auto llvmArgs = std::vector<Value*>();
        PushExecutionContext(builder, llvmArgs);

        auto callUdf = [&codegenBody, &builder] (std::vector<Value*> argValues) {
            return codegenBody(argValues, builder);
        };

        std::function<TCGValue(Value*)> codegenReturn;
        if (IsStringLikeType(type)) {
            auto resultPointer = builder.CreateAlloca(
                TDataTypeBuilder::get(
                    builder.getContext(),
                    EValueType::String));
            llvmArgs.push_back(resultPointer);

            auto resultLength = builder.CreateAlloca(
                TTypeBuilder::TLength::get(builder.getContext()));
            llvmArgs.push_back(resultLength);

            codegenReturn = [
                &,
                resultLength,
                resultPointer
            ] (Value* llvmResult) {
                return TCGValue::CreateFromValue(
                    builder,
                    builder.getFalse(),
                    builder.CreateLoad(resultLength),
                    builder.CreateLoad(resultPointer),
                    type,
                    Twine(name.c_str()));
            };
        } else {
            codegenReturn = [&] (Value* llvmResult) {
                    return TCGValue::CreateFromValue(
                        builder,
                        builder.getFalse(),
                        nullptr,
                        llvmResult,
                        type);
            };
        }

        return PropagateNullArguments(
            codegenArgs,
            llvmArgs,
            callUdf,
            codegenReturn,
            type,
            name,
            builder,
            row);
    };
}

void TSimpleCallingConvention::CheckResultType(
    const Stroka& functionName,
    Type* llvmType,
    TType resultType,
    TCGContext& builder) const
{
    auto concreteResultType = resultType.As<EValueType>();
    auto expectedResultType = TDataTypeBuilder::get(
        builder.getContext(),
        concreteResultType);
    if (IsStringLikeType(concreteResultType) &&
        llvmType != builder.getVoidTy())
    {
        THROW_ERROR_EXCEPTION(
            "Wrong result type in LLVM bitcode for function %Qv: expected void, got %Qv",
            functionName,
            ToString(llvmType));
    } else if (!IsStringLikeType(concreteResultType) &&
        llvmType != expectedResultType)
    {
        THROW_ERROR_EXCEPTION(
            "Wrong result type in LLVM bitcode: expected %Qv, got %Qv",
            ToString(expectedResultType),
            ToString(llvmType));
    }
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedValueCallingConvention::TUnversionedValueCallingConvention(
    int repeatedArgIndex)
    : RepeatedArgIndex_(repeatedArgIndex)
{ }

TCodegenExpression TUnversionedValueCallingConvention::MakeCodegenFunctionCall(
    std::vector<TCodegenExpression> codegenArgs,
    std::function<Value*(std::vector<Value*>, TCGContext&)> codegenBody,
    EValueType type,
    const Stroka& name) const
{
    return [=] (TCGContext& builder, Value* row) {
        auto unversionedValueType =
            llvm::TypeBuilder<TValue, false>::get(builder.getContext());
        auto unversionedValueOpaqueType = GetOpaqueType(
            builder,
            UnversionedValueStructName);

        auto argumentValues = std::vector<Value*>();

        PushExecutionContext(builder, argumentValues);

        auto resultPtr = builder.CreateAlloca(unversionedValueType);
        auto castedResultPtr = builder.CreateBitCast(
            resultPtr,
            PointerType::getUnqual(unversionedValueOpaqueType));
        argumentValues.push_back(castedResultPtr);

        int argIndex = 0;
        auto arg = codegenArgs.begin();
        for (;
            arg != codegenArgs.end() && argIndex != RepeatedArgIndex_;
            arg++, argIndex++) 
        {
            auto valuePtr = builder.CreateAlloca(unversionedValueType);
            auto cgValue = (*arg)(builder, row);
            cgValue.StoreToValue(builder, valuePtr, 0);

            auto castedValuePtr = builder.CreateBitCast(
                valuePtr,
                PointerType::getUnqual(unversionedValueOpaqueType));
            argumentValues.push_back(castedValuePtr);
        }

        if (argIndex == RepeatedArgIndex_) {
            auto varargSize = builder.getInt32(
                codegenArgs.size() - RepeatedArgIndex_);

            auto varargPtr = builder.CreateAlloca(
                unversionedValueType,
                varargSize);
            auto castedVarargPtr = builder.CreateBitCast(
                varargPtr,
                PointerType::getUnqual(unversionedValueOpaqueType));

            argumentValues.push_back(castedVarargPtr);
            argumentValues.push_back(varargSize);

            for (int varargIndex = 0; arg != codegenArgs.end(); arg++, varargIndex++) {
                auto valuePtr = builder.CreateConstGEP1_32(
                    varargPtr,
                    varargIndex);
                
                auto cgValue = (*arg)(builder, row);
                cgValue.StoreToValue(builder, valuePtr, 0);
            }
        }

        codegenBody(argumentValues, builder);

        return TCGValue::CreateFromLlvmValue(
            builder,
            resultPtr,
            type);
    };
}

void TUnversionedValueCallingConvention::CheckResultType(
    const Stroka& functionName,
    Type* llvmType,
    TType resultType,
    TCGContext& builder) const
{
    if (llvmType != builder.getVoidTy()) {
        THROW_ERROR_EXCEPTION(
            "Wrong result type in LLVM bitcode for function %Qv: expected void, got %Qv",
            functionName,
            ToString(llvmType));
    }
}

////////////////////////////////////////////////////////////////////////////////

TUserDefinedFunction::TUserDefinedFunction(
    const Stroka& functionName,
    std::vector<TType> argumentTypes,
    TType repeatedArgType,
    TType resultType,
    TSharedRef implementationFile,
    ICallingConventionPtr callingConvention)
    : TTypedFunction(
        functionName,
        std::vector<TType>(argumentTypes.begin(), argumentTypes.end()),
        repeatedArgType,
        resultType)
    , FunctionName_(functionName)
    , ImplementationFile_(implementationFile)
    , ResultType_(resultType)
    , ArgumentTypes_(argumentTypes)
    , CallingConvention_(callingConvention)
{ }

ICallingConventionPtr GetCallingConvention(
    ECallingConvention callingConvention)
{
    switch (callingConvention) {
        case ECallingConvention::Simple:
            return New<TSimpleCallingConvention>();
        case ECallingConvention::UnversionedValue:
            return New<TUnversionedValueCallingConvention>(-1);
        default:
            YUNREACHABLE();
    }
}

TUserDefinedFunction::TUserDefinedFunction(
    const Stroka& functionName,
    std::vector<TType> argumentTypes,
    TType resultType,
    TSharedRef implementationFile,
    ECallingConvention callingConvention)
    : TUserDefinedFunction(
        functionName,
        argumentTypes,
        EValueType::Null,
        resultType,
        implementationFile,
        GetCallingConvention(callingConvention))
{ }

TUserDefinedFunction::TUserDefinedFunction(
    const Stroka& functionName,
    std::vector<TType> argumentTypes,
    TType repeatedArgType,
    TType resultType,
    TSharedRef implementationFile)
    : TUserDefinedFunction(
        functionName,
        argumentTypes,
        repeatedArgType,
        resultType,
        implementationFile,
        New<TUnversionedValueCallingConvention>(argumentTypes.size()))
{ }

void TUserDefinedFunction::CheckCallee(
    llvm::Function* callee,
    TCGContext& builder,
    std::vector<Value*> argumentValues) const
{
    if (!callee) {
        THROW_ERROR_EXCEPTION(
            "Could not find LLVM bitcode for function %Qv",
            FunctionName_);
    } else if (callee->arg_size() != argumentValues.size()) {
        THROW_ERROR_EXCEPTION(
            "Wrong number of arguments in LLVM bitcode for function %Qv: expected %v, got %v",
            FunctionName_,
            argumentValues.size(),
            callee->arg_size());
    }

    CallingConvention_->CheckResultType(
        FunctionName_,
        callee->getReturnType(),
        ResultType_,
        builder);

    auto i = 1;
    auto expected = argumentValues.begin();
    for (
        auto actual = callee->arg_begin();
        expected != argumentValues.end();
        expected++, actual++, i++)
    {
        if (actual->getType() != (*expected)->getType()) {
            THROW_ERROR_EXCEPTION(
                "Wrong type for argument %v in LLVM bitcode for function %Qv: expected %Qv, got %Qv",
                i,
                FunctionName_,
                ToString((*expected)->getType()),
                ToString(actual->getType()));
        }
    }
}

Function* GetLlvmFunction(
    TCGContext& builder,
    const Stroka& functionName,
    std::vector<Value*> argumentValues,
    TSharedRef implementationFile,
    std::function<void(Function*, std::vector<Value*>)> checkCallee)
{
    auto module = builder.Module->GetModule();
    auto callee = module->getFunction(StringRef(functionName));
    if (!callee) {
        auto diag = SMDiagnostic();
        auto buffer = MemoryBufferRef(
            StringRef(implementationFile.Begin(), implementationFile.Size()),
            StringRef("impl"));
        auto implModule = parseIR(buffer, diag, builder.getContext());

        if (!implModule) {
            THROW_ERROR_EXCEPTION(
                "Error parsing LLVM bitcode for function %Qv",
                functionName)
                << TError(Stroka(diag.getMessage().str()));
        }

        // Link two modules together, with the first module modified to be the
        // composite of the two input modules.
        auto linkError = Linker::LinkModules(module, implModule.get());

        if (linkError) {
            THROW_ERROR_EXCEPTION(
                "Error linking LLVM bitcode for function %Qv",
                functionName);
        }

        callee = module->getFunction(StringRef(functionName));
        checkCallee(callee, argumentValues);
    }
    return callee;
}

TCodegenExpression TUserDefinedFunction::MakeCodegenExpr(
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    const Stroka& name) const
{
    auto codegenBody = [
        this_ = MakeStrong(this)
    ] (std::vector<Value*> argumentValues, TCGContext& builder) {

        auto callee = GetLlvmFunction(
            builder,
            this_->FunctionName_,
            argumentValues,
            this_->ImplementationFile_,
            [&] (Function* callee, std::vector<Value*> arguments) {
                this_->CheckCallee(callee, builder, arguments);
            });
        auto result = builder.CreateCall(callee, argumentValues);
        return result;
    };

    return CallingConvention_->MakeCodegenFunctionCall(
        codegenArgs,
        codegenBody,
        type,
        name);
}

////////////////////////////////////////////////////////////////////////////////

TUserDefinedAggregateFunction::TUserDefinedAggregateFunction(
    const Stroka& aggregateName,
    EValueType resultType,
    EValueType stateType,
    TSharedRef implementationFile,
    ICallingConventionPtr callingConvention)
    : AggregateName_(aggregateName)
    , ResultType_(resultType)
    , StateType_(stateType)
    , ImplementationFile_(implementationFile)
    , CallingConvention_(callingConvention)
{ }

TUserDefinedAggregateFunction::TUserDefinedAggregateFunction(
    const Stroka& aggregateName,
    EValueType resultType,
    EValueType stateType,
    TSharedRef implementationFile,
    ECallingConvention callingConvention)
    : TUserDefinedAggregateFunction(
        aggregateName,
        resultType,
        stateType,
        implementationFile,
        GetCallingConvention(callingConvention))
{ }

Stroka TUserDefinedAggregateFunction::GetName() const
{
    return AggregateName_;
}

const TCodegenAggregate TUserDefinedAggregateFunction::MakeCodegenAggregate(
    EValueType type,
    const Stroka& name) const
{
    //TODO
    TCodegenAggregate codegenAggregate;
    codegenAggregate.Initialize = [] (TCGContext& builder, Value* aggState) {

    };
    codegenAggregate.Update = [] (TCGContext& builder, Value* aggState, Value* newValue) {

    };
    codegenAggregate.Merge = [] (TCGContext& builder, Value* dstAggState, Value* aggState) {

    };
    codegenAggregate.Finalize = [] (TCGContext& builder, Value* result, Value* aggState) {

    };
}

EValueType TUserDefinedAggregateFunction::GetStateType(
    EValueType type) const
{
    return StateType_;
}

EValueType TUserDefinedAggregateFunction::InferResultType(
    EValueType argumentType,
    const TStringBuf& source) const
{
    //TODO
    return ResultType_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
