#include <iostream>
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

Stroka LLVMTypeToString(llvm::Type* tp)
{
    std::string str;
    llvm::raw_string_ostream stream(str);
    tp->print(stream);
    return Stroka(stream.str());
}

////////////////////////////////////////////////////////////////////////////////

std::vector<Value*> SplitStringArguments(
    TCGValue argumentValue,
    TCGContext& builder)
{
    if (IsStringLikeType(argumentValue.GetStaticType())) {
        return std::vector<Value*>{
            argumentValue.GetData(),
            argumentValue.GetLength()};
    } else {
        return std::vector<Value*>{argumentValue.GetData()};
    }
}

TCGValue PropagateNullArguments(
    std::vector<TCodegenExpression> codegenArgs,
    std::vector<Value*> initialArgumentValues,
    std::function<Value*(std::vector<Value*>)> codegenBody,
    std::function<TCGValue(Value*)> codegenReturn,
    EValueType type,
    const Stroka& name,
    TCGContext& builder,
    Value* row)
{
    if (codegenArgs.empty()) {
        auto llvmResult = codegenBody(initialArgumentValues);
        return codegenReturn(llvmResult);
    } else {
        auto currentCodegenArg = codegenArgs.back();
        auto currentArgValue = currentCodegenArg(builder, row);
            
        auto splitArgumentValue = SplitStringArguments(currentArgValue, builder);

        auto newCodegenArgs = codegenArgs;
        newCodegenArgs.pop_back();
        auto newArgumentValues = initialArgumentValues;
        newArgumentValues.insert(
            newArgumentValues.end(),
            splitArgumentValue.begin(),
            splitArgumentValue.end());

        return CodegenIf<TCGContext, TCGValue>(
            builder,
            currentArgValue.IsNull(),
            [&] (TCGContext& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGContext& builder) {
                return PropagateNullArguments(
                    newCodegenArgs,
                    newArgumentValues,
                    codegenBody,
                    codegenReturn,
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
        codegenArgs,
        codegenBody
    ] (TCGContext& builder, Value* row) {
        auto resultPointer = builder.CreateAlloca(
            TDataTypeBuilder::get(
                builder.getContext(),
                EValueType::String));
        auto resultLength = builder.CreateAlloca(
            TTypeBuilder::TLength::get(builder.getContext()));

        auto llvmArgs = std::vector<Value*>();
        if (IsStringLikeType(type)) {
            llvmArgs.push_back(resultPointer);
            llvmArgs.push_back(resultLength);
        }

        auto callUdf = [&] (std::vector<Value*> argValues) {
            return codegenBody(argValues, builder);
        };

        auto codegenReturn = [&] (Value* llvmResult) {
            if (IsStringLikeType(type)) {
                return TCGValue::CreateFromValue(
                    builder,
                    builder.getFalse(),
                    builder.CreateLoad(resultLength),
                    builder.CreateLoad(resultPointer),
                    type,
                    Twine(name.c_str()));
            } else {
                return TCGValue::CreateFromValue(
                    builder,
                    builder.getFalse(),
                    nullptr,
                    llvmResult,
                    type);
            }
        };

        auto reversedCodegenArgs = codegenArgs;
        std::reverse(
            reversedCodegenArgs.begin(),
            reversedCodegenArgs.end());

        return PropagateNullArguments(
            reversedCodegenArgs,
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
    Type* llvmType,
    EValueType resultType,
    TCGContext& builder) const
{
    auto expectedResultType = TDataTypeBuilder::get(
        builder.getContext(),
        resultType);
    if (IsStringLikeType(resultType) &&
        llvmType != builder.getVoidTy())
    {
        THROW_ERROR_EXCEPTION(
            "Wrong result type in LLVM bitcode: expected void, got %Qv",
            LLVMTypeToString(llvmType));
    } else if (resultType != EValueType::String &&
        llvmType != expectedResultType)
    {
        THROW_ERROR_EXCEPTION(
            "Wrong result type in LLVM bitcode: expected %Qv, got %Qv",
            LLVMTypeToString(expectedResultType),
            LLVMTypeToString(llvmType));
    }
}

////////////////////////////////////////////////////////////////////////////////

TCodegenExpression TUnversionedValueCallingConvention::MakeCodegenFunctionCall(
    std::vector<TCodegenExpression> codegenArgs,
    std::function<Value*(std::vector<Value*>, TCGContext&)> codegenBody,
    EValueType type,
    const Stroka& name) const
{
    return [=] (TCGContext& builder, Value* row) {
        auto argumentValues = std::vector<Value*>();

        auto unversionedValueType =
            llvm::TypeBuilder<TUnversionedValue, false>::get(builder.getContext());

        auto unversionedValueStruct = StructType::create(
            builder.getContext(),
            "struct.TUnversionedValue");

        auto resultPtr = builder.CreateAlloca(unversionedValueType);
        auto castedResultPtr = builder.CreateBitCast(
            resultPtr,
            PointerType::getUnqual(unversionedValueStruct));
        argumentValues.push_back(castedResultPtr);

        for (auto arg = codegenArgs.begin(); arg != codegenArgs.end(); arg++) {
            auto valuePtr = builder.CreateAlloca(unversionedValueType);
            auto cgValue = (*arg)(builder, row);
            cgValue.StoreToValue(builder, valuePtr, 0);

            auto castedValuePtr = builder.CreateBitCast(
                valuePtr,
                PointerType::getUnqual(unversionedValueStruct));
            argumentValues.push_back(castedValuePtr);
        }

        codegenBody(argumentValues, builder);

        return TCGValue::CreateFromLLVMValue(
            builder,
            resultPtr,
            type);
    };
}

void TUnversionedValueCallingConvention::CheckResultType(
    Type* llvmType,
    EValueType resultType,
    TCGContext& builder) const
{
}

////////////////////////////////////////////////////////////////////////////////

TUserDefinedFunction::TUserDefinedFunction(
    const Stroka& functionName,
    std::vector<EValueType> argumentTypes,
    EValueType resultType,
    TSharedRef implementationFile,
    ICallingConventionPtr callingConvention)
    : TTypedFunction(
        functionName,
        std::vector<TType>(argumentTypes.begin(), argumentTypes.end()),
        resultType)
    , FunctionName_(functionName)
    , ImplementationFile_(implementationFile)
    , ResultType_(resultType)
    , ArgumentTypes_(argumentTypes)
    , CallingConvention_(callingConvention)
{ }

void TUserDefinedFunction::CheckCallee(
    llvm::Function* callee,
    TCGContext& builder,
    std::vector<Value*> argumentValues) const
{
    if (callee == nullptr) {
        THROW_ERROR_EXCEPTION(
            "Could not find LLVM bitcode for %Qv",
            FunctionName_);
    } else if (callee->arg_size() != argumentValues.size()) {
        THROW_ERROR_EXCEPTION(
            "Wrong number of arguments in LLVM bitcode: expected %v, got %v",
            argumentValues.size(),
            callee->arg_size());
    }

    CallingConvention_->CheckResultType(
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
                "Wrong type for argument %v in LLVM bitcode: expected %Qv, got %Qv",
                i,
                LLVMTypeToString((*expected)->getType()),
                LLVMTypeToString(actual->getType()));
        }
    }
}

Function* TUserDefinedFunction::GetLLVMFunction(TCGContext& builder) const
{
    auto module = builder.Module->GetModule();
    auto callee = module->getFunction(StringRef(FunctionName_));
    if (!callee) {
        auto diag = SMDiagnostic();
        auto buffer = MemoryBufferRef(
            StringRef(ImplementationFile_.Begin(), ImplementationFile_.Size()),
            StringRef("impl"));
        auto implModule = parseIR(buffer, diag, builder.getContext());

        if (!implModule) {
            THROW_ERROR_EXCEPTION(
                "Error parsing LLVM bitcode: %v")
                << TError(Stroka(diag.getMessage().str()));
        }

        Linker::LinkModules(module, implModule.get());
        callee = module->getFunction(StringRef(FunctionName_));
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
        auto callee = this_->GetLLVMFunction(builder);
        this_->CheckCallee(callee, builder, argumentValues);
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

} // namespace NQueryClient
} // namespace NYT
