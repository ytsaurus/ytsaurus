#include "functions_cg.h"
#include "cg_fragment_compiler.h"

#include <yt/ytlib/table_client/llvm_types.h>
#include <yt/ytlib/table_client/row_base.h>

#include <yt/core/codegen/routine_registry.h>

#include <llvm/IR/DiagnosticPrinter.h>

#include <llvm/IRReader/IRReader.h>

#include <llvm/Linker/Linker.h>

#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/SourceMgr.h>

using namespace llvm;

namespace NYT {
namespace NQueryClient {

using NCodegen::MangleSymbol;
using NCodegen::DemangleSymbol;

////////////////////////////////////////////////////////////////////////////////

static const char* ExecutionContextStructName = "struct.TExpressionContext";
static const char* FunctionContextStructName = "struct.NYT::NQueryClient::TFunctionContext";
static const char* UnversionedValueStructName = "struct.TUnversionedValue";

namespace {
StringRef ToStringRef(const TString& stroka)
{
    return StringRef(stroka.c_str(), stroka.length());
}

StringRef ToStringRef(const TSharedRef& sharedRef)
{
    return StringRef(sharedRef.Begin(), sharedRef.Size());
}

TString ToString(llvm::Type* tp)
{
    std::string str;
    llvm::raw_string_ostream stream(str);
    tp->print(stream);
    return TString(stream.str());
}

Type* GetOpaqueType(
    TCGExprContext& builder,
    const char* name)
{
    auto existingType = builder.Module
        ->GetModule()
        ->getTypeByName(name);

    if (existingType) {
        return existingType;
    }

    return StructType::create(
        builder->getContext(),
        name);
}

void PushExecutionContext(
    TCGExprContext& builder,
    std::vector<Value*>& argumentValues)
{
    auto fullContext = builder.GetBuffer();
    auto contextType = GetOpaqueType(builder, ExecutionContextStructName);
    auto contextStruct = builder->CreateBitCast(
        fullContext,
        PointerType::getUnqual(contextType));
    argumentValues.push_back(contextStruct);
}

void PushFunctionContext(
    TCGExprContext& builder,
    Value* functionContext,
    std::vector<Value*>& argumentValues)
{
    auto contextType = GetOpaqueType(builder, FunctionContextStructName);
    auto contextStruct = builder->CreateBitCast(
        functionContext,
        PointerType::getUnqual(contextType));
    argumentValues.push_back(contextStruct);
}

////////////////////////////////////////////////////////////////////////////////

void CheckCallee(
    TCGBaseContext& builder,
    const TString& functionName,
    llvm::Function* callee,
    llvm::FunctionType* functionType)
{
    if (!callee) {
        THROW_ERROR_EXCEPTION(
            "Could not find LLVM bitcode for function %Qv",
            functionName);
    } else if (callee->arg_size() != functionType->getNumParams()) {
        THROW_ERROR_EXCEPTION(
            "Wrong number of arguments in LLVM bitcode for function %Qv: expected %v, got %v",
            functionName,
            functionType->getNumParams(),
            callee->arg_size());
    }

    if (callee->getReturnType() != functionType->getReturnType()) {
        THROW_ERROR_EXCEPTION(
            "Wrong result type in LLVM bitcode: expected %Qv, got %Qv",
            ToString(functionType->getReturnType()),
            ToString(callee->getReturnType()));
    }

    auto i = 1;
    auto expected = functionType->param_begin();
    for (
        auto actual = callee->arg_begin();
        expected != functionType->param_end();
        expected++, actual++, i++)
    {
        if (actual->getType() != *expected) {
            THROW_ERROR_EXCEPTION(
                "Wrong type for argument %v in LLVM bitcode for function %Qv: expected %Qv, got %Qv",
                i,
                functionName,
                ToString(*expected),
                ToString(actual->getType()));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void PushArgument(
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
    const TString& name,
    TCGExprContext& builder,
    Value* row)
{
    if (codegenArgs.empty()) {
        auto llvmResult = codegenBody(argumentValues);
        return codegenReturn(llvmResult);
    } else {
        auto currentArgValue = codegenArgs.back()(builder, row);
        codegenArgs.pop_back();

        PushArgument(argumentValues, currentArgValue);

        return CodegenIf<TCGExprContext, TCGValue>(
            builder,
            currentArgValue.IsNull(),
            [&] (TCGExprContext& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGExprContext& builder) {
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

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCodegenExpression TSimpleCallingConvention::MakeCodegenFunctionCall(
    TCodegenValue codegenFunctionContext,
    std::vector<TCodegenExpression> codegenArgs,
    std::function<Value*(std::vector<Value*>, TCGExprContext&)> codegenBody,
    EValueType type,
    const TString& name) const
{
    std::reverse(codegenArgs.begin(), codegenArgs.end());
    return [
        type,
        name,
        MOVE(codegenArgs),
        MOVE(codegenBody)
    ] (TCGExprContext& builder, Value* row) {
        auto llvmArgs = std::vector<Value*>();
        PushExecutionContext(builder, llvmArgs);

        auto callUdf = [&codegenBody, &builder] (std::vector<Value*> argValues) {
            return codegenBody(argValues, builder);
        };

        std::function<TCGValue(Value*)> codegenReturn;
        if (IsStringLikeType(type)) {
            auto resultPointer = builder->CreateAlloca(
                TDataTypeBuilder::get(
                    builder->getContext(),
                    EValueType::String));
            llvmArgs.push_back(resultPointer);

            auto resultLength = builder->CreateAlloca(
                TTypeBuilder::TLength::get(builder->getContext()));
            llvmArgs.push_back(resultLength);

            codegenReturn = [
                &,
                resultLength,
                resultPointer
            ] (Value* llvmResult) {
                return TCGValue::CreateFromValue(
                    builder,
                    builder->getFalse(),
                    builder->CreateLoad(resultLength),
                    builder->CreateLoad(resultPointer),
                    type,
                    Twine(name.c_str()));
            };
        } else {
            codegenReturn = [&] (Value* llvmResult) {
                    return TCGValue::CreateFromValue(
                        builder,
                        builder->getFalse(),
                        nullptr,
                        llvmResult,
                        type);
            };
        }

        auto codegenArgsCopy = codegenArgs;
        return PropagateNullArguments(
            codegenArgsCopy,
            llvmArgs,
            callUdf,
            codegenReturn,
            type,
            name,
            builder,
            row);
    };
}

llvm::FunctionType* TSimpleCallingConvention::GetCalleeType(
    TCGExprContext& builder,
    std::vector<EValueType> argumentTypes,
    EValueType resultType) const
{
    llvm::Type* calleeResultType;
    auto calleeArgumentTypes = std::vector<llvm::Type*>();
    calleeArgumentTypes.push_back(PointerType::getUnqual(
        GetOpaqueType(builder, ExecutionContextStructName)));

    if (IsStringLikeType(resultType)) {
        calleeResultType = builder->getVoidTy();
        calleeArgumentTypes.push_back(PointerType::getUnqual(builder->getInt8PtrTy()));
        calleeArgumentTypes.push_back(PointerType::getUnqual(builder->getInt32Ty()));

    } else {
        calleeResultType = TDataTypeBuilder::get(
            builder->getContext(),
            resultType);
    }

    for (auto type : argumentTypes) {
        if (IsStringLikeType(type)) {
            calleeArgumentTypes.push_back(builder->getInt8PtrTy());
            calleeArgumentTypes.push_back(builder->getInt32Ty());
        } else {
            calleeArgumentTypes.push_back(TDataTypeBuilder::get(
                builder->getContext(),
                type));
        }
    }

    return FunctionType::get(
        calleeResultType,
        ArrayRef<llvm::Type*>(calleeArgumentTypes),
        false);
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedValueCallingConvention::TUnversionedValueCallingConvention(
    int repeatedArgIndex,
    bool useFunctionContext)
    : RepeatedArgIndex_(repeatedArgIndex)
    , UseFunctionContext_(useFunctionContext)
{ }

TCodegenExpression TUnversionedValueCallingConvention::MakeCodegenFunctionCall(
    TCodegenValue codegenFunctionContext,
    std::vector<TCodegenExpression> codegenArgs,
    std::function<Value*(std::vector<Value*>, TCGExprContext&)> codegenBody,
    EValueType type,
    const TString& name) const
{
    return [=] (TCGExprContext& builder, Value* row) {
        auto unversionedValueType =
            llvm::TypeBuilder<TValue, false>::get(builder->getContext());
        auto unversionedValueOpaqueType = GetOpaqueType(
            builder,
            UnversionedValueStructName);

        auto argumentValues = std::vector<Value*>();

        PushExecutionContext(builder, argumentValues);

        if (UseFunctionContext_) {
            auto functionContext = codegenFunctionContext(builder);
            PushFunctionContext(builder, functionContext, argumentValues);
        }

        auto resultPtr = builder->CreateAlloca(unversionedValueType);
        auto castedResultPtr = builder->CreateBitCast(
            resultPtr,
            PointerType::getUnqual(unversionedValueOpaqueType));
        argumentValues.push_back(castedResultPtr);

        int argIndex = 0;
        auto arg = codegenArgs.begin();
        for (;
            arg != codegenArgs.end() && argIndex != RepeatedArgIndex_;
            arg++, argIndex++)
        {
            auto valuePtr = builder->CreateAlloca(unversionedValueType);
            auto cgValue = (*arg)(builder, row);
            cgValue.StoreToValue(builder, valuePtr, 0);

            auto castedValuePtr = builder->CreateBitCast(
                valuePtr,
                PointerType::getUnqual(unversionedValueOpaqueType));
            argumentValues.push_back(castedValuePtr);
        }

        if (argIndex == RepeatedArgIndex_) {
            auto varargSize = builder->getInt32(
                codegenArgs.size() - RepeatedArgIndex_);

            auto varargPtr = builder->CreateAlloca(
                unversionedValueType,
                varargSize);
            auto castedVarargPtr = builder->CreateBitCast(
                varargPtr,
                PointerType::getUnqual(unversionedValueOpaqueType));

            argumentValues.push_back(castedVarargPtr);
            argumentValues.push_back(varargSize);

            for (int varargIndex = 0; arg != codegenArgs.end(); arg++, varargIndex++) {
                auto valuePtr = builder->CreateConstGEP1_32(
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

llvm::FunctionType* TUnversionedValueCallingConvention::GetCalleeType(
    TCGExprContext& builder,
    std::vector<EValueType> argumentTypes,
    EValueType resultType) const
{
    llvm::Type* calleeResultType = builder->getVoidTy();

    auto calleeArgumentTypes = std::vector<llvm::Type*>();

    calleeArgumentTypes.push_back(PointerType::getUnqual(
        GetOpaqueType(builder, ExecutionContextStructName)));

    if (UseFunctionContext_) {
        calleeArgumentTypes.push_back(PointerType::getUnqual(
            GetOpaqueType(builder, FunctionContextStructName)));
    }

    calleeArgumentTypes.push_back(PointerType::getUnqual(
        GetOpaqueType(builder, UnversionedValueStructName)));

    auto positionalArgumentCount = RepeatedArgIndex_ == -1 ? argumentTypes.size() : RepeatedArgIndex_;
    for (auto index = 0; index < positionalArgumentCount; index++) {
        calleeArgumentTypes.push_back(PointerType::getUnqual(
            GetOpaqueType(builder, UnversionedValueStructName)));
    }
    if (RepeatedArgIndex_ != -1) {
        calleeArgumentTypes.push_back(PointerType::getUnqual(
            GetOpaqueType(builder, UnversionedValueStructName)));
        calleeArgumentTypes.push_back(builder->getInt32Ty());
    }

    return FunctionType::get(
        calleeResultType,
        ArrayRef<llvm::Type*>(calleeArgumentTypes),
        false);
}

////////////////////////////////////////////////////////////////////////////////

ICallingConventionPtr GetCallingConvention(
    ECallingConvention callingConvention,
    int repeatedArgIndex,
    TType repeatedArgType,
    bool useFunctionContext)
{
    switch (callingConvention) {
        case ECallingConvention::Simple:
            return New<TSimpleCallingConvention>();
        case ECallingConvention::UnversionedValue:
            if (repeatedArgType.TryAs<EValueType>()
                && repeatedArgType.As<EValueType>() == EValueType::Null)
            {
                return New<TUnversionedValueCallingConvention>(-1, useFunctionContext);
            } else {
                return New<TUnversionedValueCallingConvention>(repeatedArgIndex, useFunctionContext);
            }
        default:
            Y_UNREACHABLE();
    }
}

ICallingConventionPtr GetCallingConvention(ECallingConvention callingConvention)
{
    switch (callingConvention) {
        case ECallingConvention::Simple:
            return New<TSimpleCallingConvention>();
        case ECallingConvention::UnversionedValue:
            return New<TUnversionedValueCallingConvention>(-1);
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

void LoadLlvmBitcode(
    TCGBaseContext& builder,
    const TString& functionName,
    std::vector<TString> requiredSymbols,
    TSharedRef implementationFile)
{
    auto diag = SMDiagnostic();

    auto implBuffer = MemoryBufferRef(ToStringRef(implementationFile), StringRef("implementation"));
    auto implModule = parseIR(implBuffer, diag, builder->getContext());

    if (!implModule) {
        THROW_ERROR_EXCEPTION("Could not parse LLVM bitcode for function %Qv", functionName)
            << TErrorAttribute("line_no", diag.getLineNo())
            << TErrorAttribute("column_no", diag.getColumnNo())
            << TErrorAttribute("content", TStringBuf(diag.getLineContents().data(), diag.getLineContents().size()))
            << TErrorAttribute("message", TStringBuf(diag.getMessage().data(), diag.getMessage().size()));
    }

    for (const auto& symbol : requiredSymbols) {
        auto callee = implModule->getFunction(ToStringRef(symbol));
        if (!callee) {
            THROW_ERROR_EXCEPTION(
                "LLVM bitcode for function %Qv is missing required symbol %Qv",
                functionName,
                symbol);
        }
        callee->addFnAttr(Attribute::AttrKind::AlwaysInline);
    }

    for (auto& function : implModule->getFunctionList()) {
        auto name = function.getName();
        auto nameString = TString(name.begin(), name.size());
        if (builder.Module->SymbolIsLoaded(nameString)) {
            THROW_ERROR_EXCEPTION(
                "LLVM bitcode for function %Qv redefines already defined symbol %Qv",
                functionName,
                nameString);
        }
    }

    auto module = builder.Module->GetModule();

    std::string what;
    bool linkerFailed;
    {
        llvm::raw_string_ostream os(what);
        llvm::DiagnosticPrinterRawOStream printer(os);
        // Link two modules together, with the first module modified to be the
        // composite of the two input modules.
        linkerFailed = Linker::LinkModules(module, implModule.get(), [&] (const DiagnosticInfo& info) {
            info.print(printer);
        });
    }

    if (linkerFailed) {
        THROW_ERROR_EXCEPTION(
            "Error linking LLVM bitcode for function %Qv",
            functionName)
            << TErrorAttribute("message", what.c_str());
    }
}

void LoadLlvmFunctions(
    TCGBaseContext& builder,
    const TString& functionName,
    std::vector<std::pair<TString, llvm::FunctionType*>> functions,
    TSharedRef implementationFile)
{
    if (!implementationFile) {
        THROW_ERROR_EXCEPTION("UDF implementation is not available in this context");
    }

    if (builder.Module->FunctionIsLoaded(functionName)) {
        return;
    }

    auto requiredSymbols = std::vector<TString>();
    for (auto function : functions) {
        requiredSymbols.push_back(function.first);
    }

    LoadLlvmBitcode(
        builder,
        functionName,
        requiredSymbols,
        implementationFile);

    auto module = builder.Module->GetModule();

    builder.Module->AddLoadedFunction(functionName);
    for (const auto& function : functions) {
        auto callee = module->getFunction(ToStringRef(function.first));
        CheckCallee(
            builder,
            function.first,
            callee,
            function.second);
    }
}

////////////////////////////////////////////////////////////////////////////////

TCodegenExpression TExternalFunctionCodegen::Profile(
    TCodegenValue codegenFunctionContext,
    std::vector<TCodegenExpression> codegenArgs,
    std::vector<EValueType> argumentTypes,
    EValueType type,
    const TString& name,
    llvm::FoldingSetNodeID* id) const
{
    YCHECK(!ImplementationFile_.Empty());

    if (id) {
        id->AddString(ToStringRef(Fingerprint_));
    }

    auto codegenBody = [
        this_ = MakeStrong(this),
        MOVE(argumentTypes),
        type
    ] (std::vector<Value*> argumentValues, TCGExprContext& builder) {
        auto functionType = this_->CallingConvention_->GetCalleeType(
            builder,
            argumentTypes,
            type);

        LoadLlvmFunctions(
            builder,
            this_->FunctionName_,
            { std::make_pair(this_->SymbolName_, functionType) },
            this_->ImplementationFile_);

        auto callee = builder.Module->GetModule()->getFunction(
            ToStringRef(this_->SymbolName_));
        YCHECK(callee);

        auto result = builder->CreateCall(callee, argumentValues);
        return result;
    };

    return CallingConvention_->MakeCodegenFunctionCall(
        codegenFunctionContext,
        codegenArgs,
        codegenBody,
        type,
        name);
}

////////////////////////////////////////////////////////////////////////////////

TCodegenAggregate TExternalAggregateCodegen::Profile(
    EValueType argumentType,
    EValueType stateType,
    EValueType resultType,
    const TString& name,
    llvm::FoldingSetNodeID* id) const
{
    YCHECK(!ImplementationFile_.Empty());

    if (id) {
        id->AddString(ToStringRef(Fingerprint_));
    }

    auto initName = AggregateName_ + "_init";
    auto updateName = AggregateName_ + "_update";
    auto mergeName = AggregateName_ + "_merge";
    auto finalizeName = AggregateName_ + "_finalize";

    auto makeCodegenBody = [
        this_ = MakeStrong(this),
        argumentType,
        stateType,
        resultType,
        initName,
        updateName,
        mergeName,
        finalizeName
    ] (const TString& functionName) {
        return [
            this_,
            argumentType,
            stateType,
            resultType,
            functionName,
            initName,
            updateName,
            mergeName,
            finalizeName
        ] (std::vector<Value*> argumentValues, TCGExprContext& builder) {
            auto aggregateFunctions = std::vector<std::pair<TString, llvm::FunctionType*>>();

            auto initType = this_->CallingConvention_->GetCalleeType(
                builder,
                std::vector<EValueType>(),
                stateType);
            auto init = std::make_pair(initName, initType);

            auto updateType = this_->CallingConvention_->GetCalleeType(
                builder,
                std::vector<EValueType>{
                    stateType,
                    argumentType},
                stateType);
            auto update = std::make_pair(updateName, updateType);


            auto mergeType = this_->CallingConvention_->GetCalleeType(
                builder,
                std::vector<EValueType>{
                    stateType,
                    stateType},
                stateType);
            auto merge = std::make_pair(mergeName, mergeType);

            auto finalizeType = this_->CallingConvention_->GetCalleeType(
                builder,
                std::vector<EValueType>{stateType},
                resultType);
            auto finalize = std::make_pair(finalizeName, finalizeType);

            aggregateFunctions.push_back(init);
            aggregateFunctions.push_back(update);
            aggregateFunctions.push_back(merge);
            aggregateFunctions.push_back(finalize);

            LoadLlvmFunctions(
                builder,
                this_->AggregateName_,
                aggregateFunctions,
                this_->ImplementationFile_);

            auto callee = builder.Module->GetModule()->getFunction(ToStringRef(functionName));
            YCHECK(callee);

            return builder->CreateCall(callee, argumentValues);
        };
    };

    TCodegenAggregate codegenAggregate;
    codegenAggregate.Initialize = [
        this_ = MakeStrong(this),
        initName,
        argumentType,
        stateType,
        name,
        makeCodegenBody
    ] (TCGExprContext& builder, Value* row) {
        return this_->CallingConvention_->MakeCodegenFunctionCall(
            nullptr,
            std::vector<TCodegenExpression>(),
            makeCodegenBody(initName),
            stateType,
            name + "_init")(builder, row);
    };

    codegenAggregate.Update = [
        this_ = MakeStrong(this),
        updateName,
        argumentType,
        stateType,
        name,
        makeCodegenBody
    ] (TCGExprContext& builder, Value* aggState, Value* newValue) {
        auto codegenArgs = std::vector<TCodegenExpression>();
        codegenArgs.push_back([=] (TCGExprContext& builder, Value* row) {
            return TCGValue::CreateFromLlvmValue(
                builder,
                aggState,
                stateType);
        });
        codegenArgs.push_back([=] (TCGExprContext& builder, Value* row) {
            return TCGValue::CreateFromLlvmValue(
                builder,
                newValue,
                argumentType);
        });

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            nullptr,
            codegenArgs,
            makeCodegenBody(updateName),
            stateType,
            name + "_update")(builder, aggState);
    };

    codegenAggregate.Merge = [
        this_ = MakeStrong(this),
        mergeName,
        argumentType,
        stateType,
        name,
        makeCodegenBody
    ] (TCGExprContext& builder, Value* dstAggState, Value* aggState) {
        auto codegenArgs = std::vector<TCodegenExpression>();
        codegenArgs.push_back([=] (TCGExprContext& builder, Value* row) {
            return TCGValue::CreateFromLlvmValue(
                builder,
                dstAggState,
                stateType);
            });
        codegenArgs.push_back([=] (TCGExprContext& builder, Value* row) {
            return TCGValue::CreateFromLlvmValue(
                builder,
                aggState,
                stateType);
        });

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            nullptr,
            codegenArgs,
            makeCodegenBody(mergeName),
            stateType,
            name + "_merge")(builder, aggState);
    };

    codegenAggregate.Finalize = [
        this_ = MakeStrong(this),
        finalizeName,
        argumentType,
        stateType,
        name,
        makeCodegenBody
    ] (TCGExprContext& builder, Value* aggState) {
        auto codegenArgs = std::vector<TCodegenExpression>();
        codegenArgs.push_back([=] (TCGExprContext& builder, Value* row) {
            return TCGValue::CreateFromLlvmValue(
                builder,
                aggState,
                stateType);
        });

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            nullptr,
            codegenArgs,
            makeCodegenBody(finalizeName),
            argumentType,
            name + "_finalize")(builder, aggState);
    };

    return codegenAggregate;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
