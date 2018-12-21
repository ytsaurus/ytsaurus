#include "functions_cg.h"
#include "cg_fragment_compiler.h"

#include <yt/ytlib/table_client/llvm_types.h>

#include <yt/client/table_client/row_base.h>

#include <yt/core/codegen/llvm_migrate_helpers.h>
#include <yt/core/codegen/routine_registry.h>

#include <llvm/IR/DiagnosticPrinter.h>
#include <llvm/IR/DiagnosticInfo.h>

#include <llvm/IRReader/IRReader.h>

#include <llvm/Linker/Linker.h>

#include <llvm-c/Linker.h>

#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/SourceMgr.h>

using namespace llvm;

namespace NYT::NQueryClient {

using NCodegen::MangleSymbol;
using NCodegen::DemangleSymbol;

////////////////////////////////////////////////////////////////////////////////

static const char* ExecutionContextStructName = "struct.TExpressionContext";
static const char* FunctionContextStructName = "struct.NYT::NQueryClient::TFunctionContext";
static const char* UnversionedValueStructName = "struct.TUnversionedValue";

namespace {

TString ToString(llvm::Type* tp)
{
    std::string str;
    llvm::raw_string_ostream stream(str);
    tp->print(stream);
    return TString(stream.str());
}

Type* GetOpaqueType(
    TCGBaseContext& builder,
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

Value* GetTypedExecutionContext(
    TCGBaseContext& builder,
    Value* executionContext)
{
    auto fullContext = executionContext;
    auto contextType = GetOpaqueType(builder, ExecutionContextStructName);
    auto contextStruct = builder->CreateBitCast(
        fullContext,
        PointerType::getUnqual(contextType));
    return contextStruct;
}

Value* GetTypedFunctionContext(
    TCGBaseContext& builder,
    Value* functionContext)
{
    auto contextType = GetOpaqueType(builder, FunctionContextStructName);
    auto contextStruct = builder->CreateBitCast(
        functionContext,
        PointerType::getUnqual(contextType));
    return contextStruct;
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
                "Wrong type for argument %v in LLVM bitcode for function %Qv: expected %Qlv, got %Qlv",
                i,
                functionName,
                ToString(*expected),
                ToString(actual->getType()));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void PushArgument(
    TCGIRBuilderPtr& builder,
    std::vector<Value*>& argumentValues,
    TCGValue argumentValue)
{
    argumentValues.push_back(argumentValue.GetTypedData(builder, true));
    if (IsStringLikeType(argumentValue.GetStaticType())) {
        argumentValues.push_back(argumentValue.GetLength());
    }
}

TCGValue PropagateNullArguments(
    std::vector<TCodegenValue>& codegenArguments,
    std::vector<Value*>& argumentValues,
    std::function<TCGValue(std::vector<Value*>)> codegenBody,
    EValueType type,
    const TString& name,
    TCGBaseContext& builder)
{
    if (codegenArguments.empty()) {
        return codegenBody(argumentValues);
    } else {
        auto currentArgValue = codegenArguments.back()(builder);
        codegenArguments.pop_back();

        PushArgument(builder, argumentValues, currentArgValue);

        return CodegenIf<TCGBaseContext, TCGValue>(
            builder,
            currentArgValue.GetIsNull(builder),
            [&] (TCGBaseContext& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGBaseContext& builder) {
                return PropagateNullArguments(
                    codegenArguments,
                    argumentValues,
                    std::move(codegenBody),
                    type,
                    name,
                    builder);
            },
            Twine(name.c_str()));
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCGValue TSimpleCallingConvention::MakeCodegenFunctionCall(
    TCGBaseContext& builder,
    std::vector<TCodegenValue> codegenArguments,
    std::function<Value*(TCGBaseContext&, std::vector<Value*>)> codegenBody,
    EValueType type,
    const TString& name) const
{
    std::reverse(codegenArguments.begin(), codegenArguments.end());
    auto llvmArgs = std::vector<Value*>();

    std::function<TCGValue(std::vector<Value*>)> callUdf;
    if (IsStringLikeType(type)) {
        auto resultPtr = builder->CreateAlloca(
            GetABIType(builder->getContext(), EValueType::String),
            nullptr,
            "resultPtr");
        llvmArgs.push_back(resultPtr);

        auto resultLength = builder->CreateAlloca(
            TTypeBuilder::TLength::get(builder->getContext()),
            nullptr,
            "resultLength");
        llvmArgs.push_back(resultLength);

        callUdf = [
            &,
            resultLength,
            resultPtr
        ] (std::vector<Value*> argValues) {
            codegenBody(builder, argValues);
            return TCGValue::CreateFromValue(
                builder,
                builder->getFalse(),
                builder->CreateLoad(resultLength),
                builder->CreateLoad(resultPtr),
                type,
                Twine(name.c_str()));
        };
    } else {
        callUdf = [&] (std::vector<Value*> argValues) {
            Value* llvmResult = codegenBody(builder, argValues);
            if (type == EValueType::Boolean) {
                llvmResult = builder->CreateTrunc(llvmResult, builder->getInt1Ty());
            }

            return TCGValue::CreateFromValue(
                builder,
                builder->getFalse(),
                nullptr,
                llvmResult,
                type);
        };
    }

    return PropagateNullArguments(
        codegenArguments,
        llvmArgs,
        callUdf,
        type,
        name,
        builder);
}

llvm::FunctionType* TSimpleCallingConvention::GetCalleeType(
    TCGBaseContext& builder,
    std::vector<EValueType> argumentTypes,
    EValueType resultType,
    bool useFunctionContext) const
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
        calleeResultType = GetABIType(
            builder->getContext(),
            resultType);
    }

    for (auto type : argumentTypes) {
        if (IsStringLikeType(type)) {
            calleeArgumentTypes.push_back(builder->getInt8PtrTy());
            calleeArgumentTypes.push_back(builder->getInt32Ty());
        } else {
            calleeArgumentTypes.push_back(GetABIType(
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
    int repeatedArgIndex)
    : RepeatedArgIndex_(repeatedArgIndex)
{ }

TCGValue TUnversionedValueCallingConvention::MakeCodegenFunctionCall(
    TCGBaseContext& builder,
    std::vector<TCodegenValue> codegenArguments,
    std::function<Value*(TCGBaseContext&, std::vector<Value*>)> codegenBody,
    EValueType type,
    const TString& name) const
{
    auto argumentValues = std::vector<Value*>();

    auto unversionedValueType = llvm::TypeBuilder<TValue, false>::get(builder->getContext());
    auto unversionedValueOpaqueType = GetOpaqueType(
        builder,
        UnversionedValueStructName);

    auto resultPtr = builder->CreateAlloca(
        unversionedValueType,
        nullptr,
        "resultPtr");
    auto castedResultPtr = builder->CreateBitCast(
        resultPtr,
        PointerType::getUnqual(unversionedValueOpaqueType));
    argumentValues.push_back(castedResultPtr);

    int argIndex = 0;
    auto arg = codegenArguments.begin();
    for (;
        arg != codegenArguments.end() && argIndex != RepeatedArgIndex_;
        arg++, argIndex++)
    {
        auto valuePtr = builder->CreateAlloca(unversionedValueType);
        auto cgValue = (*arg)(builder);
        cgValue.StoreToValue(builder, valuePtr);

        auto castedValuePtr = builder->CreateBitCast(
            valuePtr,
            PointerType::getUnqual(unversionedValueOpaqueType));
        argumentValues.push_back(castedValuePtr);
    }

    if (argIndex == RepeatedArgIndex_) {
        auto varargSize = builder->getInt32(
            codegenArguments.size() - RepeatedArgIndex_);

        auto varargPtr = builder->CreateAlloca(
            unversionedValueType,
            varargSize);
        auto castedVarargPtr = builder->CreateBitCast(
            varargPtr,
            PointerType::getUnqual(unversionedValueOpaqueType));

        argumentValues.push_back(castedVarargPtr);
        argumentValues.push_back(varargSize);

        for (int varargIndex = 0; arg != codegenArguments.end(); arg++, varargIndex++) {
            auto valuePtr = builder->CreateConstGEP1_32(
                varargPtr,
                varargIndex);

            auto cgValue = (*arg)(builder);
            cgValue.StoreToValue(builder, valuePtr);
        }
    }

    codegenBody(builder, argumentValues);

    return TCGValue::CreateFromLlvmValue(
        builder,
        resultPtr,
        type);

}

llvm::FunctionType* TUnversionedValueCallingConvention::GetCalleeType(
    TCGBaseContext& builder,
    std::vector<EValueType> argumentTypes,
    EValueType resultType,
    bool useFunctionContext) const
{
    llvm::Type* calleeResultType = builder->getVoidTy();

    auto calleeArgumentTypes = std::vector<llvm::Type*>();

    calleeArgumentTypes.push_back(PointerType::getUnqual(
        GetOpaqueType(builder, ExecutionContextStructName)));

    if (useFunctionContext) {
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
    TType repeatedArgType)
{
    switch (callingConvention) {
        case ECallingConvention::Simple:
            return New<TSimpleCallingConvention>();
        case ECallingConvention::UnversionedValue:
            if (repeatedArgType.TryAs<EValueType>()
                && repeatedArgType.As<EValueType>() == EValueType::Null)
            {
                return New<TUnversionedValueCallingConvention>(-1);
            } else {
                return New<TUnversionedValueCallingConvention>(repeatedArgIndex);
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
    TRef implementationFile)
{
    if (builder.Module->IsModuleLoaded(implementationFile)) {
        return;
    }

    auto diag = SMDiagnostic();

    // NB(levysotsky): This is workaround for the bug in LLVM function llvm::isBitcode
    // (buffer overflow on short (< 4 bytes) inputs).
    constexpr size_t MinimalIRSize = 4;
    if (implementationFile.Size() < MinimalIRSize) {
        THROW_ERROR_EXCEPTION("LLVM bitcode for function %Qv must be at least %v bytes long",
            functionName,
            MinimalIRSize);
    }

    // MemoryBuffer must be zero-terminated.
    struct TBitcodeBufferTag { };
    auto implementationBuffer = TSharedMutableRef::Allocate<TBitcodeBufferTag>(implementationFile.Size() + 1);
    ::memcpy(implementationBuffer.Begin(), implementationFile.Begin(), implementationFile.Size());
    implementationBuffer.Begin()[implementationFile.Size()] = 0;
    auto implementationBufferRef = MemoryBufferRef(
        ToStringRef(TRef(implementationBuffer.Begin(), implementationFile.Size())),
        StringRef("implementation"));

    auto implementationModule = parseIR(implementationBufferRef, diag, builder->getContext());

    if (!implementationModule) {
        THROW_ERROR_EXCEPTION("Could not parse LLVM bitcode for function %Qv", functionName)
            << TErrorAttribute("line_no", diag.getLineNo())
            << TErrorAttribute("column_no", diag.getColumnNo())
            << TErrorAttribute("content", TStringBuf(diag.getLineContents().data(), diag.getLineContents().size()))
            << TErrorAttribute("message", TStringBuf(diag.getMessage().data(), diag.getMessage().size()));
    }

    for (const auto& symbol : requiredSymbols) {
        auto callee = implementationModule->getFunction(ToStringRef(symbol));
        if (!callee) {
            THROW_ERROR_EXCEPTION(
                "LLVM bitcode for function %Qv is missing required symbol %Qv",
                functionName,
                symbol);
        }
        callee->addFnAttr(Attribute::AttrKind::InlineHint);
    }

    for (auto& function : implementationModule->getFunctionList()) {
        auto name = function.getName();
        auto nameString = TString(name.begin(), name.size());
        if (builder.Module->IsSymbolLoaded(nameString)) {
            THROW_ERROR_EXCEPTION(
                "LLVM bitcode for function %Qv redefines already defined symbol %Qv",
                functionName,
                nameString);
        }
    }

    auto module = builder.Module->GetModule();

    std::string what;
    bool linkerFailed;
    llvm::raw_string_ostream os(what);
#if !LLVM_VERSION_GE(3, 9)
    {
        llvm::DiagnosticPrinterRawOStream printer(os);
        // Link two modules together, with the first module modified to be the
        // composite of the two input modules.
        linkerFailed = Linker::LinkModules(module, implementationModule.get(), [&] (const DiagnosticInfo& info) {
           info.print(printer);
        });
    }
#else


    {
        auto handler = [] (const DiagnosticInfo& info, void *context) {
            auto os = reinterpret_cast<llvm::raw_string_ostream*>(context);
            llvm::DiagnosticPrinterRawOStream printer(*os);
            info.print(printer);
        };

        auto dest = module;
        auto& context = dest->getContext();
#if !LLVM_VERSION_GE(6, 0)
        auto oldDiagnosticHandler = context.getDiagnosticHandler();
        void *oldDiagnosticContext = context.getDiagnosticContext();
        context.setDiagnosticHandler((LLVMContext::DiagnosticHandlerTy)handler, &os, true);

        linkerFailed = Linker::linkModules(*dest, std::move(implementationModule));

        context.setDiagnosticHandler(oldDiagnosticHandler, oldDiagnosticContext, true);
#else
        auto oldDiagnosticHandler = context.getDiagnosticHandlerCallBack();
        void *oldDiagnosticContext = context.getDiagnosticContext();
        context.setDiagnosticHandlerCallBack((DiagnosticHandler::DiagnosticHandlerTy)handler, &os, true);

        linkerFailed = Linker::linkModules(*dest, std::move(implementationModule));

        context.setDiagnosticHandlerCallBack(oldDiagnosticHandler, oldDiagnosticContext, true);
#endif
    }
#endif

    if (linkerFailed) {
        THROW_ERROR_EXCEPTION(
            "Error linking LLVM bitcode for function %Qv",
            functionName)
            << TErrorAttribute("message", what.c_str());
    }

    builder.Module->AddLoadedModule(implementationFile);
}

void LoadLlvmFunctions(
    TCGBaseContext& builder,
    const TString& functionName,
    std::vector<std::pair<TString, llvm::FunctionType*>> functions,
    TRef implementationFile)
{
    if (!implementationFile) {
        THROW_ERROR_EXCEPTION("UDF implementation is not available in this context");
    }

    if (builder.Module->IsFunctionLoaded(functionName)) {
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
    TCGVariables* variables,
    std::vector<size_t> argIds,
    std::unique_ptr<bool[]> literalArgs,
    std::vector<EValueType> argumentTypes,
    EValueType type,
    const TString& name,
    llvm::FoldingSetNodeID* id) const
{
    YCHECK(!ImplementationFile_.Empty());

    if (id) {
        id->AddString(ToStringRef(Fingerprint_));
    }

    int functionContextIndex = -1;
    if (UseFunctionContext_) {
        functionContextIndex = variables->AddOpaque<TFunctionContext>(std::move(literalArgs));
    }

    return [
        this_ = MakeStrong(this),
        type,
        name,
        functionContextIndex,
        MOVE(argIds),
        MOVE(argumentTypes)
    ] (TCGExprContext& builder) {
        std::vector<TCodegenValue> codegenArguments;
        for (size_t id : argIds) {
            codegenArguments.push_back([&, id] (TCGBaseContext& baseBuilder) {
                TCGExprContext innerBuilder(TCGOpaqueValuesContext(baseBuilder, builder), builder);
                return CodegenFragment(innerBuilder, id);
            });
        }
        Value* buffer = builder.Buffer;

        auto codegenBody = [&] (TCGBaseContext& baseBuilder, std::vector<Value*> arguments) {
            TCGExprContext innerBuilder(TCGOpaqueValuesContext(baseBuilder, builder), builder);

            if (this_->UseFunctionContext_) {
                auto functionContext = innerBuilder.GetOpaqueValue(functionContextIndex);
                arguments.insert(arguments.begin(), GetTypedFunctionContext(innerBuilder, functionContext));
            }
            arguments.insert(arguments.begin(), GetTypedExecutionContext(builder, buffer));

            auto functionType = this_->CallingConvention_->GetCalleeType(
                innerBuilder,
                argumentTypes,
                type,
                this_->UseFunctionContext_);

            LoadLlvmFunctions(
                innerBuilder,
                this_->FunctionName_,
                { std::make_pair(this_->SymbolName_, functionType) },
                this_->ImplementationFile_);

            auto callee = innerBuilder.Module->GetModule()->getFunction(
                ToStringRef(this_->SymbolName_));
            YCHECK(callee);

            auto result = innerBuilder->CreateCall(callee, arguments);
            return result;
        };

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            builder,
            codegenArguments,
            codegenBody,
            type,
            name);
    };
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
    ] (const TString& functionName, Value* executionContext) {
        return [
            this_,
            executionContext,
            argumentType,
            stateType,
            resultType,
            functionName,
            initName,
            updateName,
            mergeName,
            finalizeName
        ] (TCGBaseContext& builder, std::vector<Value*> arguments) {
            arguments.insert(arguments.begin(), GetTypedExecutionContext(builder, executionContext));

            auto aggregateFunctions = std::vector<std::pair<TString, llvm::FunctionType*>>();

            auto initType = this_->CallingConvention_->GetCalleeType(
                builder,
                std::vector<EValueType>(),
                stateType,
                false);
            auto init = std::make_pair(initName, initType);

            auto updateType = this_->CallingConvention_->GetCalleeType(
                builder,
                std::vector<EValueType>{
                    stateType,
                    argumentType},
                stateType,
                false);
            auto update = std::make_pair(updateName, updateType);


            auto mergeType = this_->CallingConvention_->GetCalleeType(
                builder,
                std::vector<EValueType>{
                    stateType,
                    stateType},
                stateType,
                false);
            auto merge = std::make_pair(mergeName, mergeType);

            auto finalizeType = this_->CallingConvention_->GetCalleeType(
                builder,
                std::vector<EValueType>{stateType},
                resultType,
                false);
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

            return builder->CreateCall(callee, arguments);
        };
    };

    TCodegenAggregate codegenAggregate;
    codegenAggregate.Initialize = [
        this_ = MakeStrong(this),
        initName,
        stateType,
        name,
        makeCodegenBody
    ] (TCGBaseContext& builder, Value* buffer) {
        return this_->CallingConvention_->MakeCodegenFunctionCall(
            builder,
            std::vector<TCodegenValue>(),
            makeCodegenBody(initName, buffer),
            stateType,
            name + "_init");
    };

    codegenAggregate.Update = [
        this_ = MakeStrong(this),
        updateName,
        stateType,
        name,
        makeCodegenBody
    ] (TCGBaseContext& builder, Value* buffer, TCGValue aggState, TCGValue newValue) {
        auto codegenArgs = std::vector<TCodegenValue>();
        codegenArgs.push_back([=] (TCGBaseContext& builder) {
            return aggState;
        });
        codegenArgs.push_back([=] (TCGBaseContext& builder) {
            return newValue;
        });

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            builder,
            codegenArgs,
            makeCodegenBody(updateName, buffer),
            stateType,
            name + "_update");
    };

    codegenAggregate.Merge = [
        this_ = MakeStrong(this),
        mergeName,
        stateType,
        name,
        makeCodegenBody
    ] (TCGBaseContext& builder, Value* buffer, TCGValue dstAggState, TCGValue aggState) {
        auto codegenArgs = std::vector<TCodegenValue>();
        codegenArgs.push_back([=] (TCGBaseContext& builder) {
            return dstAggState;
        });
        codegenArgs.push_back([=] (TCGBaseContext& builder) {
            return aggState;
        });

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            builder,
            codegenArgs,
            makeCodegenBody(mergeName, buffer),
            stateType,
            name + "_merge");
    };

    codegenAggregate.Finalize = [
        this_ = MakeStrong(this),
        finalizeName,
        resultType,
        name,
        makeCodegenBody
    ] (TCGBaseContext& builder, Value* buffer, TCGValue aggState) {
        auto codegenArgs = std::vector<TCodegenValue>();
        codegenArgs.push_back([=] (TCGBaseContext& builder) {
            return aggState;
        });

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            builder,
            codegenArgs,
            makeCodegenBody(finalizeName, buffer),
            resultType,
            name + "_finalize");
    };

    return codegenAggregate;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
