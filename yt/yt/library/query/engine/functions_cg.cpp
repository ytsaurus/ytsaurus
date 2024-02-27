#include "functions_cg.h"
#include "cg_fragment_compiler.h"

#include <yt/yt/client/table_client/llvm_types.h>

#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/library/codegen/llvm_migrate_helpers.h>
#include <yt/yt/library/codegen/routine_registry.h>

#include <llvm/IR/DiagnosticPrinter.h>
#include <llvm/IR/DiagnosticInfo.h>

#include <llvm/IRReader/IRReader.h>

#include <llvm/Linker/Linker.h>

#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/SourceMgr.h>

#include <llvm/ADT/FoldingSet.h>

using namespace llvm;

namespace NYT::NQueryClient {

using NCodegen::EExecutionBackend;
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
    auto existingType = StructType::getTypeByName(builder->getContext(), name);

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
    const TCGIRBuilderPtr& builder,
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
    bool /*aggregate*/,
    const TString& name) const
{
    std::reverse(codegenArguments.begin(), codegenArguments.end());
    auto llvmArgs = std::vector<Value*>();

    std::function<TCGValue(std::vector<Value*>)> callUdf;
    if (IsStringLikeType(type)) {
        auto resultPtrType = GetABIType(builder->getContext(), EValueType::String);
        auto resultPtr = builder->CreateAlloca(
            resultPtrType,
            nullptr,
            "resultPtr");
        llvmArgs.push_back(resultPtr);

        auto resultLengthType = TValueTypeBuilder::TLength::Get(builder->getContext());
        auto resultLength = builder->CreateAlloca(
            resultLengthType,
            nullptr,
            "resultLength");
        llvmArgs.push_back(resultLength);

        callUdf = [
            &,
            resultLength,
            resultLengthType,
            resultPtr,
            resultPtrType
        ] (std::vector<Value*> argValues) {
            codegenBody(builder, argValues);
            return TCGValue::Create(
                builder,
                builder->getFalse(),
                builder->CreateLoad(resultLengthType, resultLength),
                builder->CreateLoad(resultPtrType, resultPtr),
                type,
                Twine(name.c_str()));
        };
    } else {
        callUdf = [&] (std::vector<Value*> argValues) {
            Value* llvmResult = codegenBody(builder, argValues);
            if (type == EValueType::Boolean) {
                llvmResult = builder->CreateTrunc(llvmResult, builder->getInt1Ty());
            }

            return TCGValue::Create(
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
    bool /*useFunctionContext*/) const
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
    bool aggregate,
    const TString& /*name*/) const
{
    auto argumentValues = std::vector<Value*>();

    auto unversionedValueType = TTypeBuilder<TValue>::Get(builder->getContext());
    auto unversionedValueOpaqueType = GetOpaqueType(
        builder,
        UnversionedValueStructName);

    auto allocaAndInitResult = [&] {
        auto resultPtr = builder->CreateAlloca(
            unversionedValueType,
            nullptr,
            "resultPtr");

        builder->CreateMemSet(
            resultPtr,
            builder->getInt8(0),
            sizeof(TValue),
            llvm::Align(8));

        return resultPtr;
    };

    auto resultPtr = allocaAndInitResult();
    auto castedResultPtr = builder->CreateBitCast(
        resultPtr,
        PointerType::getUnqual(unversionedValueOpaqueType));
    argumentValues.push_back(castedResultPtr);

    auto convertFromPI = [&] (Value* piValuePtr, EValueType staticType) {
        if (NTableClient::IsStringLikeType(staticType)) {
            Value* dataPtr = builder->CreateConstInBoundsGEP2_32(
                TValueTypeBuilder::Get(builder->getContext()),
                piValuePtr,
                0,
                TValueTypeBuilder::Data,
                ".dataPointer");
            Value* data = builder->CreateLoad(
                TValueTypeBuilder::TData::Get(builder->getContext()),
                dataPtr,
                ".PIData");
            Value* dataPtrAsInt = builder->CreatePtrToInt(dataPtr, data->getType(), ".PIDataAsInt");
            data = builder->CreateAdd(data, dataPtrAsInt, ".nonPIData");  // Similar to GetStringPosition
            builder->CreateStore(
                data,
                dataPtr);
        }
    };

    auto convertToPI = [&] (Value* valuePtr, EValueType staticType) {
        if (NTableClient::IsStringLikeType(staticType)) {
            Value* dataPtr = builder->CreateConstInBoundsGEP2_32(
                TValueTypeBuilder::Get(builder->getContext()),
                valuePtr,
                0,
                TValueTypeBuilder::Data,
                ".dataPointer");
            Value* data = builder->CreateLoad(
                TValueTypeBuilder::TData::Get(builder->getContext()),
                dataPtr,
                ".nonPIData");
            Value* dataPtrAsInt = builder->CreatePtrToInt(dataPtr, data->getType(), ".nonPIDataAsInt");
            data = builder->CreateSub(data, dataPtrAsInt, ".PIdata");  // Similar to SetStringPosition.
            builder->CreateStore(
                data,
                dataPtr);
        }
    };

    int argIndex = 0;
    auto arg = codegenArguments.begin();
    for (;
        arg != codegenArguments.end() && argIndex != RepeatedArgIndex_;
        arg++, argIndex++)
    {
        auto valuePtr = builder->CreateAlloca(unversionedValueType);
        auto cgValue = (*arg)(builder);
        cgValue.StoreToValue(builder, valuePtr);

        convertFromPI(valuePtr, cgValue.GetStaticType());

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
            builder->CreateIntCast(varargSize, builder->getInt64Ty(), false));
        auto castedVarargPtr = builder->CreateBitCast(
            varargPtr,
            PointerType::getUnqual(unversionedValueOpaqueType));

        argumentValues.push_back(castedVarargPtr);
        argumentValues.push_back(varargSize);

        for (int varargIndex = 0; arg != codegenArguments.end(); arg++, varargIndex++) {
            auto valuePtr = builder->CreateConstGEP1_32(
                TValueTypeBuilder::Get(builder->getContext()),
                varargPtr,
                varargIndex);

            auto cgValue = (*arg)(builder);
            cgValue.StoreToValue(builder, valuePtr);

            convertFromPI(valuePtr, cgValue.GetStaticType());
        }
    }

    codegenBody(builder, argumentValues);

    convertToPI(resultPtr, type);

    return aggregate
        ? TCGValue::LoadFromAggregate(builder, resultPtr, type)
        : TCGValue::LoadFromRowValue(builder, resultPtr, type);
}

llvm::FunctionType* TUnversionedValueCallingConvention::GetCalleeType(
    TCGBaseContext& builder,
    std::vector<EValueType> argumentTypes,
    EValueType /*resultType*/,
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

    auto positionalArgumentCount = RepeatedArgIndex_ == -1 ? std::ssize(argumentTypes) : RepeatedArgIndex_;
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
            if (std::holds_alternative<EValueType>(repeatedArgType) &&
                std::get<EValueType>(repeatedArgType) == EValueType::Null)
            {
                return New<TUnversionedValueCallingConvention>(-1);
            } else {
                return New<TUnversionedValueCallingConvention>(repeatedArgIndex);
            }
        default:
            YT_ABORT();
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
            YT_ABORT();
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

        linkerFailed = Linker::linkModules(*dest, std::move(implementationModule), Linker::Flags::OverrideFromSrc);

        context.setDiagnosticHandlerCallBack(oldDiagnosticHandler, oldDiagnosticContext, true);
#endif
    }
#endif

    if (linkerFailed) {
        THROW_ERROR_EXCEPTION("Error linking LLVM bitcode for function %Qv",
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
            function.first,
            callee,
            function.second);
    }
}

void BuildPrototypesForFunctions(
    TCGBaseContext& builder,
    const std::vector<std::pair<TString, llvm::FunctionType*>>& functions)
{
    for (auto& [name, type] : functions) {
        builder.Module->GetModule()->getOrInsertFunction(ToStringRef(name), type);
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
    EExecutionBackend executionBackend,
    llvm::FoldingSetNodeID* id) const
{
    YT_VERIFY(!ImplementationFile_.Empty());

    if (id) {
        id->AddString(ToStringRef(Fingerprint_));
    }

    int functionContextIndex = -1;
    if (UseFunctionContext_) {
        functionContextIndex = variables->AddOpaque<TFunctionContext>(std::move(literalArgs));
    }

    return [
        =,
        this_ = MakeStrong(this),
        argIds = std::move(argIds),
        argumentTypes = std::move(argumentTypes)
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

            if (executionBackend == EExecutionBackend::WebAssembly) {
                BuildPrototypesForFunctions(
                    innerBuilder,
                    {std::make_pair(this_->SymbolName_, functionType)});
            } else {
                LoadLlvmFunctions(
                    innerBuilder,
                    this_->FunctionName_,
                    { std::pair(this_->SymbolName_, functionType) },
                    this_->ImplementationFile_);
            }

            auto callee = innerBuilder.Module->GetModule()->getFunction(
                ToStringRef(this_->SymbolName_));
            YT_VERIFY(callee);

            auto result = innerBuilder->CreateCall(callee, arguments);
            return result;
        };

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            builder,
            codegenArguments,
            codegenBody,
            type,
            false,
            name);
    };
}

////////////////////////////////////////////////////////////////////////////////

TCodegenAggregate TExternalAggregateCodegen::Profile(
    std::vector<EValueType> argumentTypes,
    EValueType stateType,
    EValueType resultType,
    const TString& name,
    EExecutionBackend executionBackend,
    llvm::FoldingSetNodeID* id) const
{
    YT_VERIFY(!ImplementationFile_.Empty());

    if (id) {
        id->AddString(ToStringRef(Fingerprint_));
    }

    auto initName = AggregateName_ + "_init";
    auto updateName = AggregateName_ + "_update";
    auto mergeName = AggregateName_ + "_merge";
    auto finalizeName = AggregateName_ + "_finalize";

    auto makeCodegenBody = [
        this_ = MakeStrong(this),
        argumentTypes = std::move(argumentTypes),
        stateType,
        resultType,
        executionBackend,
        initName,
        updateName,
        mergeName,
        finalizeName
    ] (const TString& functionName, Value* executionContext) {
        return [
            this_,
            executionContext,
            argumentTypes = std::move(argumentTypes),
            stateType,
            resultType,
            executionBackend,
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
            auto init = std::pair(initName, initType);

            std::vector<EValueType> updateArgs = {stateType};
            updateArgs.insert(updateArgs.end(), argumentTypes.begin(), argumentTypes.end());

            auto updateType = this_->CallingConvention_->GetCalleeType(
                builder,
                updateArgs,
                stateType,
                false);
            auto update = std::pair(updateName, updateType);


            auto mergeType = this_->CallingConvention_->GetCalleeType(
                builder,
                std::vector{
                    stateType,
                    stateType},
                stateType,
                false);
            auto merge = std::pair(mergeName, mergeType);

            auto finalizeType = this_->CallingConvention_->GetCalleeType(
                builder,
                std::vector{stateType},
                resultType,
                false);
            auto finalize = std::pair(finalizeName, finalizeType);

            aggregateFunctions.push_back(init);
            aggregateFunctions.push_back(update);
            aggregateFunctions.push_back(merge);
            aggregateFunctions.push_back(finalize);

            if (executionBackend == EExecutionBackend::WebAssembly) {
                BuildPrototypesForFunctions(builder, aggregateFunctions);
            } else {
                LoadLlvmFunctions(
                    builder,
                    this_->AggregateName_,
                    aggregateFunctions,
                    this_->ImplementationFile_);
            }

            auto callee = builder.Module->GetModule()->getFunction(ToStringRef(functionName));
            YT_VERIFY(callee);

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
            true,
            name + "_init");
    };

    codegenAggregate.Update = [
        this_ = MakeStrong(this),
        updateName,
        stateType,
        name,
        makeCodegenBody
    ] (TCGBaseContext& builder, Value* buffer, TCGValue aggState, std::vector<TCGValue> newValues) {
        std::vector<TCodegenValue> codegenArgs;
        codegenArgs.push_back([=] (TCGBaseContext& /*builder*/) {
            return aggState;
        });
        for (auto argument : newValues) {
            codegenArgs.push_back([=] (TCGBaseContext& /*builder*/) {
                return argument;
            });
        }

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            builder,
            codegenArgs,
            makeCodegenBody(updateName, buffer),
            stateType,
            true,
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
        codegenArgs.push_back([=] (TCGBaseContext& /*builder*/) {
            return dstAggState;
        });
        codegenArgs.push_back([=] (TCGBaseContext& /*builder*/) {
            return aggState;
        });

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            builder,
            codegenArgs,
            makeCodegenBody(mergeName, buffer),
            stateType,
            true,
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
        codegenArgs.push_back([=] (TCGBaseContext& /*builder*/) {
            return aggState;
        });

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            builder,
            codegenArgs,
            makeCodegenBody(finalizeName, buffer),
            resultType,
            true,
            name + "_finalize");
    };

    return codegenAggregate;
}

bool TExternalAggregateCodegen::IsFirst() const
{
    return IsFirst_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
