#include "user_defined_functions.h"

#include "cg_fragment_compiler.h"
#include "plan_helpers.h"

#include <ytlib/table_client/row_base.h>
#include <ytlib/table_client/llvm_types.h>

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

void ICallingConvention::CheckCallee(
    TCGContext& builder,
    const Stroka& functionName,
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

llvm::FunctionType* TSimpleCallingConvention::GetCalleeType(
    TCGContext& builder,
    std::vector<EValueType> argumentTypes,
    EValueType resultType) const
{
    llvm::Type* calleeResultType;
    auto calleeArgumentTypes = std::vector<llvm::Type*>();
    calleeArgumentTypes.push_back(PointerType::getUnqual(
        GetOpaqueType(builder, ExecutionContextStructName)));

    if (IsStringLikeType(resultType)) {
        calleeResultType = builder.getVoidTy();
        calleeArgumentTypes.push_back(PointerType::getUnqual(builder.getInt8PtrTy()));
        calleeArgumentTypes.push_back(PointerType::getUnqual(builder.getInt32Ty()));

    } else {
        calleeResultType = TDataTypeBuilder::get(
            builder.getContext(),
            resultType);
    }

    for (auto type : argumentTypes) {
        if (IsStringLikeType(type)) {
            calleeArgumentTypes.push_back(builder.getInt8PtrTy());
            calleeArgumentTypes.push_back(builder.getInt32Ty());
        } else {
            calleeArgumentTypes.push_back(TDataTypeBuilder::get(
                builder.getContext(),
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

llvm::FunctionType* TUnversionedValueCallingConvention::GetCalleeType(
    TCGContext& builder,
    std::vector<EValueType> argumentTypes,
    EValueType resultType) const
{
    llvm::Type* calleeResultType = builder.getVoidTy();

    auto calleeArgumentTypes = std::vector<llvm::Type*>();
    calleeArgumentTypes.push_back(PointerType::getUnqual(
        GetOpaqueType(builder, ExecutionContextStructName)));
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
        calleeArgumentTypes.push_back(builder.getInt32Ty());
    }

    return FunctionType::get(
        calleeResultType,
        ArrayRef<llvm::Type*>(calleeArgumentTypes),
        false);
}

////////////////////////////////////////////////////////////////////////////////

TUserDefinedFunction::TUserDefinedFunction(
    const Stroka& functionName,
    const Stroka& symbolName,
    std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    std::vector<TType> argumentTypes,
    TType repeatedArgType,
    TType resultType,
    TSharedRef implementationFile,
    ICallingConventionPtr callingConvention)
    : TTypedFunction(
        functionName,
        typeArgumentConstraints,
        std::vector<TType>(argumentTypes.begin(), argumentTypes.end()),
        repeatedArgType,
        resultType)
    , TExternallyDefinedFunction(functionName, symbolName, implementationFile, callingConvention)
{ }

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
        functionName,
        std::unordered_map<TTypeArgument, TUnionType>(),
        argumentTypes,
        EValueType::Null,
        resultType,
        implementationFile,
        GetCallingConvention(callingConvention, argumentTypes.size(), EValueType::Null))
{ }

TUserDefinedFunction::TUserDefinedFunction(
    const Stroka& functionName,
    std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    std::vector<TType> argumentTypes,
    TType repeatedArgType,
    TType resultType,
    TSharedRef implementationFile)
    : TUserDefinedFunction(
        functionName,
        functionName,
        typeArgumentConstraints,
        argumentTypes,
        repeatedArgType,
        resultType,
        implementationFile,
        GetCallingConvention(ECallingConvention::UnversionedValue, argumentTypes.size(), repeatedArgType))
{ }

TUserDefinedFunction::TUserDefinedFunction(
    const Stroka& functionName,
    const Stroka& symbolName,
    std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    std::vector<TType> argumentTypes,
    TType repeatedArgType,
    TType resultType,
    TSharedRef implementationFile)
    : TUserDefinedFunction(
        functionName,
        symbolName,
        typeArgumentConstraints,
        argumentTypes,
        repeatedArgType,
        resultType,
        implementationFile,
        GetCallingConvention(ECallingConvention::UnversionedValue, argumentTypes.size(), repeatedArgType))
{ }

bool ObjectContainsFunction(
    const std::unique_ptr<llvm::object::ObjectFile>& objectFile,
    const Stroka& functionName)
{
    auto symbols = objectFile->symbols();
    for (const auto symbol : symbols) {
        auto name = llvm::StringRef();
        auto nameError = symbol.getName(name);
        if (!nameError && name.equals(StringRef(functionName))) {
            return true;
        }
    }
    return false;
}

bool LoadSharedObject(
    TCGContext& builder,
    const Stroka& functionName,
    std::vector<Stroka> requiredSymbols,
    TSharedRef implementationFile)
{
    auto buffer = llvm::MemoryBufferRef(
        llvm::StringRef(implementationFile.Begin(), implementationFile.Size()),
        llvm::StringRef());
    auto objectFileOrError = llvm::object::ObjectFile::createObjectFile(buffer);

    if (!objectFileOrError) {
        return false;
    }

    for (auto symbol : requiredSymbols) {
        if (!ObjectContainsFunction(*objectFileOrError, symbol)) {
            THROW_ERROR_EXCEPTION(
                "Could not find implementation for %Qv",
                symbol);
        }
        if (builder.Module->GetModule()->getFunction(StringRef(symbol))) {
            THROW_ERROR_EXCEPTION(
                "Error loading shared object for function %Qv: symbol %Qv already exists",
                functionName,
                symbol);
        }
    }

    for (auto symbol : (*objectFileOrError)->symbols()) {
        auto name = llvm::StringRef();
        auto nameError = symbol.getName(name);
        if (!nameError) {
            auto nameStroka = Stroka(name.begin(), name.size());
            if (builder.Module->SymbolIsLoaded(nameStroka)) {
                THROW_ERROR_EXCEPTION(
                    "Error loading shared object for function %Qv: symbol %Qv already exists",
                    functionName,
                    nameStroka);
            }
            builder.Module->AddLoadedSymbol(nameStroka);
        }
    }

    builder.Module->AddObjectFile(std::move(*objectFileOrError));
    return true;
}

bool LoadLlvmBitcode(
    TCGContext& builder,
    const Stroka& functionName,
    std::vector<Stroka> requiredSymbols,
    TSharedRef implementationFile)
{
    auto diag = SMDiagnostic();
    auto buffer = MemoryBufferRef(
        StringRef(implementationFile.Begin(), implementationFile.Size()),
        StringRef("impl"));
    auto implModule = parseIR(buffer, diag, builder.getContext());

    if (!implModule) {
        return false;
    }

    for (auto symbol : requiredSymbols) {
        auto callee = implModule->getFunction(StringRef(symbol));
        if (!callee) {
            THROW_ERROR_EXCEPTION(
                "Could not find LLVM bitcode for %Qv",
                symbol);
        }
        callee->addFnAttr(Attribute::AttrKind::AlwaysInline);
    }

    for (auto& function : implModule->getFunctionList()) {
        auto name = function.getName();
        auto nameStroka = Stroka(name.begin(), name.size());
        if (builder.Module->SymbolIsLoaded(nameStroka)) {
            THROW_ERROR_EXCEPTION(
                "Error loading LLVM bitcode for function %Qv: symbol %Qv already exists",
                functionName,
                nameStroka);
        }
    }

    auto module = builder.Module->GetModule();
    // Link two modules together, with the first module modified to be the
    // composite of the two input modules.
    auto linkError = Linker::LinkModules(module, implModule.get());

    if (linkError) {
        THROW_ERROR_EXCEPTION(
            "Error linking LLVM bitcode for function %Qv",
            functionName);
    }

    return true;
}

void LoadLlvmFunctions(
    TCGContext& builder,
    const Stroka& functionName,
    std::vector<std::pair<Stroka, llvm::FunctionType*>> functions,
    TSharedRef implementationFile)
{
    if (!implementationFile) {
        THROW_ERROR_EXCEPTION("UDF implementation is not available in this context");
    }

    if (builder.Module->FunctionIsLoaded(functionName)) {
        return;
    }

    auto requiredSymbols = std::vector<Stroka>();
    for (auto function : functions) {
        requiredSymbols.push_back(function.first);
    }

    auto loaded = LoadLlvmBitcode(
        builder,
        functionName,
        requiredSymbols,
        implementationFile);

    auto module = builder.Module->GetModule();

    if (loaded) {
        builder.Module->AddLoadedFunction(functionName);
        for (auto function : functions) {
            auto callee = module->getFunction(StringRef(function.first));
            ICallingConvention::CheckCallee(
                builder,
                function.first,
                callee,
                function.second);
        }
        return;
    }

    loaded = LoadSharedObject(
        builder,
        functionName,
        requiredSymbols,
        implementationFile);

    if (loaded) {
        builder.Module->AddLoadedFunction(functionName);
        for (auto function : functions) {
            Function::Create(
                function.second,
                Function::ExternalLinkage,
                function.first.c_str(),
                module);
        }
        return;
    }

    THROW_ERROR_EXCEPTION(
        "Error loading implementation file for function %Qv",
        functionName);
}

TCodegenExpression TExternallyDefinedFunction::MakeCodegenExpr(
    std::vector<TCodegenExpression> codegenArgs,
    std::vector<EValueType> argumentTypes,
    EValueType type,
    const Stroka& name) const
{
    auto codegenBody = [
        this_ = MakeStrong(this),
        argumentTypes,
        type
    ] (std::vector<Value*> argumentValues, TCGContext& builder) {
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
            StringRef(this_->SymbolName_));
        YCHECK(callee);

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
    std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    TType argumentType,
    TType resultType,
    TType stateType,
    TSharedRef implementationFile,
    ICallingConventionPtr callingConvention)
    : AggregateName_(aggregateName)
    , TypeArgumentConstraints_(typeArgumentConstraints)
    , ArgumentType_(argumentType)
    , ResultType_(resultType)
    , StateType_(stateType)
    , ImplementationFile_(implementationFile)
    , CallingConvention_(callingConvention)
{ }

TUserDefinedAggregateFunction::TUserDefinedAggregateFunction(
    const Stroka& aggregateName,
    std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    TType argumentType,
    TType resultType,
    TType stateType,
    TSharedRef implementationFile,
    ECallingConvention callingConvention)
    : TUserDefinedAggregateFunction(
        aggregateName,
        typeArgumentConstraints,
        argumentType,
        resultType,
        stateType,
        implementationFile,
        GetCallingConvention(callingConvention, 1, EValueType::Null))
{ }

Stroka TUserDefinedAggregateFunction::GetName() const
{
    return AggregateName_;
}

const TCodegenAggregate TUserDefinedAggregateFunction::MakeCodegenAggregate(
    EValueType argumentType,
    EValueType stateType,
    EValueType resultType,
    const Stroka& name) const
{
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
    ] (const Stroka& functionName) {
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
        ] (std::vector<Value*> argumentValues, TCGContext& builder) {
            auto aggregateFunctions = std::vector<std::pair<Stroka, llvm::FunctionType*>>();

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

            auto callee = builder.Module->GetModule()->getFunction(
                StringRef(functionName));
            YCHECK(callee);

            return builder.CreateCall(callee, argumentValues);
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
    ] (TCGContext& builder, Value* row) {
        return this_->CallingConvention_->MakeCodegenFunctionCall(
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
    ] (TCGContext& builder, Value* aggState, Value* newValue) {
        auto codegenArgs = std::vector<TCodegenExpression>();
        codegenArgs.push_back([=] (TCGContext& builder, Value* row) {
            return TCGValue::CreateFromLlvmValue(
                builder,
                aggState,
                stateType);
        });
        codegenArgs.push_back([=] (TCGContext& builder, Value* row) {
            return TCGValue::CreateFromLlvmValue(
                builder,
                newValue,
                argumentType);
        });

        return this_->CallingConvention_->MakeCodegenFunctionCall(
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
    ] (TCGContext& builder, Value* dstAggState, Value* aggState) {
        auto codegenArgs = std::vector<TCodegenExpression>();
        codegenArgs.push_back([=] (TCGContext& builder, Value* row) {
            return TCGValue::CreateFromLlvmValue(
                builder,
                dstAggState,
                stateType);
            });
        codegenArgs.push_back([=] (TCGContext& builder, Value* row) {
            return TCGValue::CreateFromLlvmValue(
                builder,
                aggState,
                stateType);
        });

        return this_->CallingConvention_->MakeCodegenFunctionCall(
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
    ] (TCGContext& builder, Value* aggState) {
        auto codegenArgs = std::vector<TCodegenExpression>();
        codegenArgs.push_back([=] (TCGContext& builder, Value* row) {
            return TCGValue::CreateFromLlvmValue(
                builder,
                aggState,
                stateType);
        });

        return this_->CallingConvention_->MakeCodegenFunctionCall(
            codegenArgs,
            makeCodegenBody(finalizeName),
            argumentType,
            name + "_finalize")(builder, aggState);
    };

    return codegenAggregate;
}

EValueType TUserDefinedAggregateFunction::GetStateType(
    EValueType type) const
{
    return TypingFunction(
        TypeArgumentConstraints_,
        std::vector<TType>{ArgumentType_},
        EValueType::Null,
        StateType_,
        AggregateName_,
        std::vector<EValueType>{type},
        TStringBuf());
}

EValueType TUserDefinedAggregateFunction::InferResultType(
    EValueType argumentType,
    const TStringBuf& source) const
{
    return TypingFunction(
        TypeArgumentConstraints_,
        std::vector<TType>{ArgumentType_},
        EValueType::Null,
        ResultType_,
        AggregateName_,
        std::vector<EValueType>{argumentType},
        source);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
