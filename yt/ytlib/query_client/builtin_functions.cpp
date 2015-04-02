#include "builtin_functions.h"

#include "cg_fragment_compiler.h"
#include "plan_helpers.h"

#include <new_table_client/row_base.h>

#include <llvm/Object/ObjectFile.h>

#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/raw_ostream.h>

#include <llvm/IRReader/IRReader.h>

#include <llvm/Linker/Linker.h>

namespace NYT {
namespace NQueryClient {

using namespace NVersionedTableClient; 

////////////////////////////////////////////////////////////////////////////////

TTypedFunction::TTypedFunction(
    const Stroka& functionName,
    std::vector<TType> argumentTypes,
    TType repeatedArgumentType,
    TType resultType)
    : FunctionName_(functionName)
    , ArgumentTypes_(argumentTypes)
    , RepeatedArgumentType_(repeatedArgumentType)
    , ResultType_(resultType)
{ }

TTypedFunction::TTypedFunction(
    const Stroka& functionName,
    std::vector<TType> argumentTypes,
    TType resultType)
    : FunctionName_(functionName)
    , ArgumentTypes_(argumentTypes)
    , RepeatedArgumentType_(EValueType::Null)
    , ResultType_(resultType)
{ }

Stroka TTypedFunction::GetName() const
{
    return FunctionName_;
}

EValueType TTypedFunction::InferResultType(
    const std::vector<EValueType>& argumentTypes,
    const TStringBuf& source) const
{
    return TypingFunction(
        ArgumentTypes_,
        RepeatedArgumentType_,
        ResultType_,
        GetName(),
        argumentTypes,
        source);
}

Stroka TypeToString(TType tp, std::unordered_map<TTypeArgument, EValueType> genericAssignments)
{
    if (auto genericId = tp.TryAs<TTypeArgument>()) {
        return TypeToString(genericAssignments[*genericId], genericAssignments);
    } else if (auto unionType = tp.TryAs<TUnionType>()) {
        Stroka unionString = "one of { ";
        for (auto tp = (*unionType).begin(); tp != (*unionType).end(); tp++) {
            if (tp != (*unionType).begin()) {
                unionString += ", ";
            }
            unionString += TypeToString(*tp, genericAssignments);
        }
        return unionString + " }";
    } else {
        auto concreteType = tp.As<EValueType>();
        switch (concreteType) {
            case EValueType::TheBottom:
                return "the bottom type";
            case EValueType::Null:
                return "no type";
            case EValueType::Int64:
                return "int";
            case EValueType::Uint64:
                return "unsigned int";
            case EValueType::Double:
                return "double";
            case EValueType::Boolean:
                return "bool";
            case EValueType::String:
                return "string";
            case EValueType::Any:
                return "any type";
            default:
                YUNREACHABLE();
        }
    }
}

EValueType TTypedFunction::TypingFunction(
    const std::vector<TType>& expectedArgTypes,
    TType repeatedArgType,
    TType resultType,
    const Stroka& functionName,
    const std::vector<EValueType>& argTypes,
    const TStringBuf& source) const
{
    std::unordered_map<TTypeArgument, EValueType> genericAssignments;

    auto isSubtype = [&] (EValueType type1, TType type2) {
        YCHECK(!type2.TryAs<TTypeArgument>());
        if (auto* unionType = type2.TryAs<TUnionType>()) {
            auto exists = std::find(
                unionType->begin(),
                unionType->end(),
                type1);
            return exists != unionType->end();
        } else if (auto* concreteType = type2.TryAs<EValueType>()) {
            return type1 == *concreteType;
        }
        return false;
    };

    auto unify = [&] (TType type1, EValueType type2) {
        if (auto* genericId = type1.TryAs<TTypeArgument>()) {
            if (genericAssignments.count(*genericId)) {
                return genericAssignments[*genericId] == type2;
            } else {
                genericAssignments[*genericId] = type2;
                return true;
            }
        } else {
            return isSubtype(type2, type1);
        }
    };

    auto argIndex = 1;
    auto arg = argTypes.begin();
    auto expectedArg = expectedArgTypes.begin();
    for (;
        expectedArg != expectedArgTypes.end() && arg != argTypes.end();
        arg++, expectedArg++, argIndex++)
    {
        if (!unify(*expectedArg, *arg)) {
            THROW_ERROR_EXCEPTION(
                "Wrong type for argument %v. Expected %v, got %v",
                argIndex,
                TypeToString(*expectedArg, genericAssignments),
                TypeToString(*arg, genericAssignments))
                << TErrorAttribute("expression", source);
        }
    }

    EValueType* concreteRepeatedType;
    bool hasRepeatedArgument =
        (concreteRepeatedType = repeatedArgType.TryAs<EValueType>())
        && EValueType::Null != *concreteRepeatedType;
    if (expectedArg != expectedArgTypes.end()
        || (arg != argTypes.end() && !hasRepeatedArgument)) {
        THROW_ERROR_EXCEPTION(
            "Function %Qv expects %v arguments, got %v",
            functionName,
            expectedArgTypes.size(),
            argTypes.size())
            << TErrorAttribute("expression", source);
    }

    for (; arg != argTypes.end(); arg++) {
        if (!unify(repeatedArgType, *arg)) {
            THROW_ERROR_EXCEPTION(
                "Wrong type for repeated argument. Expected %v, got %v",
                TypeToString(repeatedArgType, genericAssignments),
                TypeToString(*arg, genericAssignments))
                << TErrorAttribute("expression", source);
        }
    }

    if (auto* genericResult = resultType.TryAs<TTypeArgument>()) {
        if (!genericAssignments.count(*genericResult)) {
            THROW_ERROR_EXCEPTION(
                "Ambiguous result type")
                << TErrorAttribute("expression", source);
        }
        return genericAssignments[*genericResult];
    } else if (!resultType.TryAs<EValueType>()) {
        THROW_ERROR_EXCEPTION(
            "Ambiguous result type")
            << TErrorAttribute("expression", source);
    } else {
        return resultType.As<EValueType>();
    }

    return EValueType::Null;
}

////////////////////////////////////////////////////////////////////////////////

TKeyTrieNode TUniversalRangeFunction::ExtractKeyRange(
    const TIntrusivePtr<const TFunctionExpression>& expr,
    const TKeyColumns& keyColumns,
    TRowBuffer* rowBuffer) const
{
    return TKeyTrieNode::Universal();
}

////////////////////////////////////////////////////////////////////////////////

TCodegenExpression TCodegenFunction::MakeCodegenExpr(
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    const Stroka& name) const
{
    return [
        this,
        codegenArgs,
        type,
        name
    ] (TCGContext& builder, Value* row) {
        return CodegenValue(
            codegenArgs,
            type,
            name,
            builder,
            row);
    };
}

////////////////////////////////////////////////////////////////////////////////

TIfFunction::TIfFunction() : TTypedFunction(
    "if",
    std::vector<TType>{ EValueType::Boolean, 0, 0 },
    0)
{ }

TCGValue TIfFunction::CodegenValue(
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    const Stroka& name,
    TCGContext& builder,
    Value* row) const
{
    auto nameTwine = Twine(name.c_str());

    YCHECK(codegenArgs.size() == 3);
    auto condition = codegenArgs[0](builder, row);
    YCHECK(condition.GetStaticType() == EValueType::Boolean);

    return CodegenIf<TCGContext, TCGValue>(
        builder,
        condition.IsNull(),
        [&] (TCGContext& builder) {
            return TCGValue::CreateNull(builder, type);
        },
        [&] (TCGContext& builder) {
            return CodegenIf<TCGContext, TCGValue>(
                builder,
                builder.CreateICmpNE(
                    builder.CreateZExtOrBitCast(condition.GetData(), builder.getInt64Ty()),
                    builder.getInt64(0)),
                [&] (TCGContext& builder) {
                    return codegenArgs[1](builder, row);
                },
                [&] (TCGContext& builder) {
                    return codegenArgs[2](builder, row);
                });
        },
        nameTwine);
}

////////////////////////////////////////////////////////////////////////////////

TIsPrefixFunction::TIsPrefixFunction()
    : TTypedFunction(
      "is_prefix",
      std::vector<TType>{ EValueType::String, EValueType::String },
      EValueType::Boolean)
{ }

TCGValue TIsPrefixFunction::CodegenValue(
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    const Stroka& name,
    TCGContext& builder,
    Value* row) const
{
    return MakeBinaryFunctionCall("IsPrefix", codegenArgs, type, name, builder, row);
}

TKeyTrieNode TIsPrefixFunction::ExtractKeyRange(
    const TIntrusivePtr<const TFunctionExpression>& expr,
    const TKeyColumns& keyColumns,
    TRowBuffer* rowBuffer) const
{
    auto result = TKeyTrieNode::Universal();
    auto lhsExpr = expr->Arguments[0];
    auto rhsExpr = expr->Arguments[1];

    auto referenceExpr = rhsExpr->As<TReferenceExpression>();
    auto constantExpr = lhsExpr->As<TLiteralExpression>();

    if (referenceExpr && constantExpr) {
        int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
        if (keyPartIndex >= 0) {
            auto value = TValue(constantExpr->Value);

            YCHECK(value.Type == EValueType::String);

            result.Offset = keyPartIndex;
            result.Bounds.emplace_back(value, true);

            ui32 length = value.Length;
            while (length > 0 && value.Data.String[length - 1] == std::numeric_limits<char>::max()) {
                --length;
            }

            if (length > 0) {
                char* newValue = rowBuffer->GetUnalignedPool()->AllocateUnaligned(length);
                memcpy(newValue, value.Data.String, length);
                ++newValue[length - 1];

                value.Length = length;
                value.Data.String = newValue;
            } else {
                value = MakeSentinelValue<TUnversionedValue>(EValueType::Max);
            }
            result.Bounds.emplace_back(value, false);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TIsSubstrFunction::TIsSubstrFunction()
    : TTypedFunction(
        "is_substr",
        std::vector<TType>{ EValueType::String, EValueType::String },
        EValueType::Boolean)
{ }

TCGValue TIsSubstrFunction::CodegenValue(
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    const Stroka& name,
    TCGContext& builder,
    Value* row) const
{
    return MakeBinaryFunctionCall("IsSubstr", codegenArgs, type, name, builder, row);
}

////////////////////////////////////////////////////////////////////////////////

TLowerFunction::TLowerFunction()
    : TTypedFunction(
        "lower",
        std::vector<TType>{ EValueType::String },
        EValueType::String)
{ }

TCGValue TLowerFunction::CodegenValue(
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    const Stroka& name,
    TCGContext& builder,
    Value* row) const
{
    auto nameTwine = Twine(name.c_str());

    YCHECK(codegenArgs.size() == 1);
    auto argValue = codegenArgs[0](builder, row);
    YCHECK(argValue.GetStaticType() == EValueType::String);

    return CodegenIf<TCGContext, TCGValue>(
        builder,
        argValue.IsNull(),
        [&] (TCGContext& builder) {
            return TCGValue::CreateNull(builder, type);
        },
        [&] (TCGContext& builder) {
            Value* argData = argValue.GetData();
            Value* argLength = argValue.GetLength();

            Value* result = builder.CreateCall3(
                builder.Module->GetRoutine("ToLower"),
                builder.GetExecutionContextPtr(),
                argData,
                argLength);

            return TCGValue::CreateFromValue(
                builder,
                builder.getFalse(),
                argLength,
                result,
                type);
        },
        nameTwine);
}

////////////////////////////////////////////////////////////////////////////////

THashFunction::THashFunction(
    const Stroka& functionName,
    const Stroka& routineName)
    : TTypedFunction(
        functionName,
        std::vector<TType>{ HashTypes_ },
        HashTypes_,
        EValueType::Uint64)
    , RoutineName_(routineName)
{ }

const TUnionType THashFunction::HashTypes_ = 
    TUnionType{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String};

TCGValue THashFunction::CodegenValue(
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    const Stroka& name,
    TCGContext& builder,
    Value* row) const
{
    Value* argRowPtr = builder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));
    Value* executionContextPtrRef = builder.GetExecutionContextPtr();

    builder.CreateCall3(
        builder.Module->GetRoutine("AllocateRow"),
        executionContextPtrRef,
        builder.getInt32(codegenArgs.size()),
        argRowPtr);

    Value* argRowRef = builder.CreateLoad(argRowPtr);

    std::vector<EValueType> keyTypes;
    for (int index = 0; index < codegenArgs.size(); ++index) {
        auto id = index;
        auto value = codegenArgs[index](builder, row);
        value.StoreToRow(builder, argRowRef, index, id);
    }

    Value* result = builder.CreateCall(
        builder.Module->GetRoutine(RoutineName_),
        argRowRef);

    return TCGValue::CreateFromValue(
        builder,
        builder.getInt1(false),
        nullptr,
        result,
        EValueType::Uint64);
}

////////////////////////////////////////////////////////////////////////////////

TIsNullFunction::TIsNullFunction()
    : TTypedFunction(
        "is_null",
        std::vector<TType>{ 0 },
        EValueType::Boolean)
{ }

TCGValue TIsNullFunction::CodegenValue(
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    const Stroka& name,
    TCGContext& builder,
    Value* row) const
{
    YCHECK(codegenArgs.size() == 1);
    auto argValue = codegenArgs[0](builder, row);

    return TCGValue::CreateFromValue(
        builder,
        builder.getFalse(),
        nullptr,            
        builder.CreateZExtOrBitCast(
            argValue.IsNull(),
            TDataTypeBuilder::TBoolean::get(builder.getContext())),
        type);
}

////////////////////////////////////////////////////////////////////////////////

TCastFunction::TCastFunction(
    EValueType resultType,
    const Stroka& functionName)
    : TTypedFunction(
        functionName,
        std::vector<TType>{ CastTypes_ },
        resultType)
{ }

const TUnionType TCastFunction::CastTypes_ = TUnionType{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double};

TCGValue TCastFunction::CodegenValue(
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    const Stroka& name,
    TCGContext& builder,
    Value* row) const
{
    YCHECK(codegenArgs.size() == 1);
    return codegenArgs[0](builder, row).Cast(builder, type);
}

////////////////////////////////////////////////////////////////////////////////

using namespace llvm;

TUserDefinedFunction::TUserDefinedFunction(
    const Stroka& functionName,
    std::vector<EValueType> argumentTypes,
    EValueType resultType,
    TSharedRef implementationFile)
    : TTypedFunction(
        functionName,
        std::vector<TType>(argumentTypes.begin(), argumentTypes.end()),
        resultType)
    , FunctionName_(functionName)
    , ImplementationFile_(implementationFile)
    , ResultType_(resultType)
    , ArgumentTypes_(argumentTypes)
{ }

llvm::Type* ConvertToLLVMType(EValueType type, TCGContext& builder)
{
    auto& context = builder.getContext();
    switch (type) {
        case EValueType::Int64:
        case EValueType::Uint64:
            return Type::getInt64Ty(context);
        case EValueType::Double:
            return Type::getDoubleTy(context);
        case EValueType::Boolean:
            return Type::getInt1Ty(context);
        case EValueType::String:
            return Type::getInt8PtrTy(context);
        default:
            return nullptr;
    }
}

Stroka LLVMTypeToString(llvm::Type* tp)
{
    std::string str;
    llvm::raw_string_ostream stream(str);
    tp->print(stream);
    return Stroka(stream.str());
}

void TUserDefinedFunction::CheckCallee(llvm::Function* callee, TCGContext& builder) const
{
    if (callee == nullptr) {
        THROW_ERROR_EXCEPTION(
            "Could not find LLVM bitcode for %Qv",
            FunctionName_);
    } else if (callee->arg_size() != ArgumentTypes_.size()) {
        THROW_ERROR_EXCEPTION(
            "Wrong number of arguments in LLVM bitcode: expected %v, got %v",
            ArgumentTypes_.size(),
            callee->arg_size());
    } else if (callee->getReturnType() != ConvertToLLVMType(ResultType_, builder)) {
        THROW_ERROR_EXCEPTION(
            "Wrong result type in LLVM bitcode: expected %Qv, got %Qv",
            LLVMTypeToString(ConvertToLLVMType(ResultType_, builder)),
            LLVMTypeToString(callee->getReturnType()));
    }

    auto i = 0;
    auto expected = ArgumentTypes_.begin();
    for (
        auto actual = callee->arg_begin();
        expected != ArgumentTypes_.end();
        expected++, actual++, i++)
    {
        if (actual->getType() != ConvertToLLVMType(*expected, builder)) {
            THROW_ERROR_EXCEPTION(
                "Wrong type for argument %Qv in LLVM bitcode: expected %Qv, got %Qv",
                i,
                LLVMTypeToString(ConvertToLLVMType(*expected, builder)),
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
    CheckCallee(callee, builder);
    return callee;
}

TCodegenExpression TUserDefinedFunction::MakeCodegenExpr(
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    const Stroka& name) const
{
    return [
        this_ = MakeStrong(this),
        codegenArgs,
        type
    ] (TCGContext& builder, Value* row) {
        auto callee = this_->GetLLVMFunction(builder);
        std::vector<Value*> arguments;

        for (auto arg = codegenArgs.begin(); arg != codegenArgs.end(); arg++) {
            auto argValue = (*arg)(builder, row);
            arguments.push_back(argValue.GetData());
        }

        auto result = builder.CreateCall(callee, arguments);

        auto val = TCGValue::CreateFromValue(
            builder,
            builder.getFalse(),
            nullptr,
            result,
            type);
        
        return val;
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
