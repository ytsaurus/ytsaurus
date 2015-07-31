#include "builtin_functions.h"

#include "cg_fragment_compiler.h"
#include "plan_helpers.h"
#include "plan_fragment.h"
#include "private.h"

namespace NYT {
namespace NQueryClient {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

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
        return ToString(tp.As<EValueType>());
    }
}

EValueType TypingFunction(
    const std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    const std::vector<TType>& expectedArgTypes,
    TType repeatedArgType,
    TType resultType,
    const Stroka& functionName,
    const std::vector<EValueType>& argTypes,
    const TStringBuf& source)
{
    std::unordered_map<TTypeArgument, EValueType> genericAssignments;

    auto typeInUnion = [&] (TUnionType unionType, EValueType type) {
        return std::find(
            unionType.begin(),
            unionType.end(),
            type) != unionType.end();
    };

    auto isSubtype = [&] (EValueType type1, TType type2) {
        YCHECK(!type2.TryAs<TTypeArgument>());
        if (auto* unionType = type2.TryAs<TUnionType>()) {
            return typeInUnion(*unionType, type1);
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
                "Wrong type for argument %v to function %Qv: expected %Qv, got %Qv",
                functionName,
                argIndex,
                TypeToString(*expectedArg, genericAssignments),
                TypeToString(*arg, genericAssignments))
                << TErrorAttribute("expression", source);
        }
    }

    bool hasNoRepeatedArgument = repeatedArgType.Is<EValueType>() &&
        repeatedArgType.As<EValueType>() == EValueType::Null;

    if (expectedArg != expectedArgTypes.end() ||
        (arg != argTypes.end() && hasNoRepeatedArgument))
    {
        THROW_ERROR_EXCEPTION(
            "Wrong number of arguments to function %Qv: expected %v, got %v",
            functionName,
            expectedArgTypes.size(),
            argTypes.size())
            << TErrorAttribute("expression", source);
    }

    for (; arg != argTypes.end(); arg++) {
        if (!unify(repeatedArgType, *arg)) {
            THROW_ERROR_EXCEPTION(
                "Wrong type for repeated argument to function %Qv: expected %Qv, got %Qv",
                functionName,
                TypeToString(repeatedArgType, genericAssignments),
                TypeToString(*arg, genericAssignments))
                << TErrorAttribute("expression", source);
        }
    }

    for (auto constraint : typeArgumentConstraints) {
        auto typeArg = constraint.first;
        auto allowedTypes = constraint.second;
        if (genericAssignments.count(typeArg)
            && !typeInUnion(allowedTypes, genericAssignments[typeArg]))
        {
            THROW_ERROR_EXCEPTION(
                "Invalid type inferred for type argument %v to function %Qv: expected %Qv, got %Qv",
                typeArg,
                functionName,
                TypeToString(allowedTypes, genericAssignments),
                TypeToString(typeArg, genericAssignments))
                << TErrorAttribute("expression", source);
        }
    }

    if (auto* genericResult = resultType.TryAs<TTypeArgument>()) {
        if (!genericAssignments.count(*genericResult)) {
            THROW_ERROR_EXCEPTION(
                "Ambiguous result type for function %Qv",
                functionName)
                << TErrorAttribute("expression", source);
        }
        return genericAssignments[*genericResult];
    } else if (!resultType.TryAs<EValueType>()) {
        THROW_ERROR_EXCEPTION(
            "Ambiguous result type for function %Qv",
            functionName)
            << TErrorAttribute("expression", source);
    } else {
        return resultType.As<EValueType>();
    }

    return EValueType::Null;
}

////////////////////////////////////////////////////////////////////////////////

TTypedFunction::TTypedFunction(
    const Stroka& functionName,
    std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    std::vector<TType> argumentTypes,
    TType repeatedArgumentType,
    TType resultType)
    : FunctionName_(functionName)
    , TypeArgumentConstraints_(typeArgumentConstraints)
    , ArgumentTypes_(argumentTypes)
    , RepeatedArgumentType_(repeatedArgumentType)
    , ResultType_(resultType)
{ }

TTypedFunction::TTypedFunction(
    const Stroka& functionName,
    std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    std::vector<TType> argumentTypes,
    TType resultType)
    : FunctionName_(functionName)
    , TypeArgumentConstraints_(typeArgumentConstraints)
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
        TypeArgumentConstraints_,
        ArgumentTypes_,
        RepeatedArgumentType_,
        ResultType_,
        GetName(),
        argumentTypes,
        source);
}

////////////////////////////////////////////////////////////////////////////////

TKeyTriePtr TUniversalRangeFunction::ExtractKeyRange(
    const TConstFunctionExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer) const
{
    return TKeyTrie::Universal();
}

////////////////////////////////////////////////////////////////////////////////

TCodegenExpression TCodegenFunction::MakeCodegenExpr(
    std::vector<TCodegenExpression> codegenArgs,
    std::vector<EValueType> argumentTypes,
    EValueType type,
    const Stroka& name) const
{
    return [
        this,
        MOVE(codegenArgs),
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
    std::unordered_map<TTypeArgument, TUnionType>(),
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
        std::unordered_map<TTypeArgument, TUnionType>(),
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

TKeyTriePtr TIsPrefixFunction::ExtractKeyRange(
    const TConstFunctionExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer) const
{
    auto result = TKeyTrie::Universal();
    auto lhsExpr = expr->Arguments[0];
    auto rhsExpr = expr->Arguments[1];

    auto referenceExpr = rhsExpr->As<TReferenceExpression>();
    auto constantExpr = lhsExpr->As<TLiteralExpression>();

    if (referenceExpr && constantExpr) {
        int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
        if (keyPartIndex >= 0) {
            auto value = TValue(constantExpr->Value);

            YCHECK(value.Type == EValueType::String);

            result = New<TKeyTrie>(keyPartIndex);
            result->Bounds.emplace_back(value, true);

            ui32 length = value.Length;
            while (length > 0 && value.Data.String[length - 1] == std::numeric_limits<char>::max()) {
                --length;
            }

            if (length > 0) {
                char* newValue = rowBuffer->GetPool()->AllocateUnaligned(length);
                memcpy(newValue, value.Data.String, length);
                ++newValue[length - 1];

                value.Length = length;
                value.Data.String = newValue;
            } else {
                value = MakeSentinelValue<TUnversionedValue>(EValueType::Max);
            }
            result->Bounds.emplace_back(value, false);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
