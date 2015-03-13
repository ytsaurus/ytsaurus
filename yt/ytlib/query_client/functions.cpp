#include "functions.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TFunctionDescriptor::~TFunctionDescriptor()
{ }

TTypedFunction::TTypedFunction(
    Stroka functionName,
    std::vector<TType> argumentTypes,
    TType repeatedArgumentType,
    TType resultType)
    : FunctionName_(functionName)
    , ArgumentTypes_(argumentTypes)
    , RepeatedArgumentType_(repeatedArgumentType)
    , ResultType_(resultType)
{ }

TTypedFunction::TTypedFunction(
    Stroka functionName,
    std::vector<TType> argumentTypes,
    TType resultType)
    : TTypedFunction(
        functionName,
        argumentTypes,
        EValueType::Null,
        resultType)
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

TKeyTrieNode TUniversalRangeFunction::ExtractKeyRange(
    const TIntrusivePtr<const TFunctionExpression>& expr,
    const TKeyColumns& keyColumns,
    TRowBuffer* rowBuffer) const
{
    return TKeyTrieNode::Universal();
}

TCodegenExpression TCodegenFunction::MakeCodegenExpr(
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    Stroka name) const
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

const TUnionType TSimpleHashFunction::HashTypes_ = 
    TUnionType{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String };

const TUnionType TCastFunction::CastTypes_ = TUnionType{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double };

EValueType TTypedFunction::TypingFunction(
    const std::vector<TType>& expectedArgTypes,
    TType repeatedArgType,
    TType resultType,
    Stroka functionName,
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

    //TODO: better error messages
    auto arg = argTypes.begin();
    auto expectedArg = expectedArgTypes.begin();
    for (;
        expectedArg != expectedArgTypes.end() && arg != argTypes.end();
        arg++, expectedArg++)
    {
        if (!unify(*expectedArg, *arg)) {
            THROW_ERROR_EXCEPTION(
                "Wrong argument type",
                source);
        }
    }

    if (expectedArg != expectedArgTypes.end()) {
        THROW_ERROR_EXCEPTION(
            "Expression %Qv expects %v arguments",
            functionName,
            argTypes.size())
            << TErrorAttribute("expression", source);
    }

    for (; arg != argTypes.end(); arg++)
    {
        if (!unify(repeatedArgType, *arg)) {
            THROW_ERROR_EXCEPTION(
                "Wrong argument type",
                source);
        }
    }

    if (auto* genericResult = resultType.TryAs<TTypeArgument>()) {
        if (!genericAssignments.count(*genericResult)) {
            THROW_ERROR_EXCEPTION(
                "Ambiguous result type",
                source);
        }
        return genericAssignments[*genericResult];
    } else if (!resultType.TryAs<EValueType>()) {
        THROW_ERROR_EXCEPTION(
            "Ambiguous result type",
            source);
    } else {
        return resultType.As<EValueType>();
    }

    return EValueType::Null;
}

TIfFunction::TIfFunction() : TTypedFunction(
    "if",
    std::vector<TType>{ EValueType::Boolean, 0, 0 },
    0)
{ }

TIsPrefixFunction::TIsPrefixFunction() : TTypedFunction(
    "is_prefix",
    std::vector<TType>{ EValueType::String, EValueType::String },
    EValueType::Boolean)
{ }

TIsSubstrFunction::TIsSubstrFunction() : TTypedFunction(
    "is_substr",
    std::vector<TType>{ EValueType::String, EValueType::String },
    EValueType::Boolean)
{ }

TLowerFunction::TLowerFunction() : TTypedFunction(
    "lower",
    std::vector<TType>{ EValueType::String },
    EValueType::String)
{ }

TSimpleHashFunction::TSimpleHashFunction() : TTypedFunction(
    "simple_hash",
    std::vector<TType>{ HashTypes_ },
    HashTypes_,
    EValueType::String)
{ }

TIsNullFunction::TIsNullFunction() : TTypedFunction(
    "is_null",
    std::vector<TType>{ 0 },
    EValueType::Boolean)
{ }

TCastFunction::TCastFunction(EValueType resultType, Stroka functionName)
    : TTypedFunction(
        functionName,
        std::vector<TType>{ CastTypes_ },
        resultType)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
