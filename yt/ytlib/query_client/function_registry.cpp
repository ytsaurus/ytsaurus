#include "function_registry.h"
#include "cg_fragment_compiler.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////
//TODO: move this back to plan_fragment?
typedef TVariant<EValueType, TTypeArgument, TUnionType> TType;
EValueType SimpleTypingFunction(
    const std::vector<TType>& expectedArgTypes,
    TType repeatedArgType,
    TType resultType,
    Stroka functionName,
    const std::vector<EValueType>& argTypes,
    const TStringBuf& source)
{
    std::unordered_map<TTypeArgument, EValueType> genericAssignments;

    auto isSubtype = [&] (EValueType type1, TType type2) {
        YCHECK(!type2.TryAs<TTypeArgument>());
        if (auto unionType = type2.TryAs<TUnionType>()) {
            return unionType->count(type1) > 0;
        } else if (auto concreteType = type2.TryAs<EValueType>()) {
            return type1 == *concreteType;
        }
        return false;
    };

    auto unify = [&] (TType type1, EValueType type2) {
        if (auto genericId = type1.TryAs<TTypeArgument>()) {
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
        TType ea = *expectedArg;
        if (!unify(ea, *arg)) {
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

    if (auto genericResult = resultType.TryAs<TTypeArgument>()) {
        if (!genericAssignments.count(*genericResult)) {
            //TODO: Maybe this should be checked on registering functions
            //      and just asserted here
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


TFunctionRegistry::TFunctionRegistry() {
    //TODO: add range builders
    auto getCodegenBuilder = [] (Stroka functionName) {
        return [] (
            std::vector<TCodegenExpression> codegenArgs,
            EValueType type,
            Stroka name) {
            //TODO
            return MakeCodegenFunctionExpr("is_prefix", codegenArgs, type, name);
          };
    };

    auto getTypingFunction = [&] (
        const std::vector<TType>& expectedArgTypes,
        TType repeatedType,
        TType resultType,
        Stroka functionName) {
        return [=] (
            std::vector<EValueType> argTypes,
            const TStringBuf& source) {
            return SimpleTypingFunction(
                expectedArgTypes,
                repeatedType,
                resultType,
                functionName,
                argTypes,
                source);
        };
    };


    this->RegisterFunction(
        "if",
        getTypingFunction(
            std::vector<TType>({ EValueType::Boolean, 0, 0 }),
            EValueType::Null,
            0,
            "if"),
        getCodegenBuilder("if"));

    this->RegisterFunction(
        "is_prefix",
        getTypingFunction(
            std::vector<TType>({ EValueType::String, EValueType::String }),
            EValueType::Null,
            EValueType::Boolean,
            "is_prefix"),
        getCodegenBuilder("is_prefix"));

    this->RegisterFunction(
        "is_substr",
        getTypingFunction(
            std::vector<TType>({ EValueType::String, EValueType::String }),
            EValueType::Null,
            EValueType::Boolean,
            "is_substr"),
        getCodegenBuilder("is_substr"));

    this->RegisterFunction(
        "lower",
        getTypingFunction(
            std::vector<TType>({ EValueType::String }),
            EValueType::Null,
            EValueType::String,
            "lower"),
        getCodegenBuilder("lower"));

    auto hashTypes = std::set<EValueType>({
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String });

    this->RegisterFunction(
        "simple_hash",
        getTypingFunction(
            std::vector<TType>({ hashTypes }),
            hashTypes,
            EValueType::String,
            "simple_hash"),
        getCodegenBuilder("simple_hash"));

    this->RegisterFunction(
        "is_null",
        getTypingFunction(
            std::vector<TType>({ 0 }),
            EValueType::Null,
            EValueType::Boolean,
            "is_null"),
        getCodegenBuilder("is_null"));

    auto castTypes = std::set<EValueType>({
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double });

    this->RegisterFunction(
        "int64",
        getTypingFunction(
            std::vector<TType>({ castTypes }),
            EValueType::Null,
            EValueType::Int64,
            "int64"),
        getCodegenBuilder("int64"));

    this->RegisterFunction(
        "uint64",
        getTypingFunction(
            std::vector<TType>({ castTypes }),
            EValueType::Null,
            EValueType::Uint64,
            "uint64"),
        getCodegenBuilder("uint64"));

    this->RegisterFunction(
        "double",
        getTypingFunction(
            std::vector<TType>({ castTypes }),
            EValueType::Null,
            EValueType::Double,
            "double"),
        getCodegenBuilder("double"));
}

void TFunctionRegistry::RegisterFunctionImpl(const Stroka& functionName, const TFunctionMetadata& functionMetadata)
{
    YCHECK(registeredFunctions.count(functionName) == 0);
    std::pair<Stroka, TFunctionMetadata> kv(functionName, functionMetadata);
    registeredFunctions.insert(kv);
}

//TODO: graceful handling of unregistered function lookups
TFunctionRegistry::TFunctionMetadata& TFunctionRegistry::LookupMetadata(const Stroka& functionName)
{
    return registeredFunctions.at(functionName);
}

void TFunctionRegistry::RegisterFunction(
    const Stroka& functionName,
    TTypingFunction typingFunction,
    TCodegenBuilder bodyBuilder,
    TRangeBuilder rangeBuilder)
{
    TFunctionMetadata functionMetadata(
        typingFunction,
        bodyBuilder,
        rangeBuilder);
    RegisterFunctionImpl(functionName, functionMetadata);
}


bool TFunctionRegistry::IsRegistered(const Stroka& functionName)
{
    return registeredFunctions.count(functionName) != 0;
}

TTypingFunction TFunctionRegistry::GetTypingFunction(
    const Stroka& functionName)
{
    return LookupMetadata(functionName).TypingFunction;
}

TCodegenBuilder TFunctionRegistry::GetCodegenBuilder(
    const Stroka& functionName,
    std::vector<EValueType> argumentTypes)
{
    return LookupMetadata(functionName).BodyBuilder;
}

TRangeBuilder TFunctionRegistry::ExtractRangeConstraints(
    const Stroka& functionName,
    std::vector<EValueType> argumentTypes)
{
    return LookupMetadata(functionName).RangeBuilder;
}

TFunctionRegistry* GetFunctionRegistry()
{
    static TFunctionRegistry registry;
    return &registry;
}

////////////////////////////////////////////////////////////////////////////////
} // namespace NQueryClient
} // namespace NYT
