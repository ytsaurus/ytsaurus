#include "functions.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

namespace {

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
        if (type1 == EValueType::Null) {
            return true;
        }
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
                if (type2 == EValueType::Null) {
                    return true;
                } else if (genericAssignments[*genericId] == EValueType::Null) {
                    genericAssignments[*genericId] = type2;
                    return true;
                } else {
                    return genericAssignments[*genericId] == type2;
                }
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
                argIndex,
                functionName,
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
            && genericAssignments[typeArg] != EValueType::Null
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

    Y_UNREACHABLE();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

EValueType TFunctionTypeInferrer::InferResultType(
    const std::vector<EValueType>& argumentTypes,
    const Stroka& name,
    const TStringBuf& source) const
{
    return TypingFunction(
        TypeArgumentConstraints_,
        ArgumentTypes_,
        RepeatedArgumentType_,
        ResultType_,
        name,
        argumentTypes,
        source);
}

EValueType TAggregateTypeInferrer::InferStateType(
    EValueType type,
    const Stroka& aggregateName,
    const TStringBuf& source) const
{
    return TypingFunction(
        TypeArgumentConstraints_,
        std::vector<TType>{ArgumentType_},
        EValueType::Null,
        StateType_,
        aggregateName,
        std::vector<EValueType>{type},
        source);
}

EValueType TAggregateTypeInferrer::InferResultType(
    EValueType argumentType,
    const Stroka& aggregateName,
    const TStringBuf& source) const
{
    return TypingFunction(
        TypeArgumentConstraints_,
        std::vector<TType>{ArgumentType_},
        EValueType::Null,
        ResultType_,
        aggregateName,
        std::vector<EValueType>{argumentType},
        source);
    }

////////////////////////////////////////////////////////////////////////////////

const ITypeInferrerPtr& TTypeInferrerMap::GetFunction(const Stroka& functionName) const
{
    auto found = this->find(functionName);
    if (found == this->end()) {
        THROW_ERROR_EXCEPTION("Undefined function %Qv",
            functionName);
    }
    return found->second;
}

const IFunctionCodegenPtr& TFunctionProfilerMap::GetFunction(const Stroka& functionName) const
{
    auto found = this->find(functionName);
    if (found == this->end()) {
        THROW_ERROR_EXCEPTION("Code generator not found for regular function %Qv",
            functionName);
    }
    return found->second;
}

const IAggregateCodegenPtr& TAggregateProfilerMap::GetAggregate(const Stroka& functionName) const
{
    auto found = this->find(functionName);
    if (found == this->end()) {
        THROW_ERROR_EXCEPTION("Code generator not found for aggregate function %Qv",
            functionName);
    }
    return found->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT