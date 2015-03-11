#include "function_registry.h"
#include "cg_fragment_compiler.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TTypedFunction : public TFunctionDescriptor
{
public:
    TTypedFunction(
        Stroka functionName,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType)
        : FunctionName(functionName)
        , ArgumentTypes(argumentTypes)
        , RepeatedArgumentType(repeatedArgumentType)
        , ResultType(resultType)
    {}

    TTypedFunction(
        Stroka functionName,
        std::vector<TType> argumentTypes,
        TType resultType)
        : TTypedFunction(
            functionName,
            argumentTypes,
            EValueType::Null,
            resultType)
    {}

    Stroka GetName()
    {
        return FunctionName;
    }

    EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source)
    {
        return TypingFunction(
                ArgumentTypes,
                RepeatedArgumentType,
                ResultType,
                GetName(),
                argumentTypes,
                source);
    }

private:
    EValueType TypingFunction(
        const std::vector<TType>& expectedArgTypes,
        TType repeatedArgType,
        TType resultType,
        Stroka functionName,
        const std::vector<EValueType>& argTypes,
        const TStringBuf& source);

    Stroka FunctionName;
    std::vector<TType> ArgumentTypes;
    TType RepeatedArgumentType;
    TType ResultType;
};

EValueType TTypedFunction::TypingFunction(
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


class IfFunction : public TTypedFunction
{
public:
    IfFunction() : TTypedFunction(
        "if",
        std::vector<TType>({ EValueType::Boolean, 0, 0 }),
        0)
    {}
};

class IsPrefixFunction : public TTypedFunction
{
public:
    IsPrefixFunction() : TTypedFunction(
        "is_prefix",
        std::vector<TType>({ EValueType::String, EValueType::String }),
        EValueType::Boolean)
    {}
};

class IsSubstrFunction : public TTypedFunction
{
public:
    IsSubstrFunction() : TTypedFunction(
        "is_substr",
        std::vector<TType>({ EValueType::String, EValueType::String }),
        EValueType::Boolean)
    {}
};

class LowerFunction : public TTypedFunction
{
public:
    LowerFunction() : TTypedFunction(
        "lower",
        std::vector<TType>({ EValueType::String }),
        EValueType::String)
    {}
};

class SimpleHashFunction : public TTypedFunction
{
public:
    SimpleHashFunction() : TTypedFunction(
        "simple_hash",
        std::vector<TType>({ HashTypes }),
        HashTypes,
        EValueType::String)
    {}

private:
    static const std::set<EValueType> HashTypes;
};

const std::set<EValueType> SimpleHashFunction::HashTypes = 
    std::set<EValueType>({
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String });

class IsNullFunction : public TTypedFunction
{
public:
    IsNullFunction() : TTypedFunction(
        "is_null",
        std::vector<TType>({ 0 }),
        EValueType::Boolean)
    {}
};

class CastFunction : public TTypedFunction
{
public:
    CastFunction( EValueType resultType, Stroka functionName)
        : TTypedFunction(
            functionName,
            std::vector<TType>({ CastTypes }),
            resultType)
    {}

private:
    static const std::set<EValueType> CastTypes;
};

const std::set<EValueType> CastFunction::CastTypes = std::set<EValueType>({
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double });


TFunctionRegistry::TFunctionRegistry() {
    this->RegisterFunction(std::make_unique<IfFunction>());
    this->RegisterFunction(std::make_unique<IsPrefixFunction>());
    this->RegisterFunction(std::make_unique<IsSubstrFunction>());
    this->RegisterFunction(std::make_unique<LowerFunction>());
    this->RegisterFunction(std::make_unique<SimpleHashFunction>());
    this->RegisterFunction(std::make_unique<IsNullFunction>());
    this->RegisterFunction(std::make_unique<CastFunction>(
        EValueType::Int64,
        "int64"));
    this->RegisterFunction(std::make_unique<CastFunction>(
        EValueType::Uint64,
        "uint64"));
    this->RegisterFunction(std::make_unique<CastFunction>(
        EValueType::Double,
        "double"));
}

void TFunctionRegistry::RegisterFunction(std::unique_ptr<TFunctionDescriptor> function)
{
    Stroka functionName = function->GetName();
    YCHECK(registeredFunctions.count(functionName) == 0);
    registeredFunctions.insert(std::pair<Stroka, std::unique_ptr<TFunctionDescriptor>>(functionName, std::move(function)));
}

TFunctionDescriptor& TFunctionRegistry::GetFunction(const Stroka& functionName)
{
    return *registeredFunctions.at(functionName);
}

bool TFunctionRegistry::IsRegistered(const Stroka& functionName)
{
    return registeredFunctions.count(functionName) != 0;
}

TFunctionRegistry* GetFunctionRegistry()
{
    static TFunctionRegistry registry;
    return &registry;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
