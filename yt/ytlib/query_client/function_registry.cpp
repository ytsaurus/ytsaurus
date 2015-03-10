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

class IfFunction : public FunctionDescriptor
{
public:
    Stroka GetName()
    {
        return "if";
    }

    EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source)
    {
        return SimpleTypingFunction(
            std::vector<TType>({ EValueType::Boolean, 0, 0 }),
            EValueType::Null,
            0,
            GetName(),
            argumentTypes,
            source);
    }
};

class IsPrefixFunction : public FunctionDescriptor
{
    Stroka GetName()
    {
        return "is_prefix";
    }

    EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source)
    {
        return SimpleTypingFunction(
            std::vector<TType>({ EValueType::String, EValueType::String }),
            EValueType::Null,
            EValueType::Boolean,
            GetName(),
            argumentTypes,
            source);
    }
};

class IsSubstrFunction : public FunctionDescriptor
{
    Stroka GetName()
    {
        return "is_substr";
    }

    EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source)
    {
        return SimpleTypingFunction(
            std::vector<TType>({ EValueType::String, EValueType::String }),
            EValueType::Null,
            EValueType::Boolean,
            GetName(),
            argumentTypes,
            source);
    }
};

class LowerFunction : public FunctionDescriptor
{
    Stroka GetName()
    {
        return "lower";
    }

    EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source)
    {
        return SimpleTypingFunction(
            std::vector<TType>({ EValueType::String }),
            EValueType::Null,
            EValueType::String,
            GetName(),
            argumentTypes,
            source);
    }
};

class SimpleHashFunction : public FunctionDescriptor
{
public:
    Stroka GetName()
    {
        return "simple_hash";
    }

    EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source)
    {
        return SimpleTypingFunction(
            std::vector<TType>({ HashTypes }),
            HashTypes,
            EValueType::String,
            GetName(),
            argumentTypes,
            source);
    }

private:
    std::set<EValueType> HashTypes = std::set<EValueType>({
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String });

};

class IsNullFunction : public FunctionDescriptor
{
    Stroka GetName()
    {
        return "is_null";
    }

    EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source)
    {
        return SimpleTypingFunction(
            std::vector<TType>({ 0 }),
            EValueType::Null,
            EValueType::Boolean,
            GetName(),
            argumentTypes,
            source);
    }
};

class CastFunction : public FunctionDescriptor
{
public:
    CastFunction(
        EValueType resultType,
        Stroka functionName)
        : ResultType(resultType)
        , FunctionName(functionName)
    {}

    Stroka GetName()
    {
        return FunctionName;
    }

    EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source)
    {
        return SimpleTypingFunction(
            std::vector<TType>({ CastTypes }),
            EValueType::Null,
            ResultType,
            GetName(),
            argumentTypes,
            source);
    }

private:
    std::set<EValueType> CastTypes = std::set<EValueType>({
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double });

    EValueType ResultType;
    Stroka FunctionName;
};


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

void TFunctionRegistry::RegisterFunction(std::unique_ptr<FunctionDescriptor> function)
{
    Stroka functionName = function->GetName();
    YCHECK(registeredFunctions.count(functionName) == 0);
    registeredFunctions.insert(std::pair<Stroka, std::unique_ptr<FunctionDescriptor>>(functionName, std::move(function)));
}

FunctionDescriptor& TFunctionRegistry::GetFunction(const Stroka& functionName)
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
