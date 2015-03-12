#include "function_registry.h"

#include <mutex>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void TFunctionRegistry::RegisterFunction(std::unique_ptr<TFunctionDescriptor> function)
{
    Stroka functionName = to_lower(function->GetName());
    YCHECK(RegisteredFunctions_.insert(std::make_pair(functionName, std::move(function))).second);
}

TFunctionDescriptor& TFunctionRegistry::GetFunction(const Stroka& functionName)
{
    return *RegisteredFunctions_.at(to_lower(functionName));
}

bool TFunctionRegistry::IsRegistered(const Stroka& functionName)
{
    return RegisteredFunctions_.count(to_lower(functionName)) != 0;
}

void RegisterFunctionsImpl(TFunctionRegistry* registry)
{
    registry->RegisterFunction(std::make_unique<TIfFunction>());
    registry->RegisterFunction(std::make_unique<TIsPrefixFunction>());
    registry->RegisterFunction(std::make_unique<TIsSubstrFunction>());
    registry->RegisterFunction(std::make_unique<TLowerFunction>());
    registry->RegisterFunction(std::make_unique<TSimpleHashFunction>());
    registry->RegisterFunction(std::make_unique<TIsNullFunction>());
    registry->RegisterFunction(std::make_unique<TCastFunction>(
        EValueType::Int64,
        "int64"));
    registry->RegisterFunction(std::make_unique<TCastFunction>(
        EValueType::Uint64,
        "uint64"));
    registry->RegisterFunction(std::make_unique<TCastFunction>(
        EValueType::Double,
        "double"));
}

TFunctionRegistry* GetFunctionRegistry()
{
    static TFunctionRegistry registry;
    static std::once_flag onceFlag;
    std::call_once(onceFlag, &RegisterFunctionsImpl, &registry);
    return &registry;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
