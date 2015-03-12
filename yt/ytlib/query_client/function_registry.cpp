#include "function_registry.h"

#include <mutex>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void TFunctionRegistry::RegisterFunction(std::unique_ptr<TFunctionDescriptor> function)
{
    Stroka functionName = to_lower(function->GetName());
    YCHECK(RegisteredFunctions_.count(functionName) == 0);
    RegisteredFunctions_.insert(std::pair<Stroka, std::unique_ptr<TFunctionDescriptor>>(functionName, std::move(function)));
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
    registry->RegisterFunction(std::make_unique<IfFunction>());
    registry->RegisterFunction(std::make_unique<IsPrefixFunction>());
    registry->RegisterFunction(std::make_unique<IsSubstrFunction>());
    registry->RegisterFunction(std::make_unique<LowerFunction>());
    registry->RegisterFunction(std::make_unique<SimpleHashFunction>());
    registry->RegisterFunction(std::make_unique<IsNullFunction>());
    registry->RegisterFunction(std::make_unique<CastFunction>(
        EValueType::Int64,
        "int64"));
    registry->RegisterFunction(std::make_unique<CastFunction>(
        EValueType::Uint64,
        "uint64"));
    registry->RegisterFunction(std::make_unique<CastFunction>(
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
