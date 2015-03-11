#include "function_registry.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

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
