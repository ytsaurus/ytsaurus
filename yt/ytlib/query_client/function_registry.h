#pragma once

#include "functions.h"

#include <util/generic/stroka.h>

#include <unordered_map>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TFunctionRegistry
{
public:
    TFunctionRegistry();

    void RegisterFunction(std::unique_ptr<TFunctionDescriptor> descriptor);
    TFunctionDescriptor& GetFunction(const Stroka& functionName);

    bool IsRegistered(const Stroka& functionName);

private:
    std::unordered_map<Stroka, std::unique_ptr<TFunctionDescriptor>> registeredFunctions;
};

TFunctionRegistry* GetFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
