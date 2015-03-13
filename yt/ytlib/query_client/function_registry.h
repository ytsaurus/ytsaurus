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
    void RegisterFunction(std::unique_ptr<IFunctionDescriptor> descriptor);
    IFunctionDescriptor& GetFunction(const Stroka& functionName);

    bool IsRegistered(const Stroka& functionName);

private:
    std::unordered_map<Stroka, std::unique_ptr<IFunctionDescriptor>> RegisteredFunctions_;
};

TFunctionRegistry* GetFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
