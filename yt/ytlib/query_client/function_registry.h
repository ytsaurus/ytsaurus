#pragma once

#include "functions.h"

#include <unordered_map>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TFunctionRegistry
{
public:
    void RegisterFunction(TIntrusivePtr<IFunctionDescriptor> descriptor);
    IFunctionDescriptor& GetFunction(const Stroka& functionName);

    bool IsRegistered(const Stroka& functionName);

private:
    std::unordered_map<Stroka, TIntrusivePtr<IFunctionDescriptor>> RegisteredFunctions_;
};

TFunctionRegistry* GetFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
