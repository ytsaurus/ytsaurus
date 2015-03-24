#pragma once

#include "functions.h"
#include "public.h"

#include <unordered_map>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TFunctionRegistry
{
public:
    void RegisterFunction(IFunctionDescriptorPtr descriptor);
    IFunctionDescriptor& GetFunction(const Stroka& functionName);

    bool IsRegistered(const Stroka& functionName);

private:
    std::unordered_map<Stroka, IFunctionDescriptorPtr> RegisteredFunctions_;
};

TFunctionRegistry* GetFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
