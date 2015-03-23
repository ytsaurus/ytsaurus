#pragma once

#include "public.h"

#include <unordered_map>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TFunctionRegistry
{
public:
    void RegisterFunction(IFunctionDescriptorPtr descriptor);
    IFunctionDescriptor& GetFunction(const Stroka& functionName) const;

    bool IsRegistered(const Stroka& functionName) const;

private:
    std::unordered_map<Stroka, IFunctionDescriptorPtr> RegisteredFunctions_;
};

TFunctionRegistry CreateBuiltinFunctionRegistry();
TFunctionRegistry* GetFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
