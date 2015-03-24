#pragma once

#include "public.h"

#include <ytlib/api/public.h>

#include <unordered_map>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TFunctionRegistry
    : public TRefCounted
{
public:
    void RegisterFunction(IFunctionDescriptorPtr descriptor);
    IFunctionDescriptor& GetFunction(const Stroka& functionName);

    bool IsRegistered(const Stroka& functionName);

private:
    std::unordered_map<Stroka, IFunctionDescriptorPtr> RegisteredFunctions_;
};

DECLARE_REFCOUNTED_CLASS(TFunctionRegistry)
DEFINE_REFCOUNTED_TYPE(TFunctionRegistry)

TFunctionRegistryPtr CreateBuiltinFunctionRegistry();
TFunctionRegistryPtr CreateFunctionRegistry(NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
