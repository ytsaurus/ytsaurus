#pragma once

#include "public.h"

#include <unordered_map>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TFunctionRegistry
    : public TRefCounted
{
public:
    void RegisterFunction(IFunctionDescriptorPtr descriptor);
    IFunctionDescriptor& GetFunction(const Stroka& functionName) const;

    bool IsRegistered(const Stroka& functionName) const;

private:
    std::unordered_map<Stroka, IFunctionDescriptorPtr> RegisteredFunctions_;
};

DECLARE_REFCOUNTED_CLASS(TFunctionRegistry)
DEFINE_REFCOUNTED_TYPE(TFunctionRegistry)

TFunctionRegistryPtr CreateBuiltinFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
