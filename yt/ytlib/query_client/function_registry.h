#pragma once

#include "public.h"

#include <ytlib/api/public.h>

#include <unordered_map>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class IFunctionRegistry
    : public TRefCounted
{
public:
    virtual ~IFunctionRegistry();

    virtual void RegisterFunction(IFunctionDescriptorPtr descriptor) = 0;

    virtual IFunctionDescriptor& GetFunction(const Stroka& functionName) = 0;

    virtual bool IsRegistered(const Stroka& functionName) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFunctionRegistry)

////////////////////////////////////////////////////////////////////////////////

class TFunctionRegistry
    : public IFunctionRegistry
{
public:
    virtual void RegisterFunction(IFunctionDescriptorPtr descriptor) override;
    virtual IFunctionDescriptor& GetFunction(const Stroka& functionName) override;
    virtual bool IsRegistered(const Stroka& functionName) override;

private:
    std::unordered_map<Stroka, IFunctionDescriptorPtr> RegisteredFunctions_;
};

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateBuiltinFunctionRegistry();
IFunctionRegistryPtr CreateFunctionRegistry(NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
