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
    virtual void RegisterFunction(IFunctionDescriptorPtr descriptor);

    virtual IFunctionDescriptor& GetFunction(const Stroka& functionName);

    virtual bool IsRegistered(const Stroka& functionName);

private:
    std::unordered_map<Stroka, IFunctionDescriptorPtr> RegisteredFunctions_;
};

////////////////////////////////////////////////////////////////////////////////

class TCypressFunctionRegistry
    : public TFunctionRegistry
{
public:
    TCypressFunctionRegistry(
        NApi::IClientPtr client,
        IFunctionRegistryPtr builtinRegistry);

    virtual IFunctionDescriptor& GetFunction(const Stroka& functionName);

    virtual bool IsRegistered(const Stroka& functionName);

private:
    const NApi::IClientPtr Client_;
    const IFunctionRegistryPtr BuiltinRegistry_;
    const IFunctionRegistryPtr UDFRegistry_;

    void LookupInCypress(const Stroka& functionName);
};

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateBuiltinFunctionRegistry();
IFunctionRegistryPtr CreateFunctionRegistry(NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
