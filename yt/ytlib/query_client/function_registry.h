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

DEFINE_REFCOUNTED_TYPE(TFunctionRegistry)

////////////////////////////////////////////////////////////////////////////////

class IFunctionDescriptorFetcher
{
public:
    virtual ~IFunctionDescriptorFetcher();

    virtual IFunctionDescriptorPtr LookupFunction(const Stroka& functionName) = 0;
};

class TCypressFunctionRegistry
    : public TFunctionRegistry
{
public:
    TCypressFunctionRegistry(
        std::unique_ptr<IFunctionDescriptorFetcher> functionFetcher,
        TFunctionRegistryPtr builtinRegistry);

    virtual IFunctionDescriptor& GetFunction(const Stroka& functionName);

    virtual bool IsRegistered(const Stroka& functionName);

private:
    const std::unique_ptr<IFunctionDescriptorFetcher> FunctionFetcher_;
    const TFunctionRegistryPtr BuiltinRegistry_;
    const TFunctionRegistryPtr UDFRegistry_;

    void LookupAndRegister(const Stroka& functionName);
};

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateBuiltinFunctionRegistry();
IFunctionRegistryPtr CreateFunctionRegistry(NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
