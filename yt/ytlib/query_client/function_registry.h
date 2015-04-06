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

    virtual IFunctionDescriptorPtr FindFunction(const Stroka& functionName) = 0;

    IFunctionDescriptorPtr GetFunction(const Stroka& functionName);
};

DEFINE_REFCOUNTED_TYPE(IFunctionRegistry)

////////////////////////////////////////////////////////////////////////////////

class TFunctionRegistry
    : public IFunctionRegistry
{
public:
    void RegisterFunction(IFunctionDescriptorPtr descriptor);

    virtual IFunctionDescriptorPtr FindFunction(const Stroka& functionName);

private:
    std::unordered_map<Stroka, IFunctionDescriptorPtr> RegisteredFunctions_;
};

DEFINE_REFCOUNTED_TYPE(TFunctionRegistry)

////////////////////////////////////////////////////////////////////////////////

class TCypressFunctionRegistry
    : public IFunctionRegistry
{
public:
    TCypressFunctionRegistry(
        NApi::IClientPtr client,
        const Stroka& registryPath,
        TFunctionRegistryPtr builtinRegistry);

    virtual IFunctionDescriptorPtr FindFunction(const Stroka& functionName);

private:
    const NApi::IClientPtr Client_;
    const Stroka RegistryPath_;
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
