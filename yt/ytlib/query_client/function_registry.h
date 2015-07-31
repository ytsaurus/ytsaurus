#pragma once

#include "public.h"
#include "udf_descriptor.h"

#include <core/ypath/public.h>

#include <ytlib/api/public.h>

#include <unordered_map>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IFunctionRegistry
    : public virtual TRefCounted
{
    virtual IFunctionDescriptorPtr FindFunction(const Stroka& functionName) = 0;

    IFunctionDescriptorPtr GetFunction(const Stroka& functionName);

    virtual IAggregateFunctionDescriptorPtr FindAggregateFunction(const Stroka& aggregateName) = 0;

    IAggregateFunctionDescriptorPtr GetAggregateFunction(const Stroka& aggregateName);
};

DEFINE_REFCOUNTED_TYPE(IFunctionRegistry)

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateBuiltinFunctionRegistry();

IFunctionRegistryPtr CreateClientFunctionRegistry(NApi::IClientPtr client);

IFunctionRegistryPtr CreateJobFunctionRegistry(
    const std::vector<TUdfDescriptorPtr>& descriptors,
    TNullable<Stroka> implementationPath = Null,
    IFunctionRegistryPtr builtinRegistry = CreateBuiltinFunctionRegistry());

////////////////////////////////////////////////////////////////////////////////

Stroka GetUdfDescriptorPath(Stroka registryPath, Stroka functionName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
