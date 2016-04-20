#pragma once

#include "public.h"
#include "udf_descriptor.h"

#include <yt/ytlib/api/public.h>

#include <yt/core/ypath/public.h>

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

class TFunctionRegistry
    : public IFunctionRegistry
{
public:
    IFunctionDescriptorPtr RegisterFunction(IFunctionDescriptorPtr descriptor);

    virtual IFunctionDescriptorPtr FindFunction(const Stroka& functionName) override;

    IAggregateFunctionDescriptorPtr RegisterAggregateFunction(IAggregateFunctionDescriptorPtr descriptor);

    virtual IAggregateFunctionDescriptorPtr FindAggregateFunction(const Stroka& aggregateName) override;

private:
    std::unordered_map<Stroka, IFunctionDescriptorPtr> RegisteredFunctions_;
    std::unordered_map<Stroka, IAggregateFunctionDescriptorPtr> RegisteredAggregateFunctions_;
    TSpinLock Lock_;
};

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateBuiltinFunctionRegistry();

IFunctionRegistryPtr CreateClientFunctionRegistry(NApi::IClientPtr client);

IFunctionRegistryPtr CreateJobFunctionRegistry(
    const std::vector<TUdfDescriptorPtr>& descriptors,
    TNullable<Stroka> implementationPath = Null,
    IFunctionRegistryPtr builtinRegistry = CreateBuiltinFunctionRegistry());

////////////////////////////////////////////////////////////////////////////////

Stroka GetUdfDescriptorPath(const NYPath::TYPath& registryPath, const Stroka& functionName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
