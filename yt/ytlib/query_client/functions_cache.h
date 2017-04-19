#pragma once

#include "public.h"
#include "functions.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/chunk_spec.pb.h>
#include <yt/ytlib/chunk_client/chunk_owner_ypath.pb.h>
#include <yt/ytlib/query_client/functions_cache.pb.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TExternalFunctionSpec
{
    // nullptr if empty
    NYTree::INodePtr Descriptor;

    std::vector<NChunkClient::NProto::TChunkSpec> Chunks;
    NNodeTrackerClient::NProto::TNodeDirectory NodeDirectory;
};

struct TExternalFunctionImpl
{
    bool IsAggregate;
    Stroka Name;
    Stroka SymbolName;
    ECallingConvention CallingConvention;
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;

    TType RepeatedArgType = EValueType::Min;
    int RepeatedArgIndex = -1;
    bool UseFunctionContext = false;
};

struct TExternalCGInfo
    : public TIntrinsicRefCounted
{
    TExternalCGInfo();

    std::vector<TExternalFunctionImpl> Functions;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TExternalFunctionSpec> LookupAllUdfDescriptors(
    const std::vector<Stroka>& functionNames,
    const Stroka& udfRegistryPath,
    NApi::INativeClientPtr client);

void AppendUdfDescriptors(
    const TTypeInferrerMapPtr& typers,
    const TExternalCGInfoPtr& cgInfo,
    const std::vector<Stroka>& names,
    const std::vector<TExternalFunctionSpec>& external);

////////////////////////////////////////////////////////////////////////////////

struct IFunctionRegistry
    : public virtual TRefCounted
{
    virtual TFuture<std::vector<TExternalFunctionSpec>> FetchFunctions(const std::vector<Stroka>& names) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateFunctionRegistryCache(
    const Stroka& udfRegistryPath,
    TExpiringCacheConfigPtr config,
    TWeakPtr<NApi::INativeClient> client,
    IInvokerPtr invoker);

TFunctionImplCachePtr CreateFunctionImplCache(
    const TSlruCacheConfigPtr& config,
    TWeakPtr<NApi::INativeClient> client);

////////////////////////////////////////////////////////////////////////////////

void FetchImplementations(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    const TConstExternalCGInfoPtr& externalCGInfo,
    TFunctionImplCachePtr cache);

void FetchJobImplementations(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    const TConstExternalCGInfoPtr& externalCGInfo,
    Stroka implementationPath);

////////////////////////////////////////////////////////////////////////////////

struct TDescriptorType
{
    TType Type = EValueType::Min;

};

void Serialize(const TDescriptorType& value, NYson::IYsonConsumer* consumer);
void Deserialize(TDescriptorType& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExternalFunctionImpl* proto, const TExternalFunctionImpl& options);
void FromProto(TExternalFunctionImpl* original, const NProto::TExternalFunctionImpl& serialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
