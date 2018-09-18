#pragma once

#include "public.h"
#include "functions.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/ytlib/chunk_client/public.h>
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
    bool IsAggregate = false;
    TString Name;
    TString SymbolName;
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
    const std::vector<TString>& functionNames,
    const TString& udfRegistryPath,
    const NApi::NNative::IClientPtr& client);

void AppendUdfDescriptors(
    const TTypeInferrerMapPtr& typeInferrers,
    const TExternalCGInfoPtr& cgInfo,
    const std::vector<TString>& functionNames,
    const std::vector<TExternalFunctionSpec>& externalFunctionSpecs);

////////////////////////////////////////////////////////////////////////////////

struct IFunctionRegistry
    : public virtual TRefCounted
{
    virtual TFuture<std::vector<TExternalFunctionSpec>> FetchFunctions(const std::vector<TString>& names) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateFunctionRegistryCache(
    const TString& udfRegistryPath,
    TAsyncExpiringCacheConfigPtr config,
    TWeakPtr<NApi::NNative::IClient> client,
    IInvokerPtr invoker);

TFunctionImplCachePtr CreateFunctionImplCache(
    const TSlruCacheConfigPtr& config,
    TWeakPtr<NApi::NNative::IClient> client);

////////////////////////////////////////////////////////////////////////////////

void FetchFunctionImplementationsFromCypress(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    const TConstExternalCGInfoPtr& externalCGInfo,
    const TFunctionImplCachePtr& cache,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions);

void FetchFunctionImplementationsFromFiles(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    const TConstExternalCGInfoPtr& externalCGInfo,
    const TString& rootPath);

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
