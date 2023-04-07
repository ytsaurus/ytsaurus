#pragma once

#include <yt/yt/library/query/engine_api/public.h>

#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/functions.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/library/query/proto/functions_cache.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node_directory.pb.h>

namespace NYT::NQueryClient {

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
    : public TRefCounted
{
    TExternalCGInfo();

    std::vector<TExternalFunctionImpl> Functions;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TExternalFunctionSpec> LookupAllUdfDescriptors(
    const std::vector<std::pair<TString, TString>>& functionNames,
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
    virtual TFuture<std::vector<TExternalFunctionSpec>> FetchFunctions(
        const TString& udfRegistryPath,
        const std::vector<TString>& names) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateFunctionRegistryCache(
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
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions);

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
void Deserialize(TDescriptorType& value, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExternalFunctionImpl* proto, const TExternalFunctionImpl& options);
void FromProto(TExternalFunctionImpl* original, const NProto::TExternalFunctionImpl& serialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
