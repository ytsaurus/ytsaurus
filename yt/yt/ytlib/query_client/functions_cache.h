#pragma once

#include <yt/yt/library/query/engine_api/public.h>

#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/functions.h>

#include <yt/yt/library/web_assembly/api/bytecode.h>

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
    std::string Name;
    std::string SymbolName;
    ECallingConvention CallingConvention;
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;

    TType RepeatedArgType = EValueType::Min;
    int RepeatedArgIndex = -1;
    bool UseFunctionContext = false;
};

struct TExternalSdkImpl
{
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
};

struct TExternalCGInfo
    : public TRefCounted
{
    TExternalCGInfo();

    std::vector<TExternalFunctionImpl> Functions;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;

    TExternalSdkImpl Sdk;
};

////////////////////////////////////////////////////////////////////////////////

struct TExternalFunction
{
    const NCodegen::EExecutionBackend ExecutionBackend;
    const NYPath::TYPath Path;
    const bool IsSdk;
    const std::string Name;

    operator size_t() const;
    bool operator==(const TExternalFunction& other) const;
};

void FormatValue(TStringBuilderBase* builder, const TExternalFunction& /*val*/, TStringBuf /*spec*/);

static_assert(CFormattable<TExternalFunction>, "TExternalFunction must be formattable for TAsyncExpiringCache");

////////////////////////////////////////////////////////////////////////////////

std::vector<TExternalFunctionSpec> LookupAllUdfDescriptors(
    const std::vector<TExternalFunction>& functionNames,
    const NApi::NNative::IClientPtr& client);

void AppendUdfDescriptors(
    const TTypeInferrerMapPtr& typeInferrers,
    const TExternalCGInfoPtr& cgInfo,
    const std::vector<std::string>& functionNames,
    const std::vector<TExternalFunctionSpec>& externalFunctionSpecs,
    NCodegen::EExecutionBackend executionBackend);

////////////////////////////////////////////////////////////////////////////////

struct IFunctionRegistry
    : public virtual TRefCounted
{
    virtual TFuture<std::vector<TExternalFunctionSpec>> FetchFunctions(
        const NYPath::TYPath& udfRegistryPath,
        const std::vector<std::string>& names,
        NCodegen::EExecutionBackend executionBackend) = 0;
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
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    NWebAssembly::TModuleBytecode* sdk,
    NCodegen::EExecutionBackend executionBackend);

void FetchFunctionImplementationsFromFiles(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    const TConstExternalCGInfoPtr& externalCGInfo,
    TStringBuf rootPath);

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

void ToProto(NProto::TExternalSdkImpl* proto, const TExternalSdkImpl& sdk);
void FromProto(TExternalSdkImpl* original, const NProto::TExternalSdkImpl& serialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
