#include "functions_cache.h"
#include "functions_cg.h"
#include "private.h"

#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/config.h>
#include <yt/ytlib/api/file_reader.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/read_limit.h>
#include <yt/ytlib/chunk_client/block.h>

#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/object_ypath_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/file_client/file_chunk_reader.h>

#include <yt/core/logging/log.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/guid.h>
#include <yt/core/misc/async_cache.h>
#include <yt/core/misc/expiring_cache.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;
using namespace NFileClient;
using namespace NApi;

using NObjectClient::TObjectServiceProxy;
using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressFunctionDescriptor
    : public NYTree::TYsonSerializable
{
public:
    TString Name;
    std::vector<TDescriptorType> ArgumentTypes;
    TNullable<TDescriptorType> RepeatedArgumentType;
    TDescriptorType ResultType;
    ECallingConvention CallingConvention;
    bool UseFunctionContext;

    TCypressFunctionDescriptor()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("argument_types", ArgumentTypes);
        RegisterParameter("result_type", ResultType);
        RegisterParameter("calling_convention", CallingConvention);
        RegisterParameter("use_function_context", UseFunctionContext)
            .Default(false);
        RegisterParameter("repeated_argument_type", RepeatedArgumentType)
            .Default();
    }

    std::vector<TType> GetArgumentsTypes() const
    {
        std::vector<TType> argumentTypes;
        for (const auto& type : ArgumentTypes) {
            argumentTypes.push_back(type.Type);
        }
        return argumentTypes;
    }
};

DECLARE_REFCOUNTED_CLASS(TCypressFunctionDescriptor)
DEFINE_REFCOUNTED_TYPE(TCypressFunctionDescriptor)

class TCypressAggregateDescriptor
    : public NYTree::TYsonSerializable
{
public:
    TString Name;
    TDescriptorType ArgumentType;
    TDescriptorType StateType;
    TDescriptorType ResultType;
    ECallingConvention CallingConvention;

    TCypressAggregateDescriptor()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("argument_type", ArgumentType);
        RegisterParameter("state_type", StateType);
        RegisterParameter("result_type", ResultType);
        RegisterParameter("calling_convention", CallingConvention);
    }
};

DECLARE_REFCOUNTED_CLASS(TCypressAggregateDescriptor)
DEFINE_REFCOUNTED_TYPE(TCypressAggregateDescriptor)

DEFINE_REFCOUNTED_TYPE(TExternalCGInfo)

////////////////////////////////////////////////////////////////////////////////

static const TString FunctionDescriptorAttribute("function_descriptor");
static const TString AggregateDescriptorAttribute("aggregate_descriptor");

////////////////////////////////////////////////////////////////////////////////

TExternalCGInfo::TExternalCGInfo()
    : NodeDirectory(New<NNodeTrackerClient::TNodeDirectory>())
{ }

////////////////////////////////////////////////////////////////////////////////

namespace {

TString GetUdfDescriptorPath(const TYPath& registryPath, const TString& functionName)
{
    return registryPath + "/" + ToYPathLiteral(functionName);
}

} // namespace

std::vector<TExternalFunctionSpec> LookupAllUdfDescriptors(
    const std::vector<TString>& functionNames,
    const TString& udfRegistryPath,
    INativeClientPtr client)
{
    using NObjectClient::TObjectYPathProxy;
    using NApi::EMasterChannelKind;
    using NObjectClient::FromObjectId;
    using NNodeTrackerClient::TNodeDirectory;
    using NChunkClient::TReadRange;

    std::vector<TExternalFunctionSpec> result;

    LOG_DEBUG("Looking for UDFs in Cypress");

    auto attributeFilter = std::vector<TString>{
        FunctionDescriptorAttribute,
        AggregateDescriptorAttribute};

    TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Follower));
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& functionName : functionNames) {
        auto path = GetUdfDescriptorPath(udfRegistryPath, functionName);

        auto getReq = TYPathProxy::Get(path);

        ToProto(getReq->mutable_attributes()->mutable_keys(), attributeFilter);
        batchReq->AddRequest(getReq, "get_attributes");

        auto basicAttributesReq = TObjectYPathProxy::GetBasicAttributes(path);
        batchReq->AddRequest(basicAttributesReq, "get_basic_attributes");
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    auto getRspsOrError = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_attributes");
    auto basicAttributesRspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_basic_attributes");

    THashMap<NObjectClient::TCellTag, std::vector<std::pair<NObjectClient::TObjectId, size_t>>> infoByCellTags;

    for (int index = 0; index < functionNames.size(); ++index) {
        const auto& functionName = functionNames[index];
        auto path = GetUdfDescriptorPath(udfRegistryPath, functionName);

        auto getRspOrError = getRspsOrError[index];

        THROW_ERROR_EXCEPTION_IF_FAILED(getRspOrError, "Failed to find implementation of function %Qv in Cypress",
            functionName);

        auto getRsp = getRspOrError
            .ValueOrThrow();

        auto basicAttrsRsp = basicAttributesRspsOrError[index]
            .ValueOrThrow();

        auto item = ConvertToNode(NYson::TYsonString(getRsp->value()));
        auto objectId = NYT::FromProto<NObjectClient::TObjectId>(basicAttrsRsp->object_id());
        auto cellTag = basicAttrsRsp->cell_tag();

        LOG_DEBUG("Found UDF implementation in Cypress (Name: %v, Descriptor: %v)",
            functionName,
            ConvertToYsonString(item, NYson::EYsonFormat::Text).GetData());

        TExternalFunctionSpec cypressInfo;
        cypressInfo.Descriptor = item;

        result.push_back(cypressInfo);

        infoByCellTags[cellTag].emplace_back(objectId, index);
    }

    for (const auto& infoByCellTag : infoByCellTags) {
        const auto& cellTag = infoByCellTag.first;
        const auto& functionSpecs = infoByCellTag.second;

        TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Follower, cellTag));
        auto fetchBatchReq = proxy.ExecuteBatch();

        for (auto functionSpec : functionSpecs) {
            auto fetchReq = TFileYPathProxy::Fetch(FromObjectId(functionSpec.first));
            fetchReq->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            ToProto(fetchReq->mutable_ranges(), std::vector<TReadRange>({TReadRange()}));
            fetchBatchReq->AddRequest(fetchReq);
        }

        auto fetchBatchRsp = WaitFor(fetchBatchReq->Invoke())
            .ValueOrThrow();

        for (size_t rspIndex = 0; rspIndex < functionSpecs.size(); ++rspIndex) {
            auto resultIndex = functionSpecs[rspIndex].second;

            auto fetchRsp = fetchBatchRsp->GetResponse<TFileYPathProxy::TRspFetch>(rspIndex)
                .ValueOrThrow();

            auto nodeDirectory = New<TNodeDirectory>();

            NChunkClient::ProcessFetchResponse(
                client,
                fetchRsp,
                cellTag,
                nodeDirectory,
                10000,
                Null,
                Logger,
                &result[resultIndex].Chunks);

            YCHECK(!result[resultIndex].Chunks.empty());

            nodeDirectory->DumpTo(&result[resultIndex].NodeDirectory);
        }
    }

    return result;
}

void AppendUdfDescriptors(
    const TTypeInferrerMapPtr& typers,
    const TExternalCGInfoPtr& cgInfo,
    const std::vector<TString>& names,
    const std::vector<TExternalFunctionSpec>& external)
{
    YCHECK(names.size() == external.size());

    LOG_DEBUG("Appending UDF descriptors (Count: %v)", external.size());

    for (size_t index = 0; index < external.size(); ++index) {
        const auto& item = external[index];
        const auto& descriptor = item.Descriptor;
        const auto& name = names[index];

        LOG_DEBUG("Appending UDF descriptor (Name: %v, Descriptor: %v)",
            name,
            ConvertToYsonString(descriptor, NYson::EYsonFormat::Text).GetData());

        if (!descriptor) {
            continue;
        }
        cgInfo->NodeDirectory->MergeFrom(item.NodeDirectory);

        const auto& attributes = descriptor->Attributes();

        auto functionDescriptor = attributes.Find<TCypressFunctionDescriptorPtr>(
            FunctionDescriptorAttribute);
        auto aggregateDescriptor = attributes.Find<TCypressAggregateDescriptorPtr>(
            AggregateDescriptorAttribute);

        if (bool(functionDescriptor) == bool(aggregateDescriptor)) {
            THROW_ERROR_EXCEPTION("Item must have either function descriptor or aggregate descriptor");
        }

        const auto& chunks = item.Chunks;

        // NB(lukyan): Aggregate initialization is not supported in GCC in this case
        TExternalFunctionImpl functionBody;
        functionBody.Name = name;
        functionBody.ChunkSpecs = chunks;

        LOG_DEBUG("Appending UDF descriptor {%v}",
            MakeFormattableRange(chunks, [] (TStringBuilder* builder, const NChunkClient::NProto::TChunkSpec& chunkSpec) {
                builder->AppendFormat("%v", FromProto<TGuid>(chunkSpec.chunk_id()));
            }));

        if (functionDescriptor) {
            LOG_DEBUG("Appending function UDF descriptor %Qv", name);

            functionBody.IsAggregate = false;
            functionBody.SymbolName = functionDescriptor->Name;
            functionBody.CallingConvention = functionDescriptor->CallingConvention;
            functionBody.RepeatedArgType = functionDescriptor->RepeatedArgumentType
                ? functionDescriptor->RepeatedArgumentType->Type
                : EValueType::Null,
            functionBody.RepeatedArgIndex = int(functionDescriptor->GetArgumentsTypes().size());
            functionBody.UseFunctionContext = functionDescriptor->UseFunctionContext;

            auto typer = functionDescriptor->RepeatedArgumentType
                ? New<TFunctionTypeInferrer>(
                    std::unordered_map<TTypeArgument, TUnionType>(),
                    functionDescriptor->GetArgumentsTypes(),
                    functionDescriptor->RepeatedArgumentType->Type,
                    functionDescriptor->ResultType.Type)
                : New<TFunctionTypeInferrer>(
                    std::unordered_map<TTypeArgument, TUnionType>(),
                    functionDescriptor->GetArgumentsTypes(),
                    functionDescriptor->ResultType.Type);

            typers->emplace(name, typer);
            cgInfo->Functions.push_back(std::move(functionBody));
        }

        if (aggregateDescriptor) {
            LOG_DEBUG("Appending aggregate UDF descriptor %Qv", name);

            functionBody.IsAggregate = true;
            functionBody.SymbolName = aggregateDescriptor->Name;
            functionBody.CallingConvention = aggregateDescriptor->CallingConvention;
            functionBody.RepeatedArgType = EValueType::Null;
            functionBody.RepeatedArgIndex = -1;

            auto typer = New<TAggregateTypeInferrer>(
                std::unordered_map<TTypeArgument, TUnionType>(),
                aggregateDescriptor->ArgumentType.Type,
                aggregateDescriptor->ResultType.Type,
                aggregateDescriptor->StateType.Type);

            typers->emplace(name, typer);
            cgInfo->Functions.push_back(std::move(functionBody));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IFunctionRegistry)

namespace {

class TCypressFunctionRegistry
    : public TExpiringCache<TString, TExternalFunctionSpec>
    , public IFunctionRegistry
{
public:
    typedef TExpiringCache<TString, TExternalFunctionSpec> TBase;

    TCypressFunctionRegistry(
        const TString& registryPath,
        TExpiringCacheConfigPtr config,
        TWeakPtr<INativeClient> client,
        IInvokerPtr invoker)
        : TBase(config)
        , RegistryPath_(registryPath)
        , Client_(client)
        , Invoker_(invoker)
    { }

    virtual TFuture<std::vector<TExternalFunctionSpec>> FetchFunctions(const std::vector<TString>& names) override
    {
        return Get(names);
    }

private:
    const TString RegistryPath_;
    const TWeakPtr<INativeClient> Client_;
    const IInvokerPtr Invoker_;

    virtual TFuture<TExternalFunctionSpec> DoGet(const TString& key) override
    {
        return DoGetMany({key})
            .Apply(BIND([] (const std::vector<TExternalFunctionSpec>& result) {
                return result[0];
            }));
    }

    virtual TFuture<std::vector<TExternalFunctionSpec>> DoGetMany(const std::vector<TString>& keys) override
    {
        return BIND(LookupAllUdfDescriptors, keys, RegistryPath_, Client_.Lock())
            .AsyncVia(Invoker_)
            .Run();
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateFunctionRegistryCache(
    const TString& registryPath,
    TExpiringCacheConfigPtr config,
    TWeakPtr<INativeClient> client,
    IInvokerPtr invoker)
{
    return New<TCypressFunctionRegistry>(
        registryPath,
        std::move(config),
        std::move(client),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TFunctionImplKey
{
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;

    // Hasher.
    operator size_t() const;

    // Comparer.
    bool operator == (const TFunctionImplKey& other) const;
};

TFunctionImplKey::operator size_t() const
{
    size_t result = 0;

    for (const auto& spec : ChunkSpecs) {
        auto id = FromProto<TGuid>(spec.chunk_id());
        HashCombine(result, id);
    }

    return result;
}

bool TFunctionImplKey::operator == (const TFunctionImplKey& other) const
{
    if (ChunkSpecs.size() != other.ChunkSpecs.size())
        return false;

    for (int index = 0; index < ChunkSpecs.size(); ++index) {
        const auto& lhs = ChunkSpecs[index];
        const auto& rhs = other.ChunkSpecs[index];

        auto leftId = FromProto<TGuid>(lhs.chunk_id());
        auto rightId = FromProto<TGuid>(rhs.chunk_id());

        if (leftId != rightId)
            return false;
    }

    return true;
}

TString ToString(const TFunctionImplKey& key)
{
    return Format("{%v}", JoinToString(key.ChunkSpecs, [] (
        TStringBuilder* builder,
        const NChunkClient::NProto::TChunkSpec& chunkSpec)
    {
        builder->AppendFormat("%v", FromProto<TGuid>(chunkSpec.chunk_id()));
    }));
}

class TFunctionImplCacheEntry
    : public TAsyncCacheValueBase<TFunctionImplKey, TFunctionImplCacheEntry>
{
public:
    TFunctionImplCacheEntry(const TFunctionImplKey& key, TSharedRef file)
        : TAsyncCacheValueBase(key)
        , File(std::move(file))
    { }

    TSharedRef File;
};

typedef TIntrusivePtr<TFunctionImplCacheEntry> TFunctionImplCacheEntryPtr;

} // namespace

class TFunctionImplCache
    : public TAsyncSlruCacheBase<TFunctionImplKey, TFunctionImplCacheEntry>
{
public:
    TFunctionImplCache(
        const TSlruCacheConfigPtr& config,
        TWeakPtr<INativeClient> client)
        : TAsyncSlruCacheBase(config)
        , Client_(client)
    { }

    TSharedRef DoFetch(const TFunctionImplKey& key, TNodeDirectoryPtr nodeDirectory)
    {
        auto client = Client_.Lock();
        YCHECK(client);
        auto chunks = key.ChunkSpecs;

        auto reader = NFileClient::CreateFileMultiChunkReader(
            New<NApi::TFileReaderConfig>(),
            New<NChunkClient::TMultiChunkReaderOptions>(),
            client,
            NNodeTrackerClient::TNodeDescriptor(),
            client->GetNativeConnection()->GetBlockCache(),
            std::move(nodeDirectory),
            std::move(chunks));

        LOG_DEBUG("Downloading implementation for UDF function (Chunks: %v)", key);

        std::vector<TSharedRef> blocks;
        while (true) {
            NChunkClient::TBlock block;
            if (!reader->ReadBlock(&block)) {
                break;
            }

            if (block.Data) {
                blocks.push_back(std::move(block.Data));
            }

            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
        }

        i64 size = GetByteSize(blocks);
        YCHECK(size);
        auto file = TSharedMutableRef::Allocate(size);
        auto memoryOutput = TMemoryOutput(file.Begin(), size);

        for (const auto& block : blocks) {
            memoryOutput.Write(block.Begin(), block.Size());
        }

        return file;
    }

    TFuture<TFunctionImplCacheEntryPtr> FetchImplementation(
        const TFunctionImplKey& key,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto cookie = BeginInsert(key);
        if (cookie.IsActive()) {
            try {
                auto file = DoFetch(key, std::move(nodeDirectory));
                cookie.EndInsert(New<TFunctionImplCacheEntry>(key, file));
            } catch (const std::exception& ex) {
                cookie.Cancel(TError(ex).Wrap("Failed to download function implementation"));
            }
        }
        return cookie.GetValue();
    }

private:
    const TWeakPtr<INativeClient> Client_;

};

DEFINE_REFCOUNTED_TYPE(TFunctionImplCache)

TFunctionImplCachePtr CreateFunctionImplCache(
    const TSlruCacheConfigPtr& config,
    TWeakPtr<INativeClient> client)
{
    return New<TFunctionImplCache>(config, client);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TSharedRef GetImplFingerprint(const std::vector<NChunkClient::NProto::TChunkSpec>& chunks)
{
    auto size = chunks.size();
    auto fingerprint = TSharedMutableRef::Allocate(2 * sizeof(ui64) * size);
    auto memoryOutput = TMemoryOutput(fingerprint.Begin(), fingerprint.Size());

    for (const auto& chunk : chunks) {
        auto id = FromProto<TGuid>(chunk.chunk_id());
        memoryOutput.Write(id.Parts64, 2 * sizeof(ui64));
    }

    return fingerprint;
}

void AppendFunctionImplementation(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    const TExternalFunctionImpl& function,
    const TSharedRef& impl)
{
    YCHECK(!impl.Empty());

    const auto& name = function.Name;

    if (function.IsAggregate) {
        aggregateProfilers->emplace(name, New<TExternalAggregateCodegen>(
            name,
            impl,
            function.CallingConvention,
            GetImplFingerprint(function.ChunkSpecs)));
    } else {
        functionProfilers->emplace(name, New<TExternalFunctionCodegen>(
            name,
            function.SymbolName,
            impl,
            function.CallingConvention,
            function.RepeatedArgType,
            function.RepeatedArgIndex,
            function.UseFunctionContext,
            GetImplFingerprint(function.ChunkSpecs)));
    }
}

} // namespace

void FetchImplementations(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    const TConstExternalCGInfoPtr& externalCGInfo,
    TFunctionImplCachePtr cache)
{
    std::vector<TFuture<TFunctionImplCacheEntryPtr>> asyncResults;

    for (const auto& function : externalCGInfo->Functions) {
        const auto& name = function.Name;

        LOG_DEBUG("Fetching UDF implementation (Name: %v)", name);

        TFunctionImplKey key;
        key.ChunkSpecs = function.ChunkSpecs;

        auto cacheEntry = BIND(&TFunctionImplCache::FetchImplementation, cache)
            .AsyncVia(GetCurrentInvoker())
            .Run(key, externalCGInfo->NodeDirectory);

        asyncResults.push_back(cacheEntry);
    }

    auto results = WaitFor(Combine(asyncResults))
        .ValueOrThrow();

    for (size_t index = 0; index < externalCGInfo->Functions.size(); ++index) {
        const auto& function = externalCGInfo->Functions[index];
        AppendFunctionImplementation(functionProfilers, aggregateProfilers, function, results[index]->File);
    }
}

void FetchJobImplementations(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    const TConstExternalCGInfoPtr& externalCGInfo,
    TString implementationPath)
{
     for (const auto& function : externalCGInfo->Functions) {
        const auto& name = function.Name;

        LOG_DEBUG("Fetching UDF implementation (Name: %v)", name);

        auto path = implementationPath + "/" + function.Name;
            TUnbufferedFileInput file(path);
        auto impl =  TSharedRef::FromString(file.ReadAll());

        AppendFunctionImplementation(functionProfilers, aggregateProfilers, function, impl);
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TDescriptorType& value, NYson::IYsonConsumer* consumer)
{
    using namespace NYTree;

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("tag").Value(ETypeCategory(value.Type.Tag()))
            .DoIf(value.Type.TryAs<TTypeArgument>(), [&] (TFluentMap fluent) {
                fluent.Item("value").Value(value.Type.As<TTypeArgument>());
            })
            .DoIf(value.Type.TryAs<TUnionType>(), [&] (TFluentMap fluent) {
                fluent.Item("value").Value(value.Type.As<TUnionType>());
            })
            .DoIf(value.Type.TryAs<EValueType>(), [&] (TFluentMap fluent) {
                fluent.Item("value").Value(value.Type.As<EValueType>());
            })
        .EndMap();
}

void Deserialize(TDescriptorType& value, NYTree::INodePtr node)
{
    using namespace NYTree;

    auto mapNode = node->AsMap();

    auto tagNode = mapNode->GetChild("tag");
    ETypeCategory tag;
    Deserialize(tag, tagNode);

    auto valueNode = mapNode->GetChild("value");
    switch (tag) {
        case ETypeCategory::TypeArgument:
            {
                TTypeArgument type;
                Deserialize(type, valueNode);
                value.Type = type;
                break;
            }
        case ETypeCategory::UnionType:
            {
                TUnionType type;
                Deserialize(type, valueNode);
                value.Type = type;
                break;
            }
        case ETypeCategory::ConcreteType:
            {
                EValueType type;
                Deserialize(type, valueNode);
                value.Type = type;
                break;
            }
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExternalFunctionImpl* proto, const TExternalFunctionImpl& object)
{
    proto->set_is_aggregate(object.IsAggregate);
    proto->set_name(object.Name);
    proto->set_symbol_name(object.SymbolName);
    proto->set_calling_convention(int(object.CallingConvention));
    ToProto(proto->mutable_chunk_specs(), object.ChunkSpecs);

    TDescriptorType descriptorType;
    descriptorType.Type = object.RepeatedArgType;

    proto->set_repeated_arg_type(ConvertToYsonString(descriptorType).GetData());
    proto->set_repeated_arg_index(object.RepeatedArgIndex);
    proto->set_use_function_context(object.UseFunctionContext);
}

void FromProto(TExternalFunctionImpl* original, const NProto::TExternalFunctionImpl& serialized)
{
    original->IsAggregate = serialized.is_aggregate();
    original->Name = serialized.name();
    original->SymbolName = serialized.symbol_name();
    original->CallingConvention = ECallingConvention(serialized.calling_convention());
    original->ChunkSpecs = FromProto<std::vector<NChunkClient::NProto::TChunkSpec>>(serialized.chunk_specs());
    original->RepeatedArgType = ConvertTo<TDescriptorType>(NYson::TYsonString(serialized.repeated_arg_type())).Type;
    original->RepeatedArgIndex = serialized.repeated_arg_index();
    original->UseFunctionContext = serialized.use_function_context();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
