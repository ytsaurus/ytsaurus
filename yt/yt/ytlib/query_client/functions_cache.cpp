#include "functions_cache.h"

#include <yt/yt/library/query/engine_api/append_function_implementation.h>

#include <yt/yt/library/query/base/private.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/api/file_reader.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/object_ypath_proxy.h>
#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/file_client/file_chunk_reader.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/async_expiring_cache.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

namespace NYT::NQueryClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NCodegen;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NFileClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NObjectClient::TObjectServiceProxy;
using NNodeTrackerClient::TNodeDirectoryPtr;
using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = QueryClientLogger;

struct TQueryUdfTag
{ };

////////////////////////////////////////////////////////////////////////////////

struct TCypressFunctionDescriptor
    : public NYTree::TYsonStruct
{
    TString Name;
    std::vector<TDescriptorType> ArgumentTypes;
    std::optional<TDescriptorType> RepeatedArgumentType;
    TDescriptorType ResultType;
    ECallingConvention CallingConvention;
    bool UseFunctionContext;

    std::vector<TType> GetArgumentsTypes() const
    {
        std::vector<TType> argumentTypes;
        for (const auto& type : ArgumentTypes) {
            argumentTypes.push_back(type.Type);
        }
        return argumentTypes;
    }

    REGISTER_YSON_STRUCT(TCypressFunctionDescriptor);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("name", &TThis::Name)
            .NonEmpty();
        registrar.Parameter("argument_types", &TThis::ArgumentTypes);
        registrar.Parameter("result_type", &TThis::ResultType);
        registrar.Parameter("calling_convention", &TThis::CallingConvention);
        registrar.Parameter("use_function_context", &TThis::UseFunctionContext)
            .Default(false);
        registrar.Parameter("repeated_argument_type", &TThis::RepeatedArgumentType)
            .Default();
    }
};

DECLARE_REFCOUNTED_STRUCT(TCypressFunctionDescriptor)
DEFINE_REFCOUNTED_TYPE(TCypressFunctionDescriptor)

struct TCypressAggregateDescriptor
    : public NYTree::TYsonStruct
{
    TString Name;
    TDescriptorType ArgumentType;
    TDescriptorType StateType;
    TDescriptorType ResultType;
    ECallingConvention CallingConvention;

    REGISTER_YSON_STRUCT(TCypressAggregateDescriptor);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("name", &TThis::Name)
            .NonEmpty();
        registrar.Parameter("argument_type", &TThis::ArgumentType);
        registrar.Parameter("state_type", &TThis::StateType);
        registrar.Parameter("result_type", &TThis::ResultType);
        registrar.Parameter("calling_convention", &TThis::CallingConvention);
    }
};

DECLARE_REFCOUNTED_STRUCT(TCypressAggregateDescriptor)
DEFINE_REFCOUNTED_TYPE(TCypressAggregateDescriptor)

DEFINE_REFCOUNTED_TYPE(TExternalCGInfo)

////////////////////////////////////////////////////////////////////////////////

static const std::string FunctionDescriptorAttribute("function_descriptor");
static const std::string AggregateDescriptorAttribute("aggregate_descriptor");
static const std::string IsWebAssemblySdkAttribute("is_web_assembly_sdk");

////////////////////////////////////////////////////////////////////////////////

TExternalCGInfo::TExternalCGInfo()
    : NodeDirectory(New<NNodeTrackerClient::TNodeDirectory>())
{ }

////////////////////////////////////////////////////////////////////////////////

TExternalFunction::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, ExecutionBackend);
    HashCombine(result, Path);
    HashCombine(result, IsSdk);
    HashCombine(result, Name);
    return result;
}

bool TExternalFunction::operator == (const TExternalFunction& /*other*/) const = default;

void FormatValue(TStringBuilderBase* builder, const TExternalFunction& value, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "(ExecutionBackend: %v, Path: %v, IsSdk: %v, Name: %v)",
        value.ExecutionBackend,
        value.Path,
        value.IsSdk,
        value.Name);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TString GetUdfDescriptorPath(const TYPath& registryPath, const std::string& functionName)
{
    return registryPath + "/" + ToYPathLiteral(functionName);
}

std::vector<TExternalFunctionSpec> LookupAllNativeUdfDescriptors(
    const std::vector<TExternalFunction>& functions,
    const NNative::IClientPtr& client)
{
    using NObjectClient::TObjectYPathProxy;
    using NApi::EMasterChannelKind;
    using NObjectClient::FromObjectId;
    using NNodeTrackerClient::TNodeDirectory;
    using NChunkClient::TLegacyReadRange;

    if (functions.empty()) {
        return {};
    }

    std::vector<TExternalFunctionSpec> result;

    YT_LOG_DEBUG("Looking for UDFs in Cypress");

    auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& function : functions) {
        auto path = GetUdfDescriptorPath(function.Path, function.Name);

        auto getReq = TYPathProxy::Get(path);
        ToProto(getReq->mutable_attributes()->mutable_keys(), std::vector<std::string>{
            FunctionDescriptorAttribute,
            AggregateDescriptorAttribute
        });
        batchReq->AddRequest(getReq, "get_attributes");

        auto basicAttributesReq = TObjectYPathProxy::GetBasicAttributes(path);
        batchReq->AddRequest(basicAttributesReq, "get_basic_attributes");
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    auto getRspsOrError = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_attributes");
    auto basicAttributesRspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_basic_attributes");

    THashMap<NObjectClient::TCellTag, std::vector<std::pair<NObjectClient::TObjectId, size_t>>> externalCellTagToInfo;
    for (int index = 0; index < std::ssize(functions); ++index) {
        const auto& function = functions[index];
        auto path = GetUdfDescriptorPath(function.Path, function.Name);

        auto getRspOrError = getRspsOrError[index];

        THROW_ERROR_EXCEPTION_IF_FAILED(
            getRspOrError,
            "Failed to find implementation of function %Qv at %v in Cypress",
            function.Name,
            function.Path);

        auto getRsp = getRspOrError
            .ValueOrThrow();

        auto basicAttrsRsp = basicAttributesRspsOrError[index]
            .ValueOrThrow();

        auto item = ConvertToNode(NYson::TYsonString(getRsp->value()));
        auto objectId = NYT::FromProto<NObjectClient::TObjectId>(basicAttrsRsp->object_id());
        auto cellTag = FromProto<TCellTag>(basicAttrsRsp->external_cell_tag());

        YT_LOG_DEBUG("Found UDF implementation in Cypress (Name: %v, Descriptor: %v)",
            function.Name,
            ConvertToYsonString(item, NYson::EYsonFormat::Text).AsStringBuf());

        TExternalFunctionSpec cypressInfo;
        cypressInfo.Descriptor = item;

        result.push_back(cypressInfo);

        externalCellTagToInfo[cellTag].emplace_back(objectId, index);
    }

    for (const auto& [externalCellTag, infos] : externalCellTagToInfo) {
        auto proxy = CreateObjectServiceReadProxy(
            client,
            EMasterChannelKind::Follower,
            externalCellTag);
        auto fetchBatchReq = proxy.ExecuteBatchWithRetries(client->GetNativeConnection()->GetConfig()->ChunkFetchRetries);

        for (auto [objectId, index] : infos) {
            auto fetchReq = TFileYPathProxy::Fetch(FromObjectId(objectId));
            AddCellTagToSyncWith(fetchReq, objectId);
            fetchReq->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            ToProto(fetchReq->mutable_ranges(), std::vector<TLegacyReadRange>({TLegacyReadRange()}));
            fetchBatchReq->AddRequest(fetchReq);
        }

        auto fetchBatchRsp = WaitFor(fetchBatchReq->Invoke())
            .ValueOrThrow();

        for (size_t rspIndex = 0; rspIndex < infos.size(); ++rspIndex) {
            auto resultIndex = infos[rspIndex].second;

            auto fetchRsp = fetchBatchRsp->GetResponse<TFileYPathProxy::TRspFetch>(rspIndex)
                .ValueOrThrow();

            auto nodeDirectory = New<TNodeDirectory>();

            NChunkClient::ProcessFetchResponse(
                client,
                fetchRsp,
                externalCellTag,
                nodeDirectory,
                10000,
                std::nullopt,
                Logger(),
                &result[resultIndex].Chunks);

            if (result[resultIndex].Chunks.empty()) {
                THROW_ERROR_EXCEPTION("UDF file is empty")
                    << TErrorAttribute("udf", functions[resultIndex]);
            }

            nodeDirectory->DumpTo(&result[resultIndex].NodeDirectory);
        }
    }

    return result;
}

std::vector<TYPath> GetUniqueDirectories(const std::vector<TExternalFunction>& functions)
{
    auto allDirectories = THashSet<TYPath>();

    for (const auto& function : functions) {
        allDirectories.insert(function.Path);
    }

    auto uniqueDirectories = std::vector<TYPath>();

    for (const auto& item : allDirectories) {
        uniqueDirectories.push_back(item);
    }

    return uniqueDirectories;
}

THashMap<TExternalFunction, i64> GetResultIndices(const std::vector<TExternalFunction>& functions)
{
    auto functionIndices = THashMap<TExternalFunction, i64>();

    for (i64 index = 0; index < std::ssize(functions); ++index) {
        InsertOrCrash(functionIndices, std::pair(functions[index], index));
    }

    return functionIndices;
}

std::pair<TObjectServiceProxy, std::vector<TErrorOr<TIntrusivePtr<TYPathProxy::TRspGet>>>> ListDirectory(
    const NNative::IClientPtr& client,
    std::vector<TYPath> uniqueDirectories)
{
    auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);

    auto batchReq = proxy.ExecuteBatch();

    for (const auto& directory : uniqueDirectories) {
        auto listReq = TYPathProxy::List(directory);
        ToProto(listReq->mutable_attributes()->mutable_keys(), std::vector<std::string>{
            FunctionDescriptorAttribute,
            AggregateDescriptorAttribute,
            IsWebAssemblySdkAttribute,
        });

        batchReq->AddRequest(listReq, "get_attributes");
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    auto getRspsOrError = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_attributes");

    return {proxy, getRspsOrError};
}

void UnpackFunctionDescriptors(
    const std::string& filename,
    const TYPath& directoryPath,
    const IAttributeDictionary& attributes,
    THashMap<TExternalFunction, TYPath>* functionToCypressPath,
    THashSet<TYPath>* neededBasicAttributes,
    std::vector<TExternalFunctionSpec>* result,
    const THashMap<TExternalFunction, i64>& resultIndices,
    const std::string& descriptorAttribute)
{
    auto functionDescriptors = attributes.Get<INodePtr>(descriptorAttribute)->AsList()->GetChildren();

    for (auto& item : functionDescriptors) {
        auto functionDescriptor = item->AsMap();

        auto nameNode = functionDescriptor->FindChild("name");
        THROW_ERROR_EXCEPTION_IF(
            !nameNode,
            "Name expected, but found nothing in %v",
            ConvertToYsonString(functionDescriptor, NYson::EYsonFormat::Text).AsStringBuf());

        auto name = nameNode->AsString();
        THROW_ERROR_EXCEPTION_IF(
            !name,
            "String name expected, but found %v",
            ConvertToYsonString(nameNode, NYson::EYsonFormat::Text).AsStringBuf());

        auto function = TExternalFunction(
            EExecutionBackend::WebAssembly,
            directoryPath,
            /*IsSdk*/ false,
            name->GetValue());

        THROW_ERROR_EXCEPTION_IF(
            functionToCypressPath->contains(function),
            "Function %Qv in %v has more than one definition",
            function.Name,
            function.Path);

        (*functionToCypressPath)[function] = YPathJoin(function.Path, filename);

        if (auto it = resultIndices.find(function); it != resultIndices.end()) {
            YT_LOG_DEBUG("Found UDF implementation in Cypress (NameAndPath: %v, Descriptor: %v)",
                function,
                ConvertToYsonString(functionDescriptor, NYson::EYsonFormat::Text).AsStringBuf());

            auto buffer = GetEphemeralNodeFactory()->CreateString();
            buffer->MutableAttributes()->Set(descriptorAttribute, functionDescriptor);
            buffer->SetValue(name->GetValue());
            (*result)[it->second].Descriptor = buffer;

            neededBasicAttributes->insert(YPathJoin(function.Path, filename));
        }
    }
}

void UnpackSdkDescriptors(
    const std::string& cypressFilename,
    const TYPath& cypressDirectoryPath,
    const IAttributeDictionary& attributes,
    THashMap<TExternalFunction, TYPath>* functionToCypressPath,
    THashSet<TYPath>* neededBasicAttributes,
    std::vector<TExternalFunctionSpec>* result,
    const THashMap<TExternalFunction, i64>& resultIndices)
{
    auto isWebAssemblySdkDescriptor = attributes.Get<INodePtr>(IsWebAssemblySdkAttribute);

    auto function = TExternalFunction(
        EExecutionBackend::WebAssembly,
        cypressDirectoryPath,
        /*IsSdk*/ true,
        /*Name*/ "");

    THROW_ERROR_EXCEPTION_IF(
        functionToCypressPath->contains(function),
        "Sdk in %v has more than one definition",
        function.Path);

    (*functionToCypressPath)[function] = YPathJoin(function.Path, cypressFilename);

    if (auto it = resultIndices.find(function); it != resultIndices.end()) {
        (*result)[it->second].Descriptor = isWebAssemblySdkDescriptor;
        neededBasicAttributes->insert(YPathJoin(function.Path, cypressFilename));
    }
}

THashMap<TYPath, std::pair<NObjectClient::TObjectId, TCellTag>> FetchBasicAttributes(
    const NNative::IClientPtr& client,
    const THashSet<TYPath>& neededBasicAttributes)
{
    auto paths = std::vector<TYPath>();
    for (auto& item : neededBasicAttributes) {
        paths.push_back(item);
    }

    auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);

    auto batchReq = proxy.ExecuteBatch();

    for (const auto& path : paths) {
        auto basicAttributesReq = TObjectYPathProxy::GetBasicAttributes(path);
        batchReq->AddRequest(basicAttributesReq, "get_basic_attributes");
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    auto getRspsOrError = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_attributes");

    auto basicAttributesRspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_basic_attributes");

    auto basicAttributes = THashMap<TYPath, std::pair<NObjectClient::TObjectId, TCellTag>>();

    for (i64 index = 0; index < std::ssize(paths); ++index) {
        THROW_ERROR_EXCEPTION_IF_FAILED(
            basicAttributesRspsOrError[index],
            "Failed to fetch basic attributes of at %v in Cypress",
            paths[index]);

        auto basicAttrsRsp = basicAttributesRspsOrError[index]
            .ValueOrThrow();

        auto objectId = NYT::FromProto<NObjectClient::TObjectId>(basicAttrsRsp->object_id());
        auto cellTag = FromProto<TCellTag>(basicAttrsRsp->external_cell_tag());

        basicAttributes[paths[index]] = {objectId, cellTag};
    }

    return basicAttributes;
}

using TExternalCellTagInfo = THashMap<NObjectClient::TCellTag, THashMap<NObjectClient::TObjectId, std::vector<TExternalFunction>>>;

TExternalCellTagInfo GroupByExternalCellTag(
    const THashMap<TExternalFunction, TYPath>& functionToCypressPath,
    const THashMap<TExternalFunction, i64>& resultIndices,
    const THashMap<TYPath, std::pair<NObjectClient::TObjectId, TCellTag>>& basicAttributes)
{
    auto externalCellInfo = TExternalCellTagInfo();

    for (auto& [nameAndPath, _] : resultIndices) {
        auto it = functionToCypressPath.find(nameAndPath);

        if (it == functionToCypressPath.end()) {
            if (nameAndPath.IsSdk) {
                THROW_ERROR_EXCEPTION("Could not find SDK implementation in %v", nameAndPath.Path);
            } else {
                THROW_ERROR_EXCEPTION("Could not find UDF %Qv implementation in %v", nameAndPath.Name, nameAndPath.Path);
            }
        }

        auto& filepath = it->second;

        auto basicAttributeIt = basicAttributes.find(filepath);

        THROW_ERROR_EXCEPTION_IF(
            basicAttributeIt == basicAttributes.end(),
            "Could not find basic attribute for UDF (Name: %v, CypressPath: %v)",
            nameAndPath,
            filepath);

        auto& [objectId, cellTag] = basicAttributeIt->second;

        externalCellInfo[cellTag][objectId].push_back(nameAndPath);
    }

    return externalCellInfo;
}

void FetchChunks(
    const std::vector<TExternalFunction>& functions,
    const NNative::IClientPtr& client,
    const NObjectClient::TCellTag& externalCellTag,
    const THashMap<NObjectClient::TObjectId, std::vector<TExternalFunction>>& infos,
    std::vector<TExternalFunctionSpec>* result,
    const THashMap<TExternalFunction, i64>& resultIndices)
{
    auto proxy = CreateObjectServiceReadProxy(
        client,
        EMasterChannelKind::Follower,
        externalCellTag);

    auto objectIds = std::vector<NObjectClient::TObjectId>();
    auto groupedFunctions = std::vector<std::vector<TExternalFunction>>();
    auto fetchBatchReq = proxy.ExecuteBatchWithRetries(client->GetNativeConnection()->GetConfig()->ChunkFetchRetries);

    for (auto& [objectId, namesAndPathList] : infos) {
        objectIds.push_back(objectId);
        groupedFunctions.push_back(namesAndPathList);

        auto fetchReq = TFileYPathProxy::Fetch(FromObjectId(objectId));
        AddCellTagToSyncWith(fetchReq, objectId);
        fetchReq->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        ToProto(fetchReq->mutable_ranges(), std::vector<TLegacyReadRange>({TLegacyReadRange()}));
        fetchBatchReq->AddRequest(fetchReq);
    }

    auto fetchBatchRsp = WaitFor(fetchBatchReq->Invoke())
        .ValueOrThrow();

    for (i64 rspIndex = 0; rspIndex < std::ssize(objectIds); ++rspIndex) {
        for (auto& function : groupedFunctions[rspIndex]) {
            auto it = resultIndices.find(function);
            THROW_ERROR_EXCEPTION_IF(
                it == resultIndices.end(),
                "Could not find UDF (NameAndPath: %v)",
                function);
            i64 resultIndex = it->second;

            auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();

            auto fetchRsp = fetchBatchRsp->GetResponse<TFileYPathProxy::TRspFetch>(rspIndex)
                .ValueOrThrow();

            NChunkClient::ProcessFetchResponse(
                client,
                fetchRsp,
                externalCellTag,
                nodeDirectory,
                /*maxChunksPerLocateRequest*/ 10000,
                std::nullopt,
                QueryClientLogger(),
                &(*result)[resultIndex].Chunks);

            if ((*result)[resultIndex].Chunks.empty()) {
                THROW_ERROR_EXCEPTION("UDF file is empty, %v %v", functions[resultIndex], resultIndex)
                    << TErrorAttribute("udf", functions[resultIndex]);
            }

            nodeDirectory->DumpTo(&(*result)[resultIndex].NodeDirectory);
        }
    }
}

std::vector<TExternalFunctionSpec> LookupAllWebAssemblyUdfDescriptors(
    const std::vector<TExternalFunction>& functions,
    const NNative::IClientPtr& client)
{
    using NObjectClient::TObjectYPathProxy;
    using NApi::EMasterChannelKind;
    using NObjectClient::FromObjectId;
    using NChunkClient::TLegacyReadRange;

    if (functions.empty()) {
        return {};
    }

    YT_LOG_DEBUG("Looking for WebAssembly UDFs in Cypress");

    auto uniqueDirectories = GetUniqueDirectories(functions);
    auto [proxy, getRspsOrError] = ListDirectory(client, uniqueDirectories);
    auto resultIndices = GetResultIndices(functions);
    auto function = THashMap<TExternalFunction, TYPath>();
    auto result = std::vector<TExternalFunctionSpec>();
    result.resize(std::ssize(functions));
    auto neededBasicAttributes = THashSet<TYPath>();

    for (i64 index = 0; index < std::ssize(uniqueDirectories); ++index) {
        THROW_ERROR_EXCEPTION_IF_FAILED(
            getRspsOrError[index],
            "Failed to list directory %v in Cypress",
            uniqueDirectories[index]);

        auto getRsp = getRspsOrError[index]
            .ValueOrThrow();

        auto directoryListing = ConvertToNode(NYson::TYsonString(getRsp->value()));

        auto directoryItems = directoryListing->AsList()->GetChildren();
        for (auto& directoryItem : directoryItems) {
            auto filename = directoryItem->AsString();

            auto& attributes = directoryItem->Attributes();
            if (attributes.Contains(FunctionDescriptorAttribute)) {
                UnpackFunctionDescriptors(
                    filename->GetValue(),
                    uniqueDirectories[index],
                    attributes,
                    &function,
                    &neededBasicAttributes,
                    &result,
                    resultIndices,
                    FunctionDescriptorAttribute);
            }

            if (attributes.Contains(AggregateDescriptorAttribute)) {
                UnpackFunctionDescriptors(
                    filename->GetValue(),
                    uniqueDirectories[index],
                    attributes,
                    &function,
                    &neededBasicAttributes,
                    &result,
                    resultIndices,
                    AggregateDescriptorAttribute);
            }

            if (attributes.Contains(IsWebAssemblySdkAttribute)) {
                UnpackSdkDescriptors(
                    filename->GetValue(),
                    uniqueDirectories[index],
                    attributes,
                    &function,
                    &neededBasicAttributes,
                    &result,
                    resultIndices);
            }
        }
    }

    auto basicAttributes = FetchBasicAttributes(client, neededBasicAttributes);
    auto externalCellTagInfo = GroupByExternalCellTag(function, resultIndices, basicAttributes);

    for (const auto& [externalCellTag, infos] : externalCellTagInfo) {
        FetchChunks(functions, client, externalCellTag, infos, &result, resultIndices);
    }

    return result;
}

} // namespace

std::vector<TExternalFunctionSpec> LookupAllUdfDescriptors(
    const std::vector<TExternalFunction>& functions,
    const NNative::IClientPtr& client)
{
    std::vector<TExternalFunction> nativeFunctions;
    std::vector<TExternalFunction> webAssemblyFunctions;

    for (auto& function : functions) {
        switch (function.ExecutionBackend) {
            case EExecutionBackend::Native:
                nativeFunctions.push_back(function);
                break;

            case EExecutionBackend::WebAssembly:
                webAssemblyFunctions.push_back(function);
                break;

            default:
                THROW_ERROR_EXCEPTION("Unknown execution backend: %Qv", function.ExecutionBackend);
        }
    }

    auto nativeDescriptors = LookupAllNativeUdfDescriptors(nativeFunctions, client);
    auto webAssemblyDescriptors = LookupAllWebAssemblyUdfDescriptors(webAssemblyFunctions, client);

    auto result = std::vector<TExternalFunctionSpec>();

    i64 nativeDescriptorsIndex = 0;
    i64 webAssemblyDescriptorsIndex = 0;

    for (auto& functionName : functions) {
        switch (functionName.ExecutionBackend) {
            case EExecutionBackend::Native:
                result.push_back(std::move(nativeDescriptors[nativeDescriptorsIndex++]));
                break;

            case EExecutionBackend::WebAssembly:
                result.push_back(std::move(webAssemblyDescriptors[webAssemblyDescriptorsIndex++]));
                break;

            default:
                THROW_ERROR_EXCEPTION("Unknown execution backend: %Qv", functionName.ExecutionBackend);
        }
    }

    return result;
}

void AppendNativeUdfDescriptors(
    const TTypeInferrerMapPtr& typeInferrers,
    const TExternalCGInfoPtr& cgInfo,
    const std::vector<std::string>& functionNames,
    const std::vector<TExternalFunctionSpec>& externalFunctionSpecs)
{
    YT_VERIFY(functionNames.size() == externalFunctionSpecs.size());

    YT_LOG_DEBUG("Appending UDF descriptors (Count: %v)", externalFunctionSpecs.size());

    for (size_t index = 0; index < externalFunctionSpecs.size(); ++index) {
        const auto& item = externalFunctionSpecs[index];
        const auto& descriptor = item.Descriptor;
        const auto& name = functionNames[index];

        YT_LOG_DEBUG("Appending UDF descriptor (Name: %v, Descriptor: %v)",
            name,
            ConvertToYsonString(descriptor, NYson::EYsonFormat::Text).AsStringBuf());

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

        YT_LOG_DEBUG("Appending UDF descriptor {%v}",
            MakeFormattableView(chunks, [] (TStringBuilderBase* builder, const NChunkClient::NProto::TChunkSpec& chunkSpec) {
                builder->AppendFormat("%v", FromProto<TGuid>(chunkSpec.chunk_id()));
            }));

        if (functionDescriptor) {
            YT_LOG_DEBUG("Appending function UDF descriptor (Name: %v)", name);

            functionBody.IsAggregate = false;
            functionBody.SymbolName = functionDescriptor->Name;
            functionBody.CallingConvention = functionDescriptor->CallingConvention;
            functionBody.RepeatedArgType = functionDescriptor->RepeatedArgumentType
                ? functionDescriptor->RepeatedArgumentType->Type
                : EValueType::Null,
            functionBody.RepeatedArgIndex = int(functionDescriptor->GetArgumentsTypes().size());
            functionBody.UseFunctionContext = functionDescriptor->UseFunctionContext;

            auto typer = functionDescriptor->RepeatedArgumentType
                ? CreateFunctionTypeInferrer(
                    functionDescriptor->ResultType.Type,
                    functionDescriptor->GetArgumentsTypes(),
                    /*typeParameterConstraints*/ {},
                    functionDescriptor->RepeatedArgumentType->Type)
                : CreateFunctionTypeInferrer(
                    functionDescriptor->ResultType.Type,
                    functionDescriptor->GetArgumentsTypes());

            typeInferrers->emplace(name, typer);
            cgInfo->Functions.push_back(std::move(functionBody));
        }

        if (aggregateDescriptor) {
            YT_LOG_DEBUG("Appending aggregate UDF descriptor (Name: %v)", name);

            functionBody.IsAggregate = true;
            functionBody.SymbolName = aggregateDescriptor->Name;
            functionBody.CallingConvention = aggregateDescriptor->CallingConvention;
            functionBody.RepeatedArgType = EValueType::Null;
            functionBody.RepeatedArgIndex = -1;

            auto typer = CreateAggregateTypeInferrer(
                aggregateDescriptor->ResultType.Type,
                aggregateDescriptor->ArgumentType.Type,
                aggregateDescriptor->StateType.Type);

            typeInferrers->emplace(name, typer);
            cgInfo->Functions.push_back(std::move(functionBody));
        }
    }
}

void AppendWebAssemblyUdfDescriptors(
    const TTypeInferrerMapPtr& typeInferrers,
    const TExternalCGInfoPtr& cgInfo,
    const std::vector<std::string>& functionNames,
    const std::vector<TExternalFunctionSpec>& externalFunctionSpecs)
{
    YT_LOG_DEBUG("Appending WebAssembly UDF descriptors (Functions: %v, Files: %v)",
        functionNames.size(),
        externalFunctionSpecs.size());

    if (std::ssize(functionNames) + 1 != std::ssize(externalFunctionSpecs)) {
        YT_LOG_ALERT("Could not load WebAssembly UDFs (Functions: %v, Files: %v)",
            functionNames.size(),
            externalFunctionSpecs.size());
        THROW_ERROR_EXCEPTION("Could not load WebAssembly UDFs");
    }

    for (i64 functionIndex = 0; functionIndex < std::ssize(functionNames); ++functionIndex) {
        auto& externalFunctionSpec = externalFunctionSpecs[functionIndex];
        auto& functionName = functionNames[functionIndex];

        if (!externalFunctionSpec.Descriptor) {
            YT_LOG_ALERT("Could not load WebAssembly UDFs descriptor (FunctionName: %v)", functionName);
            continue;
        }

        YT_LOG_DEBUG("Appending UDF descriptor (Name: %v Descriptor: %v)",
            functionName,
            ConvertToYsonString(externalFunctionSpec.Descriptor, NYson::EYsonFormat::Text).AsStringBuf());

        cgInfo->NodeDirectory->MergeFrom(externalFunctionSpec.NodeDirectory);

        const auto& attributes = externalFunctionSpec.Descriptor->Attributes();
        auto functionDescriptor = attributes.Find<TCypressFunctionDescriptorPtr>(
            FunctionDescriptorAttribute);

        YT_LOG_DEBUG_IF(functionDescriptor, "Appending UDF descriptor (FunctionName: %v, Descriptor: %v)",
            functionName,
            ConvertToYsonString(functionDescriptor, NYson::EYsonFormat::Text).AsStringBuf());

        auto aggregateDescriptor = attributes.Find<TCypressAggregateDescriptorPtr>(
            AggregateDescriptorAttribute);

        YT_LOG_DEBUG_IF(aggregateDescriptor, "Appending aggregate UDF descriptor (FunctionName: %v, Descriptor: %v)",
            functionName,
            ConvertToYsonString(aggregateDescriptor, NYson::EYsonFormat::Text).AsStringBuf());

        const auto& chunks = externalFunctionSpec.Chunks;

        TExternalFunctionImpl functionBody;
        functionBody.Name = functionName;
        functionBody.ChunkSpecs = chunks;

        YT_LOG_DEBUG("Appending UDF descriptor (Chunks: %v)",
            MakeFormattableView(chunks, [] (TStringBuilderBase* builder, const NChunkClient::NProto::TChunkSpec& chunkSpec) {
                builder->AppendFormat("%v", FromProto<TGuid>(chunkSpec.chunk_id()));
            }));

        if (functionDescriptor) {
            YT_ASSERT(functionDescriptor->Name == functionName);

            YT_LOG_DEBUG("Appending function UDF descriptor (Name: %v)", functionName);

            functionBody.IsAggregate = false;
            functionBody.SymbolName = functionDescriptor->Name;
            functionBody.CallingConvention = functionDescriptor->CallingConvention;
            functionBody.RepeatedArgType = functionDescriptor->RepeatedArgumentType
                ? functionDescriptor->RepeatedArgumentType->Type
                : EValueType::Null,
            functionBody.RepeatedArgIndex = std::ssize(functionDescriptor->GetArgumentsTypes());
            functionBody.UseFunctionContext = functionDescriptor->UseFunctionContext;

            auto typer = functionDescriptor->RepeatedArgumentType
                ? CreateFunctionTypeInferrer(
                    functionDescriptor->ResultType.Type,
                    functionDescriptor->GetArgumentsTypes(),
                    /*typeParameterConstraints*/ {},
                    functionDescriptor->RepeatedArgumentType->Type)
                : CreateFunctionTypeInferrer(
                    functionDescriptor->ResultType.Type,
                    functionDescriptor->GetArgumentsTypes());

            typeInferrers->emplace(functionName, typer);
            cgInfo->Functions.push_back(std::move(functionBody));
        } else if (aggregateDescriptor) {
            YT_ASSERT(functionDescriptor->Name == functionName);

            YT_LOG_DEBUG("Appending aggregate UDF descriptor (Name: %v)", functionName);

            functionBody.IsAggregate = true;
            functionBody.SymbolName = aggregateDescriptor->Name;
            functionBody.CallingConvention = aggregateDescriptor->CallingConvention;
            functionBody.RepeatedArgType = EValueType::Null;
            functionBody.RepeatedArgIndex = -1;

            auto typer = CreateAggregateTypeInferrer(
                aggregateDescriptor->ResultType.Type,
                aggregateDescriptor->ArgumentType.Type,
                aggregateDescriptor->StateType.Type);

            typeInferrers->emplace(functionName, typer);
            cgInfo->Functions.push_back(std::move(functionBody));
        } else {
            THROW_ERROR_EXCEPTION("function %Qv has no descriptors", functionName);
        }
    }

    auto sdk = externalFunctionSpecs.back();
    cgInfo->Sdk.ChunkSpecs = sdk.Chunks;
    YT_LOG_DEBUG("Appending SDK descriptor (Chunks: %v)",
        MakeFormattableView(cgInfo->Sdk.ChunkSpecs, [] (TStringBuilderBase* builder, const NChunkClient::NProto::TChunkSpec& chunkSpec) {
            builder->AppendFormat("%v", FromProto<TGuid>(chunkSpec.chunk_id()));
        }));
}

void AppendUdfDescriptors(
    const TTypeInferrerMapPtr& typeInferrers,
    const TExternalCGInfoPtr& cgInfo,
    const std::vector<std::string>& functionNames,
    const std::vector<TExternalFunctionSpec>& externalFunctionSpecs,
    EExecutionBackend executionBackend)
{
    switch (executionBackend) {
        case EExecutionBackend::Native:
            AppendNativeUdfDescriptors(typeInferrers, cgInfo, functionNames, externalFunctionSpecs);
            break;

        case EExecutionBackend::WebAssembly:
            AppendWebAssemblyUdfDescriptors(typeInferrers, cgInfo, functionNames, externalFunctionSpecs);
            break;

        default:
            THROW_ERROR_EXCEPTION("Unknown execution backend: %Qv", executionBackend);
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IFunctionRegistry)

namespace {

class TCypressFunctionRegistry
    : public TAsyncExpiringCache<TExternalFunction, TExternalFunctionSpec>
    , public IFunctionRegistry
{
public:
    TCypressFunctionRegistry(
        TAsyncExpiringCacheConfigPtr config,
        TWeakPtr<NNative::IClient> client,
        IInvokerPtr invoker)
        : TAsyncExpiringCache(
            std::move(config),
            invoker,
            QueryClientLogger().WithTag("Cache: CypressFunctionRegistry"))
        , Client_(std::move(client))
        , Invoker_(std::move(invoker))
    { }

    TFuture<std::vector<TExternalFunctionSpec>> FetchFunctions(
        const TYPath& udfRegistryPath,
        const std::vector<std::string>& names,
        NCodegen::EExecutionBackend executionBackend) override
    {
        auto keys = std::vector<TExternalFunction>();

        for (const auto& name : names) {
            keys.emplace_back(executionBackend, udfRegistryPath, /*IsSdk*/ false, name);
        }

        if (executionBackend == EExecutionBackend::WebAssembly) {
            keys.emplace_back(executionBackend, udfRegistryPath, /*IsSdk*/ true, /*Name*/ "");
        }

        return GetMany(keys)
            .Apply(BIND([names] (std::vector<TErrorOr<TExternalFunctionSpec>> specs) {
                int index = 0;
                for (const auto& spec : specs) {
                    if (!spec.IsOK()) {
                        THROW_ERROR_EXCEPTION("Function %Qv is not known", names[index])
                            << spec;
                    }
                    index++;
                }
                std::vector<TExternalFunctionSpec> result;
                result.reserve(specs.size());
                for (const auto& spec : specs) {
                    result.emplace_back(std::move(spec.Value()));
                }
                return result;
            }));
    }

private:
    const TWeakPtr<NNative::IClient> Client_;
    const IInvokerPtr Invoker_;

    TFuture<TExternalFunctionSpec> DoGet(
        const TExternalFunction& key,
        bool isPeriodicUpdate) noexcept override
    {
        return DoGetMany({key}, isPeriodicUpdate)
            .Apply(BIND([] (const std::vector<TErrorOr<TExternalFunctionSpec>>& specs) {
                return specs[0]
                    .ValueOrThrow();
            }));
    }

    TFuture<std::vector<TErrorOr<TExternalFunctionSpec>>> DoGetMany(
        const std::vector<TExternalFunction>& keys,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        if (auto client = Client_.Lock()) {
            auto future = BIND(LookupAllUdfDescriptors, keys, std::move(client))
                .AsyncVia(Invoker_)
                .Run();
            return future
                .Apply(BIND([] (const std::vector<TExternalFunctionSpec>& specs) {
                    return std::vector<TErrorOr<TExternalFunctionSpec>>(specs.begin(), specs.end());
                }));
        } else {
            return MakeFuture<std::vector<TErrorOr<TExternalFunctionSpec>>>(TError("Client destroyed"));
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateFunctionRegistryCache(
    TAsyncExpiringCacheConfigPtr config,
    TWeakPtr<NNative::IClient> client,
    IInvokerPtr invoker)
{
    return New<TCypressFunctionRegistry>(
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

    for (int index = 0; index < std::ssize(ChunkSpecs); ++index) {
        const auto& lhs = ChunkSpecs[index];
        const auto& rhs = other.ChunkSpecs[index];

        auto leftId = FromProto<TGuid>(lhs.chunk_id());
        auto rightId = FromProto<TGuid>(rhs.chunk_id());

        if (leftId != rightId)
            return false;
    }

    return true;
}

[[maybe_unused]] TString ToString(const TFunctionImplKey& key)
{
    return Format("{%v}", JoinToString(key.ChunkSpecs, [] (
        TStringBuilderBase* builder,
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

using TFunctionImplCacheEntryPtr = TIntrusivePtr<TFunctionImplCacheEntry>;

} // namespace

class TFunctionImplCache
    : public TAsyncSlruCacheBase<TFunctionImplKey, TFunctionImplCacheEntry>
{
public:
    TFunctionImplCache(
        const TSlruCacheConfigPtr& config,
        TWeakPtr<NNative::IClient> client)
        : TAsyncSlruCacheBase(config)
        , Client_(client)
    { }

    TFuture<TFunctionImplCacheEntryPtr> FetchImplementation(
        const TFunctionImplKey& key,
        TNodeDirectoryPtr nodeDirectory,
        const TClientChunkReadOptions& chunkReadOptions)
    {
        auto cookie = BeginInsert(key);
        if (!cookie.IsActive()) {
            return cookie.GetValue();
        }

        return BIND([=, this, this_ = MakeStrong(this), cookie = std::move(cookie)] (
                const TFunctionImplKey& key,
                TNodeDirectoryPtr nodeDirectory,
                const TClientChunkReadOptions& chunkReadOptions) mutable
            {
                try {
                    auto file = DoFetch(key, std::move(nodeDirectory), chunkReadOptions);
                    cookie.EndInsert(New<TFunctionImplCacheEntry>(key, file));
                } catch (const std::exception& ex) {
                    cookie.Cancel(TError(ex).Wrap("Failed to download function implementation"));
                }

                return cookie.GetValue();
            })
            .AsyncVia(GetCurrentInvoker())
            .Run(key, nodeDirectory, chunkReadOptions);
    }

private:
    const TWeakPtr<NNative::IClient> Client_;


    TSharedRef DoFetch(
        const TFunctionImplKey& key,
        const TNodeDirectoryPtr& nodeDirectory,
        const TClientChunkReadOptions& chunkReadOptions)
    {
        auto client = Client_.Lock();
        YT_VERIFY(client);
        auto chunks = key.ChunkSpecs;

        YT_LOG_DEBUG("Downloading implementation for UDF function (Chunks: %v, ReadSessionId: %v)",
            key,
            chunkReadOptions.ReadSessionId);

        client->GetNativeConnection()->GetNodeDirectory()->MergeFrom(nodeDirectory);

        // TODO(gepardo): pass correct data source here. See YT-16305 for details.
        auto reader = NFileClient::CreateFileMultiChunkReader(
            New<NApi::TFileReaderConfig>(),
            New<NChunkClient::TMultiChunkReaderOptions>(),
            New<TChunkReaderHost>(client),
            chunkReadOptions,
            std::move(chunks),
            MakeFileDataSource(std::nullopt));

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
        if (size == 0) {
            THROW_ERROR_EXCEPTION("UDF file is empty");
        }

        auto file = TSharedMutableRef::Allocate<TQueryUdfTag>(size);
        auto memoryOutput = TMemoryOutput(file.Begin(), size);

        for (const auto& block : blocks) {
            memoryOutput.Write(block.Begin(), block.Size());
        }

        return file;
    }
};

DEFINE_REFCOUNTED_TYPE(TFunctionImplCache)

TFunctionImplCachePtr CreateFunctionImplCache(
    const TSlruCacheConfigPtr& config,
    TWeakPtr<NNative::IClient> client)
{
    return New<TFunctionImplCache>(config, client);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TSharedRef GetImplFingerprint(const std::vector<NChunkClient::NProto::TChunkSpec>& chunks)
{
    auto size = chunks.size();
    auto fingerprint = TSharedMutableRef::Allocate<TQueryUdfTag>(2 * sizeof(ui64) * size);
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
    const TEnumIndexedArray<EExecutionBackend, TSharedRef>& impl)
{
    AppendFunctionImplementation(
        functionProfilers,
        aggregateProfilers,
        function.IsAggregate,
        function.Name,
        function.SymbolName,
        function.CallingConvention,
        GetImplFingerprint(function.ChunkSpecs),
        function.RepeatedArgType,
        function.RepeatedArgIndex,
        function.UseFunctionContext,
        impl);
}

} // namespace

void FetchFunctionImplementationsFromCypress(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    const TConstExternalCGInfoPtr& externalCGInfo,
    const TFunctionImplCachePtr& cache,
    const TClientChunkReadOptions& chunkReadOptions,
    NWebAssembly::TModuleBytecode* sdk,
    NCodegen::EExecutionBackend executionBackend)
{
    std::vector<TFuture<TFunctionImplCacheEntryPtr>> asyncResults;

    for (const auto& function : externalCGInfo->Functions) {
        const auto& name = function.Name;

        YT_LOG_DEBUG("Fetching UDF implementation (Name: %v, ReadSessionId: %v)",
            name,
            chunkReadOptions.ReadSessionId);

        TFunctionImplKey key;
        key.ChunkSpecs = function.ChunkSpecs;

        YT_LOG_DEBUG("Fetching UDF implementation (Name: %v, ChunkSpecs: %v)",
            name,
            key.ChunkSpecs);

        asyncResults.push_back(cache->FetchImplementation(key, externalCGInfo->NodeDirectory, chunkReadOptions));
    }

    if (executionBackend == EExecutionBackend::WebAssembly) {
        YT_LOG_DEBUG("Fetching SDK implementation (ReadSessionId: %v)",
            chunkReadOptions.ReadSessionId);

        TFunctionImplKey key;
        key.ChunkSpecs = externalCGInfo->Sdk.ChunkSpecs;

        YT_LOG_DEBUG("Fetching SDK implementation (ChunkSpecs: %v)",
            key.ChunkSpecs);

        asyncResults.push_back(cache->FetchImplementation(key, externalCGInfo->NodeDirectory, chunkReadOptions));
    }

    auto results = WaitFor(AllSucceeded(asyncResults))
        .ValueOrThrow();

    for (size_t index = 0; index < externalCGInfo->Functions.size(); ++index) {
        const auto& function = externalCGInfo->Functions[index];

        auto implementationFiles = TEnumIndexedArray<EExecutionBackend, TSharedRef>();
        implementationFiles[executionBackend] = results[index]->File;

        AppendFunctionImplementation(
            functionProfilers,
            aggregateProfilers,
            function,
            implementationFiles);
    }

    if (executionBackend == EExecutionBackend::WebAssembly) {
        sdk->Data = results.back()->File;
    }
}

void FetchFunctionImplementationsFromFiles(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    const TConstExternalCGInfoPtr& externalCGInfo,
    TStringBuf rootPath)
{
    for (const auto& function : externalCGInfo->Functions) {
        const auto& name = function.Name;

        YT_LOG_DEBUG("Fetching UDF implementation (Name: %v)", name);

        auto path = TString(rootPath) + "/" + function.Name;
        auto file = TUnbufferedFileInput(path);

        auto implementationFiles = TEnumIndexedArray<EExecutionBackend, TSharedRef>();
        implementationFiles[EExecutionBackend::Native] = TSharedRef::FromString(file.ReadAll());

        AppendFunctionImplementation(functionProfilers, aggregateProfilers, function, implementationFiles);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

ETypeTag TypeTagFromType(TType type)
{
    return Visit(type,
        [] (EValueType) {
            return ETypeTag::ConcreteType;
        },
        [] (const TTypeParameter&) {
            return ETypeTag::TypeParameter;
        },
        [] (const TUnionType&) {
            return ETypeTag::UnionType;
        },
        [] (const TLogicalTypePtr&) {
            return ETypeTag::LogicalType;
        });
}

} // namespace

void Serialize(const TDescriptorType& value, NYson::IYsonConsumer* consumer)
{
    using namespace NYTree;

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("tag").Value(TypeTagFromType(value.Type))
            .Do([&] (TFluentMap fluent) {
                Visit(value.Type,
                    [&] (const auto& v) { fluent.Item("value").Value(v); });
            })
        .EndMap();
}

void Deserialize(TDescriptorType& value, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    auto tag = ConvertTo<ETypeTag>(mapNode->GetChildOrThrow("tag"));
    auto valueNode = mapNode->GetChildOrThrow("value");
    switch (tag) {
        case ETypeTag::TypeParameter:
            value.Type = ConvertTo<TTypeParameter>(valueNode);
            break;
        case ETypeTag::UnionType:
            value.Type = ConvertTo<TUnionType>(valueNode);
            break;
        case ETypeTag::ConcreteType:
            value.Type = ConvertTo<EValueType>(valueNode);
            break;
        case ETypeTag::LogicalType:
            value.Type = ConvertTo<TLogicalTypePtr>(valueNode);
            break;
        default:
            YT_ABORT();
    }
}

void Deserialize(TDescriptorType& value, TYsonPullParserCursor* cursor)
{
    Deserialize(value, ExtractTo<INodePtr>(cursor));
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

    proto->set_repeated_arg_type(ToProto(ConvertToYsonString(descriptorType)));
    proto->set_repeated_arg_index(object.RepeatedArgIndex);
    proto->set_use_function_context(object.UseFunctionContext);
}

void FromProto(TExternalFunctionImpl* original, const NProto::TExternalFunctionImpl& serialized)
{
    original->IsAggregate = serialized.is_aggregate();
    original->Name = serialized.name();
    original->SymbolName = serialized.symbol_name();
    original->CallingConvention = FromProto<ECallingConvention>(serialized.calling_convention());
    original->ChunkSpecs = FromProto<std::vector<NChunkClient::NProto::TChunkSpec>>(serialized.chunk_specs());
    original->RepeatedArgType = ConvertTo<TDescriptorType>(NYson::TYsonString(serialized.repeated_arg_type())).Type;
    original->RepeatedArgIndex = serialized.repeated_arg_index();
    original->UseFunctionContext = serialized.use_function_context();
}

void ToProto(NProto::TExternalSdkImpl* proto, const TExternalSdkImpl& object)
{
    ToProto(proto->mutable_chunk_specs(), object.ChunkSpecs);
}

void FromProto(TExternalSdkImpl* original, const NProto::TExternalSdkImpl& serialized)
{
    original->ChunkSpecs = FromProto<std::vector<NChunkClient::NProto::TChunkSpec>>(serialized.chunk_specs());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
