#include "storage.h"

#include "private.h"

#include "attributes_helpers.h"
#include "auth_token.h"
#include "chunk_reader.h"
#include "client_cache.h"
#include "convert_row.h"
#include "data_slice.h"
#include "document.h"
#include "partition_tables.h"
#include "path.h"
#include "read_job.h"
#include "read_job_spec.h"
#include "schemaful_table_reader.h"
#include "table_reader.h"
#include "table_schema.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>
#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/api/file_reader.h>
#include <yt/client/api/transaction.h>
#include <yt/client/node_tracker_client/node_directory.h>
#include <yt/client/ypath/rich.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/yson/string.h>
#include <yt/core/ytree/convert.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/string/join.h>

#include <stack>
#include <vector>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NApi;
using namespace NYT::NChunkClient;
using namespace NYT::NConcurrency;
using namespace NYT::NCypressClient;
using namespace NYT::NNodeTrackerClient;
using namespace NYT::NObjectClient;
using namespace NYT::NTableClient;
using namespace NYT::NTransactionClient;
using namespace NYT::NYPath;
using namespace NYT::NYTree;
using namespace NYT::NYson;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TStringBuf NODE_TYPE_MAP = "map_node";
const TStringBuf NODE_TYPE_TABLE = "table";
const TStringBuf NODE_TYPE_DOCUMENT = "document";

////////////////////////////////////////////////////////////////////////////////

TString GetAbsolutePath(const TString& path)
{
    if (path.empty()) {
        return "/";
    }
    if (path.StartsWith("//")) {
        return path;
    }
    return "//" + path;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
NInterop::TTableList BuildTablesList(
    const TString& rootPath,
    T&& fetchNode,
    bool recursive = false)
{
    const NLogging::TLogger& Logger = ServerLogger;

    NInterop::TTableList tables;

    LOG_DEBUG("Start traverse from %Qlv", rootPath);

    std::stack<INodePtr> queue;
    if (auto rootNode = fetchNode(rootPath)) {
        LOG_DEBUG("Add root node %Qlv", rootPath);
        queue.push(rootNode);
    }

    while (!queue.empty()) {
        auto node = queue.top();
        queue.pop();

        const auto& attrs = node->Attributes();
        auto type = attrs.Get<TString>("type");
        auto path = attrs.Get<TString>("path");

        LOG_DEBUG("Current node: %Qlv, type: %Qlv", path, type);

        if (type == NODE_TYPE_TABLE) {
            auto table = std::make_shared<NInterop::TTable>(path);
            tables.emplace_back(std::move(table));
        } else if ((recursive && type == NODE_TYPE_MAP) || path == rootPath) {
            for (const auto& kv: node->AsMap()->GetChildren()) {
                if (kv.second->GetType() == ENodeType::Entity) {
                    if (auto node = fetchNode(path + "/" + kv.first)) {
                        queue.push(node);
                    }
                } else {
                    queue.push(kv.second);
                }
            }
        }
    }

    Sort(tables, [] (const NInterop::TTablePtr& l, const NInterop::TTablePtr& r) {
        return l->Name < r->Name;
    });

    return tables;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TTableObject
    : public TUserObject
{
    int ChunkCount = 0;
    bool Dynamic = false;
    TTableSchema Schema;
};

////////////////////////////////////////////////////////////////////////////////

class TStorage
    : public TRefCounted
{
private:
    const NLogging::TLogger& Logger = ServerLogger;
    const NNative::IConnectionPtr Connection;
    const INativeClientCachePtr ClientCache;
    const IThroughputThrottlerPtr ScanThrottler;

public:
    TStorage(NNative::IConnectionPtr connection,
             INativeClientCachePtr clientCache,
             IThroughputThrottlerPtr scanThrottler)
        : Connection(std::move(connection))
        , ClientCache(std::move(clientCache))
        , ScanThrottler(std::move(scanThrottler))
    {}

    TFuture<NInterop::TTableList> ListTables(
        const NInterop::IAuthorizationToken& token,
        const TString& path,
        bool recursive)
    {
        return BIND(&TStorage::DoListTables, MakeStrong(this))
            .AsyncVia(Connection->GetInvoker())
            .Run(CreateNativeClient(token), path, recursive);
    }

    TFuture<NInterop::TTablePtr> GetTable(
        const NInterop::IAuthorizationToken& token,
        const TString& name)
    {
        return BIND(&TStorage::DoGetTable, MakeStrong(this))
            .AsyncVia(Connection->GetInvoker())
            .Run(CreateNativeClient(token), name);
    }

    TFuture<NInterop::TTablePartList> GetTablesParts(
        const NInterop::IAuthorizationToken& token,
        const std::vector<TString>& names,
        const NInterop::IRangeFilterPtr& rangeFilter,
        size_t maxTableParts)
    {
        return BIND(&TStorage::DoGetTablesParts, MakeStrong(this))
            .AsyncVia(Connection->GetInvoker())
            .Run(CreateNativeClient(token), names, rangeFilter, maxTableParts);
    }

    TFuture<NInterop::TTableReaderList> CreateTableReaders(
        const NInterop::IAuthorizationToken& token,
        const TString& jobSpec,
        const NInterop::TStringList& columns,
        const NInterop::TSystemColumns& systemColumns,
        size_t maxStreamCount,
        const NInterop::TTableReaderOptions& options)
    {
        return BIND(&TStorage::DoCreateTableReaders, MakeStrong(this))
            .AsyncVia(Connection->GetInvoker())
            .Run(CreateNativeClient(token),
                jobSpec, columns, systemColumns, maxStreamCount, options);
    }

    TFuture<NInterop::ITableReaderPtr> CreateTableReader(
        const NInterop::IAuthorizationToken& token,
        const TString& name,
        const NInterop::TTableReaderOptions& options)
    {
        return BIND(&TStorage::DoCreateTableReader, MakeStrong(this))
            .AsyncVia(Connection->GetInvoker())
            .Run(CreateNativeClient(token), name, options);
    }

    TFuture<TString> ReadFile(
        const NInterop::IAuthorizationToken& token,
        const TString& name)
    {
        return BIND(&TStorage::DoReadFile, MakeStrong(this))
            .AsyncVia(Connection->GetInvoker())
            .Run(CreateNativeClient(token), name);
    }

    TFuture<NInterop::IDocumentPtr> ReadDocument(
        const NInterop::IAuthorizationToken& token,
        const TString& name)
    {
        return BIND(&TStorage::DoReadDocument, MakeStrong(this))
            .AsyncVia(Connection->GetInvoker())
            .Run(CreateNativeClient(token), name);
    }

    TFuture<bool> Exists(
        const NInterop::IAuthorizationToken& token,
        const TString& name)
    {
        return BIND(&TStorage::DoExists, MakeStrong(this))
            .AsyncVia(Connection->GetInvoker())
            .Run(CreateNativeClient(token), name);
    }

    TFuture<NInterop::TObjectList> ListObjects(
        const NInterop::IAuthorizationToken& token,
        const TString& name)
    {
        return BIND(&TStorage::DoListObjects, MakeStrong(this))
            .AsyncVia(Connection->GetInvoker())
            .Run(CreateNativeClient(token), name);
    }

    TFuture<NInterop::TObjectAttributes> GetObjectAttributes(
        const NInterop::IAuthorizationToken& token,
        const TString& name)
    {
        return BIND(&TStorage::DoGetObjectAttributes, MakeStrong(this))
            .AsyncVia(Connection->GetInvoker())
            .Run(CreateNativeClient(token), name);
    }

    TFuture<TMaybe<NInterop::TRevision>> GetObjectRevision(
        const NInterop::IAuthorizationToken& token,
        const TString& name,
        bool throughCache)
    {
        return BIND(&TStorage::DoGetObjectRevision, MakeStrong(this))
            .AsyncVia(Connection->GetInvoker())
            .Run(CreateNativeClient(token), name, throughCache);
    }

private:
    NNative::IClientPtr CreateNativeClient(
        const NInterop::IAuthorizationToken& token);

    std::unique_ptr<TTableObject> GetTableAttributes(
        NApi::NNative::IClientPtr client,
        ITransactionPtr transaction,
        const TRichYPath& path,
        EPermission permission,
        bool suppressAccessTracking);

    IMapNodePtr GetAttributes(
        const NNative::IClientPtr& client,
        const TString& path,
        const std::vector<TString>& attributes);

    INodePtr GetAttribute(
        const NNative::IClientPtr& client,
        const TString& path,
        const TString& attribute);

    NInterop::TTableList DoListTables(
        const NNative::IClientPtr& client,
        const TString& path,
        bool recursive);

    NInterop::TTablePtr DoGetTable(
        const NNative::IClientPtr& client,
        const TString& name);

    NInterop::TTablePartList DoGetTablesParts(
        const NNative::IClientPtr& client,
        const std::vector<TString>& names,
        const NInterop::IRangeFilterPtr& rangeFilter,
        size_t maxTableParts);

    NInterop::TTableReaderList DoCreateTableReaders(
        const NNative::IClientPtr& client,
        const TString& jobSpec,
        const NInterop::TStringList& columns,
        const NInterop::TSystemColumns& systemColumns,
        size_t maxStreamCount,
        const NInterop::TTableReaderOptions& options);

    NInterop::ITableReaderPtr DoCreateTableReader(
        const NNative::IClientPtr& client,
        const TString& name,
        const NInterop::TTableReaderOptions& options);

    TString DoReadFile(
        const NNative::IClientPtr& client,
        const TString& name);

    NInterop::IDocumentPtr DoReadDocument(
        const NNative::IClientPtr& client,
        const TString& name);

    bool DoExists(
        const NNative::IClientPtr& client,
        const TString& name);

    NInterop::TObjectList DoListObjects(
        const NNative::IClientPtr& client,
        const TString& name);

    NInterop::TObjectAttributes DoGetObjectAttributes(
        const NNative::IClientPtr& client,
        const TString& name);

    TMaybe<NInterop::TRevision> DoGetObjectRevision(
        const NNative::IClientPtr& client,
        const TString& name,
        bool throughCache);

};

DECLARE_REFCOUNTED_CLASS(TStorage);
DEFINE_REFCOUNTED_TYPE(TStorage);

////////////////////////////////////////////////////////////////////////////////

NNative::IClientPtr TStorage::CreateNativeClient(
    const NInterop::IAuthorizationToken& token)
{
    return ClientCache->CreateNativeClient(UnwrapAuthToken(token));
}

NInterop::TTableList TStorage::DoListTables(
    const NNative::IClientPtr& client,
    const TString& path,
    bool recursive)
{
    LOG_INFO("Requesting tables list in %Qlv", path);

    TGetNodeOptions options;
    options.Attributes = {
        "type",
        "path",
    };

    // do not modify last-access timestamp
    options.SuppressAccessTracking = true;

    auto fetchNode = [&] (const TString& path) -> INodePtr {
        auto rspOrError = WaitFor(client->GetNode(path, options));
        if (!rspOrError.IsOK()) {
            auto error = rspOrError.Wrap("Could not fetch Cypress node attributes")
                << TErrorAttribute("path", path);
            LOG_WARNING(error);
            return nullptr;
        }
        return ConvertToNode(rspOrError.Value());
    };

    auto tables =  BuildTablesList(GetAbsolutePath(path), fetchNode, recursive);
    LOG_INFO("Tables found in %Qlv: %v", path, tables.size());
    return tables;
}

std::unique_ptr<TTableObject> TStorage::GetTableAttributes(
    NApi::NNative::IClientPtr client,
    ITransactionPtr transaction,
    const TRichYPath& path,
    EPermission permission,
    bool suppressAccessTracking)
{
    auto userObject = std::make_unique<TTableObject>();
    userObject->Path = path;

    // LOG_INFO("Requesting object attributes");

    {
        GetUserObjectBasicAttributes(
            client,
            TMutableRange<TUserObject>(userObject.get(), 1),
            transaction ? transaction->GetId() : NullTransactionId,
            Logger,
            permission,
            suppressAccessTracking);

        if (userObject->Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Invalid object type")
                << TErrorAttribute("path", path)
                << TErrorAttribute("expected", EObjectType::Table)
                << TErrorAttribute("actual", userObject->Type);
        }
    }

    LOG_INFO("Requesting table attributes");

    {
        auto objectIdPath = FromObjectId(userObject->ObjectId);

        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
        TObjectServiceProxy proxy(channel);

        auto req = TYPathProxy::Get(objectIdPath + "/@");
        SetTransactionId(req, transaction);
        SetSuppressAccessTracking(req, suppressAccessTracking);
        NInterop::TStringList attributeKeys {
            "chunk_count",
            "dynamic",
            "schema",
        };
        NYT::ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

        auto rspOrError = WaitFor(proxy.Execute(req));
        if (!rspOrError.IsOK()) {
            THROW_ERROR(rspOrError).Wrap("Error getting table schema")
                << TErrorAttribute("path", path);
        }

        const auto& rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        userObject->ChunkCount = attributes->Get<int>("chunk_count");
        userObject->Dynamic = attributes->Get<bool>("dynamic");
        userObject->Schema = attributes->Get<TTableSchema>("schema");
    }

    return userObject;
}

IMapNodePtr TStorage::GetAttributes(
    const NNative::IClientPtr& client,
    const TString& path,
    const std::vector<TString>& attributes)
{
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Follower;
    options.SuppressAccessTracking = true;
    options.Attributes = attributes;

    const auto responseYson = WaitFor(
        client->GetNode(GetAttributesRootPath(path), options))
        .ValueOrThrow();

    return ConvertToNode(responseYson)->AsMap();
}

INodePtr TStorage::GetAttribute(
    const NNative::IClientPtr& client,
    const TString& path,
    const TString& attribute)
{
    return GetAttributes(client, path, {attribute})->GetChild(attribute);
}

NInterop::TTablePtr TStorage::DoGetTable(
    const NNative::IClientPtr& client,
    const TString& name)
{
    auto path = TRichYPath::Parse(name);

    // TODO
    ITransactionPtr transaction;

    // do not modify last-access timestamp
    const bool suppressAccessTracking = true;

    auto userObject = GetTableAttributes(
        client,
        transaction,
        path,
        EPermission::Read,
        suppressAccessTracking);

    return CreateTableSchema(
        path.GetPath(),
        userObject->Schema);
}

NInterop::TTablePartList TStorage::DoGetTablesParts(
    const NNative::IClientPtr& client,
    const std::vector<TString>& names,
    const NInterop::IRangeFilterPtr& rangeFilter,
    size_t maxTableParts)
{
    return PartitionTables(
        client,
        names,
        rangeFilter,
        maxTableParts);
}

NInterop::TTableReaderList TStorage::DoCreateTableReaders(
    const NNative::IClientPtr& client,
    const TString& jobSpec,
    const NInterop::TStringList& columns,
    const NInterop::TSystemColumns& systemColumns,
    size_t maxStreamCount,
    const NInterop::TTableReaderOptions& options)
{
    return CreateJobTableReaders(
        client,
        jobSpec,
        columns,
        systemColumns,
        ScanThrottler,
        maxStreamCount,
        options);
}

NInterop::ITableReaderPtr TStorage::DoCreateTableReader(
    const NNative::IClientPtr& client,
    const TString& name,
    const NInterop::TTableReaderOptions& options)
{
    LOG_INFO("Create reader for table %Qv", name);

    auto path = TRichYPath::Parse(name);

    ITransactionPtr transaction;

    auto tableObject = GetTableAttributes(
        client,
        transaction,
        path,
        EPermission::Read,
        /*suppressAccessTracking=*/ true);

    NApi::TTableReaderOptions readerOptions;
    readerOptions.Unordered = options.Unordered;

    auto chunkReader = NClickHouse::CreateSchemafulTableReader(
        client,
        path,
        tableObject->Schema,
        readerOptions);

    // TODO(max42): rename?
    auto readerTable = NClickHouse::CreateTableSchema(name, tableObject->Schema);
    return NClickHouse::CreateTableReader(readerTable, std::move(chunkReader));
}

TString TStorage::DoReadFile(
    const NNative::IClientPtr& client,
    const TString& name)
{
    LOG_INFO("Requesting file %Qv", name);
    TString fileContent;

    {
        TFileReaderOptions options;

        // do not modify last-access timestamp
        options.SuppressAccessTracking = true;

        auto result = WaitFor(client->CreateFileReader(name, options))
            .ValueOrThrow();

        auto adapter = CreateSyncAdapter(CreateCopyingAdapter(result));
        fileContent = adapter->ReadAll();
    }

    return fileContent;
}

NInterop::IDocumentPtr TStorage::DoReadDocument(
    const NNative::IClientPtr& client,
    const TString& name)
{

    LOG_INFO("Requesting document %Qv", name);

    // TODO: Workaround, remove later
    const auto type = GetAttribute(client, name, "type");
    const auto& typeName = type->AsString()->GetValue();
    if (typeName != NODE_TYPE_DOCUMENT) {
        THROW_ERROR_EXCEPTION("Unexpected type of object in storage")
            << TErrorAttribute("expected", "document")
            << TErrorAttribute("found", typeName);
    }

    TGetNodeOptions options;
    options.SuppressAccessTracking = true;
    options.ReadFrom = EMasterChannelKind::Follower;

    const auto rspOrError = WaitFor(client->GetNode(name, options));
    const auto value = rspOrError.ValueOrThrow();
    const auto node = ConvertToNode(value);

    return CreateDocument(std::move(node));
}

bool TStorage::DoExists(
    const NNative::IClientPtr& client,
    const TString& name)
{
    TNodeExistsOptions options;
    options.ReadFrom = EMasterChannelKind::Follower;
    options.SuppressAccessTracking = true;

    return WaitFor(client->NodeExists(name, options))
        .ValueOrThrow();
}

NInterop::TObjectList TStorage::DoListObjects(
    const NNative::IClientPtr& client,
    const TString& name)
{
    LOG_INFO("List objects in %Qv", name);

    TListNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Follower;
    options.SuppressAccessTracking = true;

    options.Attributes = GetBasicAttributesKeys();
    options.Attributes->push_back("key");

    auto responseYson = WaitFor(client->ListNode(name, options)).ValueOrThrow();
    auto children = ConvertToNode(responseYson)->AsList()->GetChildren();

    NInterop::TObjectList list;

    for (const auto& node : children) {
        const auto& attributes = node->Attributes();

        NInterop::TObjectListItem item;
        item.Name = attributes.Get<TString>("key");
        item.Attributes = CreateBasicAttributes(attributes);

        list.push_back(std::move(item));
    }

    return list;
}

NInterop::TObjectAttributes TStorage::DoGetObjectAttributes(
    const NNative::IClientPtr& client,
    const TString& name)
{
    LOG_INFO("Requesting attributes of %Qv", name);

    const auto attributesMap = GetAttributes(client, name, GetBasicAttributesKeys());
    return CreateBasicAttributes(*attributesMap);
}

TMaybe<NInterop::TRevision> TStorage::DoGetObjectRevision(
    const NNative::IClientPtr& client,
    const TString& name,
    const bool throughCache)
{
    TGetNodeOptions options;
    {
        options.Attributes = {
            "revision",
        };

        // do not modify last-access timestamp
        options.SuppressAccessTracking = true;

        options.ReadFrom = throughCache
                            ? EMasterChannelKind::Cache
                            : EMasterChannelKind::Follower;
    }

    auto rspOrError = WaitFor(
        client->GetNode(GetAttributePath(name, "revision"),
        options));

    if (!rspOrError.IsOK()) {
        if (rspOrError.GetCode() == NYTree::EErrorCode::ResolveError) {
            // node not found
            return Nothing();
        }

        auto error = rspOrError.Wrap("Cannot fetch Cypress node attributes")
            << TErrorAttribute("path", name);
        LOG_ERROR(error);
        error.ThrowOnError();
    }

    const auto node = ConvertToNode(rspOrError.Value());
    return node->AsInt64()->GetValue();
}

////////////////////////////////////////////////////////////////////////////////

class TStorageSyncWrapper
    : public NInterop::IStorage
{
private:
    const TStoragePtr Impl;

public:
    TStorageSyncWrapper(TStoragePtr impl)
        : Impl(std::move(impl))
    {}

    const NInterop::IPathService* PathService() override
    {
        return GetPathService();
    }

    NInterop::IAuthorizationTokenService* AuthTokenService() override
    {
        return GetAuthTokenService();
    }

    NInterop::TTableList ListTables(
        const NInterop::IAuthorizationToken& token,
        const TString& path,
        bool recursive) override
    {
        return WaitFor(Impl->ListTables(token, path, recursive))
            .ValueOrThrow();
    }

    NInterop::TTablePtr GetTable(
        const NInterop::IAuthorizationToken& token,
        const TString& name) override
    {
        return WaitFor(Impl->GetTable(token, name))
            .ValueOrThrow();
    }

    // TODO: spec presentation in interop
    NInterop::TTableList GetTables(
        const TString& jobSpec) override
    {
        auto readJobSpec = LoadReadJobSpec(jobSpec);
        return readJobSpec.GetTables();
    }

    NInterop::TTablePartList GetTableParts(
        const NInterop::IAuthorizationToken& token,
        const TString& name,
        const NInterop::IRangeFilterPtr& rangeFilter,
        size_t maxTableParts) override
    {
        return WaitFor(Impl->GetTablesParts(token, {name}, rangeFilter, maxTableParts))
            .ValueOrThrow();
    }

    NInterop::TTablePartList ConcatenateAndGetTableParts(
        const NInterop::IAuthorizationToken& token,
        const std::vector<TString> names,
        const NInterop::IRangeFilterPtr& rangeFilter = nullptr,
        size_t maxTableParts = 1) override
    {
        return WaitFor(Impl->GetTablesParts(token, names, rangeFilter, maxTableParts))
            .ValueOrThrow();
    }

    NInterop::TTableReaderList CreateTableReaders(
        const NInterop::IAuthorizationToken& token,
        const TString& jobSpec,
        const NInterop::TStringList& columns,
        const NInterop::TSystemColumns& systemColumns,
        size_t maxStreamCount,
        const NInterop::TTableReaderOptions& options) override
    {
        return WaitFor(Impl->CreateTableReaders(
                token, jobSpec, columns, systemColumns, maxStreamCount, options))
            .ValueOrThrow();
    }

    NInterop::ITableReaderPtr CreateTableReader(
        const NInterop::IAuthorizationToken& token,
        const TString& name,
        const NInterop::TTableReaderOptions& options) override
    {
        return WaitFor(Impl->CreateTableReader(token, name, options))
            .ValueOrThrow();
    }

    TString ReadFile(
        const NInterop::IAuthorizationToken& token,
        const TString& name) override
    {
        return WaitFor(Impl->ReadFile(token, name))
            .ValueOrThrow();
    }

    NInterop::IDocumentPtr ReadDocument(
        const NInterop::IAuthorizationToken& token,
        const TString& name) override
    {
        return WaitFor(Impl->ReadDocument(token, name))
            .ValueOrThrow();
    }

    bool Exists(
        const NInterop::IAuthorizationToken& token,
        const TString& name) override
    {
        return WaitFor(Impl->Exists(token, name))
            .ValueOrThrow();
    }

    NInterop::TObjectList ListObjects(
        const NInterop::IAuthorizationToken& token,
        const TString& name) override
    {
        return WaitFor(Impl->ListObjects(token, name))
            .ValueOrThrow();
    }

    NInterop::TObjectAttributes GetObjectAttributes(
        const NInterop::IAuthorizationToken& token,
        const TString& name) override
    {
        return WaitFor(Impl->GetObjectAttributes(token, name))
            .ValueOrThrow();
    }

    TMaybe<NInterop::TRevision> GetObjectRevision(
        const NInterop::IAuthorizationToken& token,
        const TString& name,
        const bool throughCache) override
    {
        return WaitFor(Impl->GetObjectRevision(token, name, throughCache))
            .ValueOrThrow();
    }
};

////////////////////////////////////////////////////////////////////////////////

NInterop::IStoragePtr CreateStorage(
    NNative::IConnectionPtr connection,
    INativeClientCachePtr clientCache,
    IThroughputThrottlerPtr scanThrottler)
{
    auto storage = New<TStorage>(
        std::move(connection),
        std::move(clientCache),
        std::move(scanThrottler));

    return std::make_shared<TStorageSyncWrapper>(std::move(storage));
}

}   // namespace NClickHouse
}   // namespace NYT
