#include "query_context.h"

#include "private.h"

#include "attributes_helpers.h"
#include "chunk_reader.h"
#include "convert_row.h"
#include "document.h"
#include "job_input.h"
#include "path.h"
#include "read_job.h"
#include "read_job_spec.h"
#include "schemaful_table_reader.h"
#include "table_reader.h"
#include "table_schema.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client_cache.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>
#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/api/native/transaction.h>

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
#include <yt/core/misc/optional.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/yson/string.h>
#include <yt/core/ytree/convert.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/string/join.h>

#include <stack>
#include <vector>

#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace DB;

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
std::vector<TTablePtr> BuildTablesList(
    const TString& rootPath,
    T&& fetchNode,
    bool recursive = false)
{
    const NLogging::TLogger& Logger = ServerLogger;

    std::vector<TTablePtr> tables;

    YT_LOG_DEBUG("Start traverse from %Qlv", rootPath);

    std::stack<INodePtr> queue;
    if (auto rootNode = fetchNode(rootPath)) {
        YT_LOG_DEBUG("Add root node %Qlv", rootPath);
        queue.push(rootNode);
    }

    while (!queue.empty()) {
        auto node = queue.top();
        queue.pop();

        const auto& attrs = node->Attributes();
        auto type = attrs.Get<TString>("type");
        auto path = attrs.Get<TString>("path");

        YT_LOG_DEBUG("Current node: %Qlv, type: %Qlv", path, type);

        if (type == NODE_TYPE_TABLE) {
            auto table = std::make_shared<TTable>(path);
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

    Sort(tables, [] (const TTablePtr& l, const TTablePtr& r) {
        return l->Name < r->Name;
    });

    return tables;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::vector<TTablePtr> TQueryContext::ListTables(const TString& path, bool recursive)
{
    YT_LOG_INFO("Requesting table list (Path: %v)", path);

    TGetNodeOptions options;
    options.Attributes = {
        "type",
        "path",
    };

    // do not modify last-access timestamp
    options.SuppressAccessTracking = true;

    auto fetchNode = [&] (const TString& path) -> INodePtr {
        auto rspOrError = WaitFor(Client()->GetNode(path, options));
        if (!rspOrError.IsOK()) {
            auto error = rspOrError.Wrap("Could not fetch Cypress node attributes")
                << TErrorAttribute("path", path);
            YT_LOG_WARNING(error);
            return nullptr;
        }
        return ConvertToNode(rspOrError.Value());
    };

    auto tables =  BuildTablesList(GetAbsolutePath(path), fetchNode, recursive);

    YT_LOG_INFO("Table list fetched (Path: %v)", path, tables.size());

    return tables;
}

std::vector<TTablePtr> TQueryContext::GetTables(const TString& jobSpec)
{
    auto readJobSpec = LoadReadJobSpec(jobSpec);
    return readJobSpec.GetTables();
}

std::unique_ptr<TTableObject> TQueryContext::GetTableAttributes(
    NApi::NNative::ITransactionPtr transaction,
    const TRichYPath& path,
    EPermission permission,
    bool suppressAccessTracking)
{
    auto userObject = std::make_unique<TTableObject>();
    userObject->Path = path;

    YT_LOG_INFO("Requesting object attributes (Path: %v)", path);

    {
        GetUserObjectBasicAttributes(
            Client(),
            TMutableRange<TUserObject>(userObject.get(), 1),
            transaction ? transaction->GetId() : NullTransactionId,
            Logger,
            permission,
            suppressAccessTracking,
            true /* readFromCache */);

        if (userObject->Type != NObjectClient::EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Invalid object type")
                << TErrorAttribute("path", path)
                << TErrorAttribute("expected", NObjectClient::EObjectType::Table)
                << TErrorAttribute("actual", userObject->Type);
        }
    }

    YT_LOG_INFO("Requesting table attributes (Path: %v)", path);

    {
        auto objectIdPath = FromObjectId(userObject->ObjectId);

        auto channel = Client()->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
        TObjectServiceProxy proxy(channel);

        auto req = TYPathProxy::Get(objectIdPath + "/@");
        SetTransactionId(req, transaction);
        SetSuppressAccessTracking(req, suppressAccessTracking);
        TStringList attributeKeys {
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

IMapNodePtr TQueryContext::GetAttributes(
    const TString& path,
    const std::vector<TString>& attributes)
{
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Follower;
    options.SuppressAccessTracking = true;
    options.Attributes = attributes;

    const auto responseYson = WaitFor(
        Client()->GetNode(GetAttributesRootPath(path), options))
        .ValueOrThrow();

    return ConvertToNode(responseYson)->AsMap();
}

TTablePartList TQueryContext::ConcatenateAndGetTableParts(
    const std::vector<TString>& names,
    const DB::KeyCondition* keyCondition,
    size_t maxTableParts)
{
    return GetTablesParts(names, keyCondition, maxTableParts);
}

TTablePtr TQueryContext::GetTable(const TString& name)
{
    auto path = TRichYPath::Parse(name);

    // TODO
    NApi::NNative::ITransactionPtr transaction;

    // do not modify last-access timestamp
    const bool suppressAccessTracking = true;

    auto userObject = GetTableAttributes(
        transaction,
        path,
        EPermission::Read,
        suppressAccessTracking);

    return CreateTable(
        path.GetPath(),
        userObject->Schema);
}


TTablePartList TQueryContext::GetTableParts(
    const TString& name,
    const KeyCondition* keyCondition,
    size_t maxTableParts)
{
    return GetTablesParts({name}, keyCondition, maxTableParts);
}

TTablePartList TQueryContext::GetTablesParts(
    const std::vector<TString>& names,
    const KeyCondition* keyCondition,
    size_t maxTableParts)
{
    auto fetchResult = FetchInput(Client(), names, keyCondition);
    auto chunkStripeList = BuildJobs(fetchResult.DataSlices, maxTableParts);
    return SerializeAsTablePartList(
        chunkStripeList,
        fetchResult.NodeDirectory,
        fetchResult.DataSourceDirectory);
}

TTableReaderList TQueryContext::CreateTableReaders(
    const TString& jobSpec,
    const TStringList& columns,
    const TSystemColumns& systemColumns,
    size_t maxStreamCount,
    const TTableReaderOptions& options)
{
    return CreateJobTableReaders(
        Client(),
        jobSpec,
        columns,
        systemColumns,
        maxStreamCount,
        options);
}

const IPathService* TQueryContext::PathService()
{
    return GetPathService();
}

ITableReaderPtr TQueryContext::CreateTableReader(
    const TString& name,
    const TTableReaderOptions& options)
{
    YT_LOG_INFO("Creating table reader (Path: %v)", name);

    auto path = TRichYPath::Parse(name);

    NApi::NNative::ITransactionPtr transaction;

    auto tableObject = GetTableAttributes(
        transaction,
        path,
        EPermission::Read,
        /*suppressAccessTracking=*/ true);

    NApi::TTableReaderOptions readerOptions;
    readerOptions.Unordered = options.Unordered;
    readerOptions.Config = New<NTableClient::TTableReaderConfig>();
    // TODO(max42): YT-10134.
    readerOptions.Config->FetchFromPeers = false;

    auto chunkReader = CreateSchemafulTableReader(
        Client(),
        path,
        tableObject->Schema,
        readerOptions);

    // TODO(max42): rename?
    auto readerTable = CreateTable(name, tableObject->Schema);
    return NClickHouseServer::CreateTableReader(readerTable, std::move(chunkReader));
}

TString TQueryContext::ReadFile(const TString& name)
{
    YT_LOG_INFO("Requesting file (Path: %v)", name);
    TString fileContent;

    {
        TFileReaderOptions options;

        // do not modify last-access timestamp
        options.SuppressAccessTracking = true;

        auto result = WaitFor(Client()->CreateFileReader(name, options))
            .ValueOrThrow();

        auto adapter = CreateSyncAdapter(CreateCopyingAdapter(result));
        fileContent = adapter->ReadAll();
    }

    return fileContent;
}

IDocumentPtr TQueryContext::ReadDocument(const TString& name)
{
    YT_LOG_INFO("Requesting document (Path: %v)", name);

    // TODO: Workaround, remove later
    const auto type = GetAttributes(name, {"type"})->GetChild("type");
    const auto& typeName = type->AsString()->GetValue();
    if (typeName != NODE_TYPE_DOCUMENT) {
        THROW_ERROR_EXCEPTION("Unexpected type of object in storage")
            << TErrorAttribute("expected", "document")
            << TErrorAttribute("found", typeName);
    }

    TGetNodeOptions options;
    options.SuppressAccessTracking = true;
    options.ReadFrom = EMasterChannelKind::Follower;

    const auto rspOrError = WaitFor(Client()->GetNode(name, options));
    const auto value = rspOrError.ValueOrThrow();
    const auto node = ConvertToNode(value);

    return CreateDocument(std::move(node));
}

bool TQueryContext::Exists(const TString& name)
{
    TNodeExistsOptions options;
    options.ReadFrom = EMasterChannelKind::Follower;
    options.SuppressAccessTracking = true;

    return WaitFor(Client()->NodeExists(name, options))
        .ValueOrThrow();
}

TObjectList TQueryContext::ListObjects(const TString& name)
{
    YT_LOG_INFO("List objects (Path: %v)", name);

    TListNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Follower;
    options.SuppressAccessTracking = true;

    options.Attributes = GetBasicAttributesKeys();
    options.Attributes->push_back("key");

    auto responseYson = WaitFor(Client()->ListNode(name, options)).ValueOrThrow();
    auto children = ConvertToNode(responseYson)->AsList()->GetChildren();

    TObjectList list;

    for (const auto& node : children) {
        const auto& attributes = node->Attributes();

        TObjectListItem item;
        item.Name = attributes.Get<TString>("key");
        item.Attributes = CreateBasicAttributes(attributes);

        list.push_back(std::move(item));
    }

    return list;
}

TObjectAttributes TQueryContext::GetObjectAttributes(const TString& name)
{
    YT_LOG_INFO("Requesting attributes (Path: %v)", name);

    const auto attributesMap = GetAttributes(name, GetBasicAttributesKeys());
    return CreateBasicAttributes(*attributesMap);
}

std::optional<TRevision> TQueryContext::GetObjectRevision(const TString& name, const bool throughCache)
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
        Client()->GetNode(GetAttributePath(name, "revision"),
        options));

    if (!rspOrError.IsOK()) {
        if (rspOrError.GetCode() == NYTree::EErrorCode::ResolveError) {
            // node not found
            return std::nullopt;
        }

        auto error = rspOrError.Wrap("Cannot fetch Cypress node attributes")
            << TErrorAttribute("path", name);
        YT_LOG_ERROR(error);
        error.ThrowOnError();
    }

    const auto node = ConvertToNode(rspOrError.Value());
    return node->GetType() == ENodeType::Int64 ? node->AsInt64()->GetValue() : node->AsUint64()->GetValue();
}

TQueryContext::TQueryContext(TBootstrap* bootstrap, TQueryId queryId, const DB::Context& context)
    : Logger(ServerLogger)
      , User(TString(context.getClientInfo().initial_user))
      , QueryId(queryId)
      , QueryKind(static_cast<EQueryKind>(context.getClientInfo().query_kind))
      , Bootstrap_(bootstrap)
      , Host_(Bootstrap_->GetHost())
{
    Logger.AddTag("QueryId: %v", queryId);
    YT_LOG_INFO("Query context created (User: %v, QueryKind: %v)", User, QueryKind);

    const auto& clientInfo = context.getClientInfo();
    YT_LOG_INFO(
        "Query client info (CurrentUser: %v, CurrentQueryId: %v, CurrentAddress: %v, InitialUser: %v, InitialAddress: %v, "
        "InitialQueryId: %v, Interface: %v, ClientHostname: %v, HttpUserAgent: %v)",
        clientInfo.current_user,
        clientInfo.current_query_id,
        clientInfo.current_address.toString(),
        clientInfo.initial_user,
        clientInfo.initial_address.toString(),
        clientInfo.initial_query_id,
        clientInfo.interface == DB::ClientInfo::Interface::TCP
            ? "TCP"
            : clientInfo.interface == DB::ClientInfo::Interface::HTTP
                ? "HTTP"
                : "(n/a)",
        clientInfo.client_hostname,
        clientInfo.http_user_agent);

    Bootstrap_->GetControlInvoker()->Invoke(BIND(
        &TClickHouseHost::AdjustQueryCount,
        Bootstrap_->GetHost(),
        User,
        QueryKind,
        +1 /* delta */));
}

TQueryContext::~TQueryContext()
{
    YT_LOG_INFO("Query context destroyed");

    Bootstrap_->GetControlInvoker()->Invoke(BIND(
        &TClickHouseHost::AdjustQueryCount,
        Bootstrap_->GetHost(),
        User,
        QueryKind,
        -1 /* delta */));
}

NApi::NNative::IClientPtr& TQueryContext::Client()
{
    ClientLock_.AcquireReader();
    auto clientPresent = static_cast<bool>(Client_);
    ClientLock_.ReleaseReader();

    if (!clientPresent) {
        ClientLock_.AcquireWriter();
        Client_ = Bootstrap_->GetClientCache()->GetClient(User);
        ClientLock_.ReleaseWriter();
    }

    return Client_;
}

////////////////////////////////////////////////////////////////////////////////

void SetupHostContext(TBootstrap* bootstrap, DB::Context& context, TQueryId queryId)
{
    if (!queryId) {
        queryId = TQueryId::Create();
    }

    context.getHostContext() = std::make_shared<TQueryContext>(
        bootstrap,
        queryId,
        context);
}

TQueryContext* GetQueryContext(const DB::Context& context)
{
    auto* hostContext = context.getHostContext().get();
    Y_ASSERT(dynamic_cast<TQueryContext*>(hostContext) != nullptr);
    return static_cast<TQueryContext*>(hostContext);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
