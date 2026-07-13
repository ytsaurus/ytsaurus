#include <yt/yt/server/lib/nbd/chunk/chunk_block_device.h>
#include <yt/yt/server/lib/nbd/chunk/config.h>
#include <yt/yt/server/lib/nbd/config.h>
#include <yt/yt/server/lib/nbd/server.h>

#include <library/cpp/yt/threading/event_count.h>

#include <yt/yt/library/signals/signal_registry.h>

#include <yt/yt/server/lib/nbd/dynamic_table/config.h>
#include <yt/yt/server/lib/nbd/dynamic_table/dynamic_table_block_device.h>

#include <yt/yt/server/lib/nbd/image/config.h>
#include <yt/yt/server/lib/nbd/image/image_block_device.h>
#include <yt/yt/server/lib/nbd/image/image_reader.h>
#include <yt/yt/server/lib/nbd/image/random_access_file_reader.h>

#include <yt/yt/server/lib/nbd/memory/config.h>
#include <yt/yt/server/lib/nbd/memory/memory_block_device.h>

#include <yt/yt/server/lib/nbd/journal/config.h>
#include <yt/yt/server/lib/nbd/journal/journal_block_device.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/polymorphic_yson_struct.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_proxy.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_builder.h>

#include <util/stream/mem.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/library/program/helpers.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>

#include <library/cpp/yt/system/env.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NNbd {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NFileClient;
using namespace NObjectClient;
using namespace NSignals;
using namespace NThreading;
using namespace NYPath;
using namespace NYTree;

using namespace NNbd::NChunk;
using namespace NNbd::NMemory;
using namespace NNbd::NDynamicTable;
using namespace NNbd::NImage;

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IClientPtr CreateNativeClient(
    const std::string& clusterUrl,
    const INodePtr& connectionPatch,
    const IInvokerPtr& invoker)
{
    NApi::NNative::TClientOptions clientOptions;
    static_cast<NApi::TClientOptions&>(clientOptions) = NApi::GetClientOptionsFromEnvStatic();

    // Bootstrap over RPC to fetch the native cluster connection config.
    auto rpcConnectionConfig = NApi::NRpcProxy::TConnectionConfig::CreateFromClusterUrl(clusterUrl);
    NApi::TConnectionOptions rpcConnectionOptions;
    rpcConnectionOptions.ConnectionInvoker = invoker;
    auto rpcConnection = NApi::NRpcProxy::CreateConnection(rpcConnectionConfig, std::move(rpcConnectionOptions));
    auto rpcClient = rpcConnection->CreateClient(clientOptions);

    auto clusterConnectionYson = WaitFor(rpcClient->GetNode("//sys/@cluster_connection"))
        .ValueOrThrow();
    auto connectionConfig = ConvertTo<NApi::NNative::TConnectionCompoundConfigPtr>(clusterConnectionYson);

    // Apply the caller-supplied patch over the dynamic part of the fetched cluster
    // connection. Handy to enable the client block cache (which the fetched config
    // leaves at zero), e.g. {block_cache={compressed_data={capacity=...}}}.
    if (connectionPatch) {
        connectionConfig->Dynamic = UpdateYsonStruct(connectionConfig->Dynamic, connectionPatch);
    }

    // Route Cypress requests straight to the masters (which have direct addresses)
    // instead of through cypress proxies, which would require TVM credentials.
    connectionConfig->Static->CypressProxy.Reset();

    // Now build the real native client.
    NApi::NNative::TConnectionOptions connectionOptions;
    connectionOptions.ConnectionInvoker = invoker;

    auto connection = NApi::NNative::CreateConnection(connectionConfig, std::move(connectionOptions));
    connection->GetNodeDirectorySynchronizer()->Start();
    connection->GetClusterDirectorySynchronizer()->Start();

    return connection->CreateNativeClient(clientOptions);
}

////////////////////////////////////////////////////////////////////////////////

//! The library's file-system block device config carries no path (its callers
//! build the image reader themselves); add one for this server.
DECLARE_REFCOUNTED_STRUCT(TCypressFileDeviceConfig)

struct TCypressFileDeviceConfig
    : public TImageBlockDeviceConfig
{
    TYPath Path;

    REGISTER_YSON_STRUCT(TCypressFileDeviceConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path);
    }
};

DEFINE_REFCOUNTED_TYPE(TCypressFileDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

//! The library's chunk block device config carries no data node address (its callers
//! already have a channel); add one for this server.
DECLARE_REFCOUNTED_STRUCT(TCypressChunkDeviceConfig)

struct TCypressChunkDeviceConfig
    : public TChunkBlockDeviceConfig
{
    //! Address of the data node NBD service (e.g. "host:port").
    std::string DataNodeNbdServiceAddress;

    //! Optional medium name (e.g. "ssd_nbd"). When set, overrides medium_index
    //! by resolving the name to an index via the cluster's medium directory.
    std::optional<std::string> Medium;

    REGISTER_YSON_STRUCT(TCypressChunkDeviceConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("data_node_nbd_service_address", &TThis::DataNodeNbdServiceAddress);
        registrar.Parameter("medium", &TThis::Medium)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TCypressChunkDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TJournalDeviceConfig)

struct TJournalDeviceConfig
    : public NJournal::TJournalBlockDeviceConfig
    , public NJournal::TJournalBlockDeviceOptions
{
    REGISTER_YSON_STRUCT(TJournalDeviceConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TJournalDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

//! Builds a read-write device backed by a chunk on a data node.
//! Creates a TCP channel to the data node's NBD service and wraps it in a
//! chunk block device (optionally with a write-back page cache).
IBlockDevicePtr CreateCypressChunkDevice(
    const std::string& exportId,
    const TCypressChunkDeviceConfigPtr& config,
    const NApi::NNative::IClientPtr& client,
    const IInvokerPtr& invoker,
    const NLogging::TLogger& logger)
{
    // Use the cluster client's authenticated channel factory when available
    // (requires YT_PROXY + native_authentication_manager with TVM), so that
    // the data node's DataNodeNbdService accepts our OpenSession RPC.
    // Fall back to a raw bus channel for environments without authentication.
    NRpc::IChannelPtr channel;
    if (client) {
        channel = client->GetChannelFactory()->CreateChannel(config->DataNodeNbdServiceAddress);

        // Resolve medium name to index via the cluster's medium directory.
        // The medium directory is populated asynchronously by a synchronizer,
        // so we must wait for the first sync to complete before looking up
        // the medium name.
        if (config->Medium) {
            const auto& nativeConnection = client->GetNativeConnection();
            WaitFor(nativeConnection->GetMediumDirectorySynchronizer()->NextSync())
                .ThrowOnError();
            const auto& mediumDirectory = nativeConnection->GetMediumDirectory();
            auto descriptor = mediumDirectory->GetByNameOrThrow(*config->Medium);
            config->MediumIndex = descriptor->GetIndex();
        }
    } else {
        auto busClient = NBus::NTcp::CreateBusClient(
            NBus::NTcp::TBusClientConfig::CreateTcp(config->DataNodeNbdServiceAddress));
        channel = NRpc::NBus::CreateBusChannel(std::move(busClient));

        if (config->Medium) {
            THROW_ERROR_EXCEPTION("Medium name %Qv requires a cluster client (set YT_PROXY)",
                *config->Medium);
        }
    }

    return CreateChunkBlockDevice(
        exportId,
        config,
        NConcurrency::GetUnlimitedThrottler(),
        NConcurrency::GetUnlimitedThrottler(),
        invoker,
        std::move(channel),
        /*sessionId*/ NChunkClient::TSessionId(),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

//! Builds a read-only device serving the bytes of a Cypress #path: validate the
//! file's filesystem, fetch its chunk specs, wrap them in a random-access reader,
//! then a Cypress-file image reader, then the file-system block device. Needs a
//! native client for chunk access.
IBlockDevicePtr CreateCypressFileDevice(
    const std::string& exportId,
    const TCypressFileDeviceConfigPtr& config,
    const NApi::NNative::IClientPtr& client,
    const IInvokerPtr& invoker,
    const NLogging::TLogger& logger)
{
    TUserObject userObject{TRichYPath(config->Path)};
    GetUserObjectBasicAttributes(
        client,
        {&userObject},
        NullTransactionId,
        logger,
        EPermission::Read);
    if (userObject.Type != EObjectType::File) {
        THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, but got %Qlv",
            config->Path,
            EObjectType::File,
            userObject.Type);
    }

    // Fetch the file's chunk specs.
    auto proxy = CreateObjectServiceReadProxy(
        client,
        NApi::EMasterChannelKind::Follower,
        userObject.ExternalCellTag);
    auto batchReq = proxy.ExecuteBatchWithRetries(client->GetNativeConnection()->GetConfig()->ChunkFetchRetries);

    auto req = TFileYPathProxy::Fetch(userObject.GetObjectIdPath());
    AddCellTagToSyncWith(req, userObject.ObjectId);
    ToProto(req->mutable_ranges(), std::vector<TLegacyReadRange>{TLegacyReadRange()});
    req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
    batchReq->AddRequest(req);

    // Also fetch the "filesystem" attribute to validate the image type below.
    auto attributesReq = TYPathProxy::Get(userObject.GetObjectIdPath() + "/@");
    ToProto(attributesReq->mutable_attributes()->mutable_keys(), std::vector<std::string>{"filesystem"});
    AddCellTagToSyncWith(attributesReq, userObject.ObjectId);
    SetTransactionId(attributesReq, NullTransactionId);
    batchReq->AddRequest(attributesReq, "get_attributes");

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    // The image must be a mountable filesystem; refuse anything else early.
    auto attributesRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes")
        .ValueOrThrow();
    auto filesystem = ConvertToAttributes(NYson::TYsonString(attributesRsp->value()))
        ->Get<std::string>("filesystem", /*defaultValue*/ "");
    if (filesystem != "ext4" && filesystem != "squashfs") {
        THROW_ERROR_EXCEPTION("Invalid \"filesystem\" attribute %Qv of file %v: expected \"ext4\" or \"squashfs\"",
            filesystem,
            config->Path);
    }

    auto rspOrError = batchRsp->GetResponse<TFileYPathProxy::TRspFetch>(0);
    const auto& rsp = rspOrError.ValueOrThrow();

    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;
    ProcessFetchResponse(
        client,
        rsp,
        userObject.ExternalCellTag,
        client->GetNativeConnection()->GetNodeDirectory(),
        /*maxChunksPerLocateRequest*/ 10,
        /*rangeIndex*/ std::nullopt,
        logger,
        &chunkSpecs);

    auto fileReader = CreateRandomAccessFileReader(
        std::move(chunkSpecs),
        config->Path,
        New<TChunkReaderHost>(client),
        invoker,
        logger);
    auto imageReader = CreateCypressFileImageReader(
        std::move(fileReader),
        logger);
    return CreateImageBlockDevice(
        exportId,
        config,
        std::move(imageReader),
        invoker,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_POLYMORPHIC_YSON_STRUCT(BlockDeviceConfig, TBlockDeviceConfigBase,
    ((Memory)       (TMemoryBlockDeviceConfig))
    ((DynamicTable) (TDynamicTableBlockDeviceConfig))
    ((File)         (TCypressFileDeviceConfig))
    ((Chunk)        (TCypressChunkDeviceConfig))
    ((Journal)      (TJournalDeviceConfig))
);

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateJournalDevice(
    const std::string& deviceId,
    const TJournalDeviceConfigPtr& config,
    TTransactionId transactionId,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger)
{
    // The config is both the device config and its store options (see TJournalDeviceConfig).
    return NJournal::CreateJournalBlockDevice(
        deviceId,
        config,
        config,
        transactionId,
        NChunkClient::NullChunkListId,
        client,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDeviceFactory)

//! Builds block devices of any backend type. Journal devices stage their chunks under a per-device
//! master transaction that must outlive the device; those handles are retained here for the whole
//! run (they self-ping). Shared by the startup loop and the HTTP add-device handler, so it is
//! thread-safe.
class TDeviceFactory
    : public TRefCounted
{
public:
    TDeviceFactory(
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
        : Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , Logger(std::move(logger))
    { }

    IBlockDevicePtr CreateDevice(const std::string& deviceId, const TBlockDeviceConfig& config)
    {
        auto getClientOrThrow = [&] {
            if (!Client_) {
                THROW_ERROR_EXCEPTION("Device %Qv of type %Qlv requires a client, but none is configured",
                    deviceId,
                    config.GetCurrentType());
            }
            return Client_;
        };

        switch (config.GetCurrentType()) {
            case EBlockDeviceConfigType::Memory:
                return CreateMemoryBlockDevice(config.TryGetConcrete<TMemoryBlockDeviceConfig>());
            case EBlockDeviceConfigType::DynamicTable:
                return CreateDynamicTableBlockDevice(
                    deviceId,
                    config.TryGetConcrete<TDynamicTableBlockDeviceConfig>(),
                    getClientOrThrow(),
                    Logger);
            case EBlockDeviceConfigType::File:
                return CreateCypressFileDevice(
                    deviceId,
                    config.TryGetConcrete<TCypressFileDeviceConfig>(),
                    getClientOrThrow(),
                    Invoker_,
                    Logger);
            case EBlockDeviceConfigType::Chunk:
                return CreateCypressChunkDevice(
                    deviceId,
                    config.TryGetConcrete<TCypressChunkDeviceConfig>(),
                    Client_,
                    Invoker_,
                    Logger);
            case EBlockDeviceConfigType::Journal:
                return CreateJournalDevice(
                    deviceId,
                    config.TryGetConcrete<TJournalDeviceConfig>(),
                    StartDeviceTransaction(getClientOrThrow()),
                    getClientOrThrow(),
                    Logger);
        }
        YT_ABORT();
    }

private:
    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, TransactionsLock_);
    // Retained for the whole run so the staged journal chunks stay pinned (the transaction self-pings).
    std::vector<NApi::ITransactionPtr> Transactions_;

    TTransactionId StartDeviceTransaction(const NApi::NNative::IClientPtr& client)
    {
        auto transaction = WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Master))
            .ValueOrThrow();
        auto transactionId = transaction->GetId();
        {
            auto guard = Guard(TransactionsLock_);
            Transactions_.push_back(std::move(transaction));
        }
        return transactionId;
    }
};

DEFINE_REFCOUNTED_TYPE(TDeviceFactory)

////////////////////////////////////////////////////////////////////////////////

//! Parses a request body into a node. The format follows the Content-Type header: JSON if it
//! mentions "json", otherwise (the default) YSON.
INodePtr ParseRequestBody(const NHttp::IRequestPtr& request)
{
    auto body = request->ReadAll();
    if (auto contentType = request->GetHeaders()->Find("Content-Type"_sb);
        contentType && TStringBuf(*contentType).Contains("json"_sb))
    {
        TMemoryInput input(body.Begin(), body.Size());
        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        builder->BeginTree();
        NJson::ParseJson(&input, builder.get());
        return builder->EndTree();
    }
    return ConvertToNode(NYson::TYsonString(body.ToStringBuf()));
}

//! Writes #yson as the response body. The format follows the Accept header: JSON if it mentions
//! "json", otherwise (the default) text YSON.
void WriteResponseBody(
    const NHttp::IRequestPtr& request,
    const NHttp::IResponseWriterPtr& response,
    const NYson::TYsonString& yson)
{
    response->SetStatus(NHttp::EStatusCode::OK);
    if (auto accept = request->GetHeaders()->Find("Accept"_sb);
        accept && TStringBuf(*accept).Contains("json"_sb))
    {
        NHttp::ReplyJson(response, [&] (NYson::IYsonConsumer* consumer) {
            Serialize(yson, consumer);
        });
    } else {
        response->GetHeaders()->Set("Content-Type", "application/x-yt-yson-text");
        auto text = ConvertToYsonString(yson, NYson::EYsonFormat::Text);
        WaitFor(response->WriteBody(TSharedRef::FromString(text.ToString())))
            .ThrowOnError();
    }
}

//! Replies with a bare status code and no body.
void Reply(const NHttp::IResponseWriterPtr& response, NHttp::EStatusCode statusCode)
{
    response->SetStatus(statusCode);
    WaitFor(response->Close())
        .ThrowOnError();
}

//! Gets #path from #orchidService and writes it as the response body, mapping a missing path to 404.
void ReplyOrchid(
    const NHttp::IRequestPtr& request,
    const NHttp::IResponseWriterPtr& response,
    const IYPathServicePtr& orchidService,
    const NYPath::TYPath& path)
{
    auto ysonOrError = WaitFor(AsyncYPathGet(orchidService, path));
    if (ysonOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        Reply(response, NHttp::EStatusCode::NotFound);
        return;
    }
    WriteResponseBody(request, response, ysonOrError.ValueOrThrow());
}

////////////////////////////////////////////////////////////////////////////////

//! Serves the NBD server orchid at "/status"; the path after the mount is a ypath into the orchid,
//! so both /status and /status/devices/<name> work. The response honors the Accept header.
class TStatusHttpHandler
    : public NHttp::IHttpHandler
{
public:
    explicit TStatusHttpHandler(IYPathServicePtr orchidService)
        : OrchidService_(std::move(orchidService))
    { }

    void HandleRequest(
        const NHttp::IRequestPtr& request,
        const NHttp::IResponseWriterPtr& response) override
    {
        auto path = request->GetUrl().Path;
        path.SkipPrefix("/status"_sb);
        ReplyOrchid(request, response, OrchidService_, NYPath::TYPath(path));
    }

private:
    const IYPathServicePtr OrchidService_;
};

////////////////////////////////////////////////////////////////////////////////

//! A REST resource for devices, rooted at "/devices":
//!   GET    /devices          -- list the devices (from the orchid);
//!   GET    /devices/<name>   -- a single device's status;
//!   PUT    /devices/<name>   -- create and register the device (body = device config YSON);
//!   DELETE /devices/<name>   -- unregister and finalize the device.
class TDeviceHttpHandler
    : public NHttp::IHttpHandler
{
public:
    TDeviceHttpHandler(
        INbdServerPtr nbdServer,
        IYPathServicePtr orchidService,
        TDeviceFactoryPtr deviceFactory,
        NLogging::TLogger logger)
        : NbdServer_(std::move(nbdServer))
        , OrchidService_(std::move(orchidService))
        , DeviceFactory_(std::move(deviceFactory))
        , Logger(std::move(logger))
    { }

    void HandleRequest(
        const NHttp::IRequestPtr& request,
        const NHttp::IResponseWriterPtr& response) override
    {
        // The URL path ("/devices" or "/devices/<name>") is also the path of the resource within
        // the orchid, so reads are just a get on it.
        auto path = request->GetUrl().Path;
        auto method = request->GetMethod();

        if (method == NHttp::EMethod::Get) {
            ReplyOrchid(request, response, OrchidService_, NYPath::TYPath(path));
            return;
        }

        // Mutations address a single device: /devices/<name>.
        constexpr auto prefix = "/devices/"_sb;
        auto tail = path;
        if (!tail.SkipPrefix(prefix) || tail.empty()) {
            THROW_ERROR_EXCEPTION("Method %Qlv requires a device name (%v<name>)",
                method,
                prefix);
        }
        auto name = std::string(tail);

        switch (method) {
            case NHttp::EMethod::Put:
                AddDevice(name, request, response);
                break;
            case NHttp::EMethod::Delete:
                RemoveDevice(name, response);
                break;
            default:
                Reply(response, NHttp::EStatusCode::MethodNotAllowed);
                break;
        }
    }

private:
    const INbdServerPtr NbdServer_;
    const IYPathServicePtr OrchidService_;
    const TDeviceFactoryPtr DeviceFactory_;
    const NLogging::TLogger Logger;

    void AddDevice(
        const std::string& name,
        const NHttp::IRequestPtr& request,
        const NHttp::IResponseWriterPtr& response)
    {
        auto deviceConfig = ConvertTo<TBlockDeviceConfig>(ParseRequestBody(request));
        auto device = DeviceFactory_->CreateDevice(name, deviceConfig);
        WaitFor(device->Initialize())
            .ThrowOnError();

        try {
            // RegisterDevice rejects a duplicate name atomically, so there is no point (and it would
            // be racy) to check for one beforehand.
            NbdServer_->RegisterDevice(name, device);
        } catch (const std::exception&) {
            YT_UNUSED_FUTURE(device->Finalize());
            Reply(response, NHttp::EStatusCode::Conflict);
            return;
        }

        YT_LOG_INFO("Device added via HTTP (Name: %v)", name);
        Reply(response, NHttp::EStatusCode::Created);
    }

    void RemoveDevice(
        const std::string& name,
        const NHttp::IResponseWriterPtr& response)
    {
        auto device = NbdServer_->TryUnregisterDevice(name);
        if (!device) {
            Reply(response, NHttp::EStatusCode::NotFound);
            return;
        }
        WaitFor(device->Finalize())
            .ThrowOnError();

        YT_LOG_INFO("Device removed via HTTP (Name: %v)", name);
        Reply(response, NHttp::EStatusCode::NoContent);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Registers #handler both for the exact #path and for the subtree beneath it, so e.g. mounting
//! "/status" serves both /status and /status/<subpath>.
void AddHttpHandler(const NHttp::IServerPtr& server, const std::string& path, NHttp::IHttpHandlerPtr handler)
{
    server->AddHandler(path, handler);
    server->AddHandler(path + "/", std::move(handler));
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TProgramConfig)

//! Inherits TSingletonsConfig so the config file can carry singleton sections
//! (native_authentication_manager for TVM, yp_service_discovery, address_resolver, ...)
//! alongside the server's own fields.
struct TProgramConfig
    : public TSingletonsConfig
{
    TNbdServerConfigPtr NbdServer;
    THashMap<std::string, TBlockDeviceConfig> Devices;
    //! Optional patch applied over the dynamic part of the fetched //sys/@cluster_connection.
    INodePtr ConnectionPatch;
    int PollerThreadCount;
    int NbdServerThreadCount;

    REGISTER_YSON_STRUCT(TProgramConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("nbd_server", &TThis::NbdServer)
            .DefaultNew();
        registrar.Parameter("connection_patch", &TThis::ConnectionPatch)
            .Default();
        registrar.Parameter("devices", &TThis::Devices)
            .Default();
        registrar.Parameter("poller_thread_count", &TThis::PollerThreadCount)
            .Default(1)
            .GreaterThan(0);
        registrar.Parameter("nbd_server_thread_count", &TThis::NbdServerThreadCount)
            .Default(1)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TProgramConfig)

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public virtual NYT::TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<TProgramConfig>
{
public:
    TProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    { }

private:
    void DoRun() override
    {
        auto config = GetConfig();

        ConfigureSingletons(config);

        auto poller = CreateThreadPoolPoller(config->PollerThreadCount, "Poller");
        auto threadPool = CreateThreadPool(config->NbdServerThreadCount, "Nbd");

        auto nbdServer = CreateNbdServer(
            config->NbdServer,
            poller,
            threadPool->GetInvoker());

        // Build a single shared client only if a cluster is given via YT_PROXY.
        NApi::NNative::IClientPtr client;
        if (auto ytProxy = TryGetEnvValue("YT_PROXY")) {
            client = CreateNativeClient(*ytProxy, config->ConnectionPatch, threadPool->GetInvoker());
        }

        auto deviceFactory = New<TDeviceFactory>(client, threadPool->GetInvoker(), nbdServer->GetLogger());

        for (const auto& [name, deviceConfig] : config->Devices) {
            auto device = deviceFactory->CreateDevice(name, deviceConfig);
            WaitFor(device->Initialize())
                .ThrowOnError();
            nbdServer->RegisterDevice(name, std::move(device));
        }

        // Optionally expose an HTTP API.
        NHttp::IServerPtr httpServer;
        if (auto httpPort = config->NbdServer->HttpPort) {
            httpServer = NHttp::CreateServer(*httpPort, poller);

            auto orchidService = nbdServer->GetOrchidService();

            AddHttpHandler(httpServer, "/status", NHttp::CreateErrorWrappingHttpHandler(
                New<TStatusHttpHandler>(orchidService)));
            AddHttpHandler(httpServer, "/devices", NHttp::CreateErrorWrappingHttpHandler(
                New<TDeviceHttpHandler>(
                    nbdServer,
                    orchidService,
                    deviceFactory,
                    nbdServer->GetLogger())));

            httpServer->Start();
        }

        // Wait for a shutdown signal (SIGTERM or SIGINT), then perform
        // graceful shutdown: deregister each device from the NBD server first
        // (so no new requests are accepted), then finalize it (flush dirty
        // pages, close sessions, release resources).
        TEvent shutdownEvent;

        auto signalHandler = [&shutdownEvent] {
            shutdownEvent.NotifyOne();
        };

        TSignalRegistry::Get()->PushCallback(SIGTERM, signalHandler);
        TSignalRegistry::Get()->PushCallback(SIGINT, signalHandler);

        const auto& Logger = nbdServer->GetLogger();
        YT_LOG_INFO("NBD server started, waiting for shutdown signal");

        shutdownEvent.Wait();

        YT_LOG_INFO("Shutdown signal received, deregistering and finalizing devices");

        for (const auto& [name, deviceConfig] : config->Devices) {
            // Deregister the device first so the NBD server stops accepting
            // new requests for it, then finalize it (flush, close, release).
            if (auto device = nbdServer->TryUnregisterDevice(name)) {
                YT_LOG_INFO("Deregistered device (Device: %v)", name);
                auto finalizeResult = WaitFor(device->Finalize());
                if (!finalizeResult.IsOK()) {
                    YT_LOG_ERROR(finalizeResult, "Failed to finalize device (Device: %v)", name);
                } else {
                    YT_LOG_INFO("Finalized device (Device: %v)", name);
                }
            }
        }

        YT_LOG_INFO("NBD server stopped");
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

int main(int argc, const char* argv[])
{
    return NYT::NNbd::TProgram().Run(argc, argv);
}
