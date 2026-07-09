#include <yt/yt/server/lib/nbd/chunk_block_device.h>
#include <yt/yt/server/lib/nbd/config.h>
#include <yt/yt/server/lib/nbd/dynamic_table_block_device.h>
#include <yt/yt/server/lib/nbd/file_system_block_device.h>
#include <yt/yt/server/lib/nbd/image_reader.h>
#include <yt/yt/server/lib/nbd/memory_block_device.h>
#include <yt/yt/server/lib/nbd/random_access_file_reader.h>
#include <yt/yt/server/lib/nbd/server.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>

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

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/polymorphic_yson_struct.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/library/program/helpers.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>

#include <library/cpp/yt/system/env.h>

namespace NYT::NNbd {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NFileClient;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYTree;

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
    : public TFileSystemBlockDeviceConfig
{
    NYPath::TYPath Path;

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
        if (config->Medium) {
            const auto& mediumDirectory = client->GetNativeConnection()->GetMediumDirectory();
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
        THROW_ERROR_EXCEPTION("Invalid type of %Qv: expected %Qlv, but got %Qlv",
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
        THROW_ERROR_EXCEPTION("Invalid \"filesystem\" attribute %Qv of file %Qv: expected \"ext4\" or \"squashfs\"",
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
    return CreateFileSystemBlockDevice(
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
);

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateDevice(
    const std::string& deviceId,
    const TBlockDeviceConfig& config,
    const NApi::NNative::IClientPtr& client,
    const IInvokerPtr& invoker,
    const NLogging::TLogger& logger)
{
    auto getClientOrThrow = [&] {
        if (!client) {
            THROW_ERROR_EXCEPTION("Device %Qv of type %Qlv requires a client, but none is configured",
                deviceId,
                config.GetCurrentType());
        }
        return client;
    };

    switch (config.GetCurrentType()) {
        case EBlockDeviceConfigType::Memory:
            return CreateMemoryBlockDevice(config.TryGetConcrete<TMemoryBlockDeviceConfig>());
        case EBlockDeviceConfigType::DynamicTable:
            return CreateDynamicTableBlockDevice(
                deviceId,
                config.TryGetConcrete<TDynamicTableBlockDeviceConfig>(),
                getClientOrThrow(),
                logger);
        case EBlockDeviceConfigType::File:
            return CreateCypressFileDevice(
                deviceId,
                config.TryGetConcrete<TCypressFileDeviceConfig>(),
                getClientOrThrow(),
                invoker,
                logger);
        case EBlockDeviceConfigType::Chunk:
            return CreateCypressChunkDevice(
                deviceId,
                config.TryGetConcrete<TCypressChunkDeviceConfig>(),
                client,
                invoker,
                logger);
    }
    YT_ABORT();
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

        for (const auto& [name, deviceConfig] : config->Devices) {
            auto device = CreateDevice(name, deviceConfig, client, threadPool->GetInvoker(), nbdServer->GetLogger());
            WaitFor(device->Initialize())
                .ThrowOnError();
            nbdServer->RegisterDevice(name, std::move(device));
        }

        Sleep(TDuration::Max());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

int main(int argc, const char* argv[])
{
    return NYT::NNbd::TProgram().Run(argc, argv);
}
