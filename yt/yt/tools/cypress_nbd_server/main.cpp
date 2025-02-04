#include <yt/yt/server/lib/nbd/block_device.h>
#include <yt/yt/server/lib/nbd/config.h>
#include <yt/yt/server/lib/nbd/file_system_block_device.h>
#include <yt/yt/server/lib/nbd/image_reader.h>
#include <yt/yt/server/lib/nbd/server.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NNbd {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NFileClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TCypressFileBlockDeviceConfig
    : public NYTree::TYsonStruct
{
public:
    TYPath Path;

    // For testing purposes: how long to sleep before read request
    TDuration TestSleepBeforeRead;

    REGISTER_YSON_STRUCT(TCypressFileBlockDeviceConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path)
            .Default();
        registrar.Parameter("test_sleep_before_read", &TThis::TestSleepBeforeRead)
            .Default(TDuration::Zero());
    }
};

DECLARE_REFCOUNTED_CLASS(TCypressFileBlockDeviceConfig)
DEFINE_REFCOUNTED_TYPE(TCypressFileBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

class TConfig
    : public NYTree::TYsonStruct
{
public:
    TString ClusterUser;
    NApi::NNative::TConnectionCompoundConfigPtr ClusterConnection;
    TNbdServerConfigPtr NbdServer;
    THashMap<TString, TCypressFileBlockDeviceConfigPtr> FileSystemBlockDevices;
    int ThreadCount;

    REGISTER_YSON_STRUCT(TConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("cluster_user", &TThis::ClusterUser)
            .Default();
        registrar.Parameter("cluster_connection", &TThis::ClusterConnection)
            .DefaultNew();
        registrar.Parameter("nbd_server", &TThis::NbdServer)
            .DefaultNew();
        registrar.Parameter("file_exports", &TThis::FileSystemBlockDevices)
            .Default();
        registrar.Parameter("thread_count", &TThis::ThreadCount)
            .Default(2);
    }
};

DECLARE_REFCOUNTED_CLASS(TConfig)
DEFINE_REFCOUNTED_TYPE(TConfig)

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
{
public:
    TProgram()
    {
        Opts_.AddLongOption("config", "path to config").StoreResult(&ConfigPath_).Required();
    }

protected:
    //! Fetch basic object attributes from Cypress.
    TUserObject GetUserObject(
        const TRichYPath& richPath,
        NNative::IClientPtr client,
        const TLogger& Logger)
    {
        YT_LOG_INFO("Fetching file basic attributes (File: %v)", richPath);

        TUserObject userObject(richPath);

        GetUserObjectBasicAttributes(
            client,
            {&userObject},
            NullTransactionId,
            Logger,
            NYTree::EPermission::Read);

        YT_LOG_INFO("Fetched file basic attributes (File: %v)", richPath);

        return userObject;
    }

    //! Fetch object's filesystem attribute from Cypress.
    TString GetFilesystem(
        const TUserObject& userObject,
        const NNative::IClientPtr& client,
        const TLogger& Logger)
    {
        YT_LOG_INFO("Fetching file filesystem attribute (File: %v)", userObject.GetPath());

        auto proxy = CreateObjectServiceReadProxy(
            client,
            NApi::EMasterChannelKind::Follower,
            userObject.ExternalCellTag);

        auto req = TYPathProxy::Get(userObject.GetObjectIdPath() + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "filesystem",
        });

        AddCellTagToSyncWith(
            req,
            userObject.ObjectId);
        SetTransactionId(
            req,
            NullTransactionId);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error requesting extended attributes for file %Qlv",
            userObject.GetPath());

        const auto& rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
        auto filesystem = attributes->Get<TString>(
            /*key*/ "filesystem",
            /*defaultValue*/ "");

        YT_LOG_INFO(
            "Fetched file filesystem attribute (File: %v, Filesystem: %v)",
            userObject.GetPath(),
            filesystem);

        return filesystem;
    }

    //! Fetch object's chunk specs from Cypress.
    std::vector<NChunkClient::NProto::TChunkSpec> GetChunkSpecs(
        const TUserObject& userObject,
        const NApi::NNative::IClientPtr& client,
        const NLogging::TLogger& Logger)
    {
        YT_LOG_INFO("Fetching file chunk specs (File: %v)", userObject.GetPath());

        auto proxy = CreateObjectServiceReadProxy(
            client,
            NApi::EMasterChannelKind::Follower,
            userObject.ExternalCellTag);

        auto batchReq = proxy.ExecuteBatchWithRetries(client->GetNativeConnection()->GetConfig()->ChunkFetchRetries);

        auto req = TFileYPathProxy::Fetch(userObject.GetObjectIdPath());
        AddCellTagToSyncWith(req, userObject.ObjectId);

        TLegacyReadLimit lowerLimit, upperLimit;
        ToProto(req->mutable_ranges(), std::vector<TLegacyReadRange>({TLegacyReadRange(lowerLimit, upperLimit)}));

        SetTransactionId(
            req,
            NullTransactionId);
        req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);

        batchReq->AddRequest(req);
        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error fetching chunks for file %Qlv",
            userObject.GetPath());

        const auto& batchRsp = batchRspOrError.Value();
        const auto& rspOrError = batchRsp->GetResponse<TFileYPathProxy::TRspFetch>(0);
        const auto& rsp = rspOrError.Value();

        std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;
        ProcessFetchResponse(
            client,
            rsp,
            userObject.ExternalCellTag,
            client->GetNativeConnection()->GetNodeDirectory(),
            /*maxChunksPerLocateRequest*/ 10,
            /*rangeIndex*/ std::nullopt,
            Logger,
            &chunkSpecs);

        YT_LOG_INFO("Fetched file chunk specs (File: %v, ChunkSpecs: %v)",
            userObject.GetPath(),
            chunkSpecs.size());

        return chunkSpecs;
    }

    void DoRun() override
    {
        auto singletonsConfig = New<TSingletonsConfig>();
        ConfigureSingletons(singletonsConfig);

        auto config = NYT::NYTree::ConvertTo<NYT::NNbd::TConfigPtr>(NYT::NYson::TYsonString(TFileInput(ConfigPath_).ReadAll()));

        auto poller = NConcurrency::CreateThreadPoolPoller(config->ThreadCount, "Poller");
        auto threadPool = NConcurrency::CreateThreadPool(config->ThreadCount, "Nbd");

        NApi::NNative::TConnectionOptions connectionOptions;
        auto blockCacheConfig = New<NChunkClient::TBlockCacheConfig>();
        blockCacheConfig->CompressedData->Capacity = 512_MB;
        connectionOptions.BlockCache = CreateClientBlockCache(
            std::move(blockCacheConfig),
            NChunkClient::EBlockType::CompressedData,
            GetNullMemoryUsageTracker());
        connectionOptions.ConnectionInvoker = threadPool->GetInvoker();
        auto connection = CreateConnection(config->ClusterConnection, std::move(connectionOptions));
        connection->GetNodeDirectorySynchronizer()->Start();
        connection->GetClusterDirectorySynchronizer()->Start();

        auto nbdServer = CreateNbdServer(
            config->NbdServer,
            connection,
            poller,
            threadPool->GetInvoker());

        auto clientOptions = NYT::NApi::TClientOptions::FromUser(config->ClusterUser);
        auto client = connection->CreateNativeClient(clientOptions);

        for (const auto& [exportId, exportConfig] : config->FileSystemBlockDevices) {
            auto logger = nbdServer->GetLogger();

            TRichYPath richPath{exportConfig->Path};
            auto userObject = GetUserObject(
                richPath,
                client,
                logger);
            if (userObject.Type != NCypressClient::EObjectType::File) {
                THROW_ERROR_EXCEPTION(
                    "Invalid type of file %Qlv, expected %Qlv, but got %Qlv",
                    userObject.GetPath(),
                    NCypressClient::EObjectType::File,
                    userObject.Type);
            }

            auto filesystem = GetFilesystem(
                userObject,
                client,
                logger);
            if (filesystem != "ext4" && filesystem != "squashfs") {
                THROW_ERROR_EXCEPTION(
                    "Invalid filesystem attribute %Qv of file %v",
                    filesystem,
                    userObject.GetPath());
            }

            auto chunkSpecs = GetChunkSpecs(
                userObject,
                client,
                logger);

            auto fileReader = CreateRandomAccessFileReader(
                std::move(chunkSpecs),
                exportConfig->Path,
                TChunkReaderHost::FromClient(client),
                GetUnlimitedThrottler(),
                GetUnlimitedThrottler(),
                threadPool->GetInvoker(),
                logger);

            auto imageReader = CreateCypressFileImageReader(
                std::move(fileReader),
                logger);

            auto config = New<TFileSystemBlockDeviceConfig>();
            config->TestSleepBeforeRead = exportConfig->TestSleepBeforeRead;

            auto device = CreateFileSystemBlockDevice(
                exportId,
                std::move(config),
                std::move(imageReader),
                threadPool->GetInvoker(),
                logger);

            NConcurrency::WaitFor(device->Initialize()).ThrowOnError();
            nbdServer->RegisterDevice(exportId, std::move(device));
        }

        Sleep(TDuration::Max());
    }

private:
    TString ConfigPath_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

int main(int argc, const char* argv[])
{
    return NYT::NNbd::TProgram().Run(argc, argv);
}
