#include "config.h"

#include <yt/yt/server/lib/misc/cluster_connection.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/yt/ytlib/chunk_client/proto/chunk_owner_ypath.pb.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/ytlib/file_client/file_chunk_reader.h>
#include <yt/yt/ytlib/file_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/file_client/proto/file_chunk_meta.pb.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/helpers.h>
#include <yt/yt/library/program/program_config_mixin.h>

#include <yt/yt/client/chunk_client/read_limit.h>
#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/ema_counter.h>

#include <random>

namespace NYT {

using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NNet;
using namespace NFileClient;
using namespace NProfiling;
using namespace NTracing;

static NLogging::TLogger Logger("Downloader");

////////////////////////////////////////////////////////////////////////////////

class TNativeFileDownloader
    : public TProgram
    , public TProgramConfigMixin<TConfig>
{
public:
    TNativeFileDownloader()
        : TProgramConfigMixin<TConfig>(Opts_, false)
    {
        Opts_.AddLongOption("dst-path").StoreResult(&DstPath_);
        Opts_.AddLongOption("cluster-proxy").StoreResult(&ClusterProxy_);
        Opts_.AddLongOption("user").StoreResult(&User_);
        Opts_.AddLongOption("chunk-id").StoreResult(&ChunkIdStr_);
        Opts_.AddLongOption("node-address").StoreResult(&NodeAddress_);
    }

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();

        if (HandleConfigOptions()) {
            return;
        }

        Config_ = GetConfig();

        AbortOnUnrecognizedOptions(Logger, Config_);

        Config_->Stockpile->ThreadCount = 0;
        ConfigureSingletons(Config_);

        ActionQueue_ = New<TActionQueue>();

        WaitFor(BIND([&] {
            SetupClient();
            UpdateNodeDirectory();
            DownloadChunk();
        })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run())
            .ThrowOnError();
    }

    void SetupClient()
    {
        auto clusterConnection = DownloadClusterConnection(ClusterProxy_, Logger);
        auto config = ConvertTo<NApi::NNative::TConnectionCompoundConfigPtr>(clusterConnection);

        Connection_ = NApi::NNative::CreateConnection(config);
        Client_ = NApi::NNative::CreateClient(Connection_, NApi::TClientOptions{.User = User_});
    }

    void UpdateNodeDirectory()
    {
        auto clusterMeta = WaitFor(Client_->GetClusterMeta(NApi::TGetClusterMetaOptions{.PopulateNodeDirectory = true}))
            .ValueOrThrow();
        Connection_->GetNodeDirectory()->MergeFrom(*clusterMeta.NodeDirectory);
        auto nodeDescriptors = Connection_->GetNodeDirectory()->GetAllDescriptors();
        bool found = false;
        for (const auto& [id, descriptor] : nodeDescriptors) {
            YT_LOG_DEBUG("Node descriptor (NodeId: %v, NodeAddress: %v)", id, descriptor.GetDefaultAddress());
            if (descriptor.GetDefaultAddress() == NodeAddress_){
                YT_VERIFY(!found);
                NodeId_ = id;
                found = true;
            }
        }
        if (!found) {
            THROW_ERROR_EXCEPTION("Node with address %v not found", NodeAddress_);
        }
        YT_LOG_INFO("Node descriptor found (NodeId: %v, NodeAddress: %v)", NodeId_, NodeAddress_);
    }

    void DownloadChunk()
    {
        auto readerOptions = New<TRemoteReaderOptions>();
        readerOptions->AllowFetchingSeedsFromMaster = false;

        auto chunkReaderHost = New<TChunkReaderHost>(
            Client_,
            TNodeDescriptor(std::string(GetLocalHostName())),
            GetNullBlockCache(),
            /*chunkMetaCache*/ nullptr,
            /*nodeStatusDirectory*/ nullptr,
            /*bandwidthThrottler*/ GetUnlimitedThrottler(),
            /*rpsThrottler*/ GetUnlimitedThrottler(),
            /*mediumThrottler*/ GetUnlimitedThrottler(),
            /*trafficMeter*/ nullptr);

        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::Idle, 0, TInstant::Zero(), {"Download file"}),
            .MultiplexingParallelism = Config_->MultiplexingParallelism,
        };

        TChunkReplica seedReplica(NodeId_, /*replicaIndex*/ 0);

        ChunkId_ = TChunkId::FromString(ChunkIdStr_);

        auto replicationReader = CreateReplicationReader(
            Config_->Reader,
            readerOptions,
            chunkReaderHost,
            ChunkId_,
            TChunkReplicaWithMediumList{TChunkReplicaWithMedium(seedReplica)});

        YT_LOG_INFO("Downloading meta (ChunkId: %v)", ChunkId_);

        TWallTimer timer;

        auto asyncMeta = replicationReader->GetMeta(
            IChunkReader::TGetMetaOptions{.ClientOptions = chunkReadOptions});
        auto chunkMeta = WaitFor(asyncMeta).ValueOrThrow();

        auto duration = timer.GetElapsedTime();

        YT_LOG_DEBUG("Chunk meta debug string (DebugString: %v)", chunkMeta->DebugString());
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkMeta->extensions());
        auto blocksExt = GetProtoExtension<NFileClient::NProto::TBlocksExt>(chunkMeta->extensions());
        YT_LOG_DEBUG("Chunk meta misc ext debug string (DebugString: %v)", miscExt.DebugString());
        YT_LOG_DEBUG("Chunk meta file blocks ext debug string (DebugString: %v)", blocksExt.DebugString());
        YT_LOG_INFO(
            "Chunk meta downloaded (ChunkId: %v, UncompressedSize: %v, BlockCount: %v, WallTime: %v)",
            ChunkId_,
            miscExt.uncompressed_data_size(),
            blocksExt.blocks_size(),
            duration);

        {
            timer.Restart();

            YT_LOG_INFO("Downloading blocks (BlockReadParallelism: %v)", Config_->BlockReadParallelism);

            std::vector<std::vector<int>> fiberBlockIndices;
            for (int fiberIndex = 0; fiberIndex < Config_->BlockReadParallelism; ++fiberIndex) {
                std::vector<int> blockIndices;
                for (int i = fiberIndex; i < blocksExt.blocks_size(); i += Config_->BlockReadParallelism) {
                    blockIndices.push_back(i);
                }
                fiberBlockIndices.push_back(std::move(blockIndices));
            }

            std::vector<TFuture<i64>> fiberAsyncReadBytes;
            for (int fiberIndex = 0; fiberIndex < Config_->BlockReadParallelism; ++fiberIndex) {
                fiberAsyncReadBytes.push_back(
                    BIND(
                        &TNativeFileDownloader::DownloadBlocks,
                        this,
                        fiberIndex,
                        fiberBlockIndices[fiberIndex],
                        replicationReader,
                        chunkReadOptions)
                        .AsyncVia(ActionQueue_->GetInvoker())
                        .Run());
            }
            auto result = WaitFor(AllSucceeded(fiberAsyncReadBytes))
                .ValueOrThrow();

            i64 totalSize = 0;
            for (const auto& size : result) {
                totalSize += size;
            }
//            YT_VERIFY(totalSize == miscExt.uncompressed_data_size());

            duration = timer.GetElapsedTime();
            YT_LOG_INFO(
                "Blocks downloaded (UncompressedSize: %v, WallTime: %v, Speed: %v MiB/sec)",
                totalSize,
                duration,
                static_cast<double>(totalSize) / (1024 * 1024) / duration.SecondsFloat());
        }
    }

    i64 DownloadBlocks(
        int fiberIndex,
        std::vector<int> blockIndices,
        IChunkReaderPtr replicationReader,
        TClientChunkReadOptions chunkReadOptions)
    {
        TTraceContextGuard guard(TTraceContext::NewRoot("BlockRead"));

        chunkReadOptions.ReadSessionId = TGuid::Create();
        YT_LOG_INFO(
            "Downloading blocks (FiberIndex: %v, BlockCount: %v, ReadSessionId: %v)",
            fiberIndex,
            blockIndices.size(),
            chunkReadOptions.ReadSessionId);
        auto blocks = WaitFor(
            replicationReader->ReadBlocks(
                IChunkReader::TReadBlocksOptions{.ClientOptions = chunkReadOptions},
                blockIndices))
            .ValueOrThrow();

        i64 totalSize = 0;
        for (const auto& block : blocks) {
            totalSize += block.Size();
        }
        YT_LOG_INFO("Blocks downloaded (FiberIndex: %v, BlockCount: %v)", fiberIndex, blockIndices.size());

        return totalSize;
    }

private:
    std::string DstPath_;
    std::string ClusterProxy_;
    std::string NodeAddress_;
    std::string ChunkIdStr_;
    std::string User_;

    TChunkId ChunkId_;
    TNodeId NodeId_;

    TConfigPtr Config_;

    TActionQueuePtr ActionQueue_;

    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::IClientPtr Client_;

    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TNativeFileDownloader().Run(argc, argv);
}
