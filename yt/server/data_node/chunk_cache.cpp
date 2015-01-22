#include "stdafx.h"
#include "chunk_cache.h"
#include "private.h"
#include "blob_reader_cache.h"
#include "location.h"
#include "blob_chunk.h"
#include "block_store.h"
#include "config.h"
#include "master_connector.h"

#include <core/concurrency/thread_affinity.h>

#include <core/misc/serialize.h>
#include <core/misc/string.h>
#include <core/misc/fs.h>

#include <core/concurrency/thread_affinity.h>

#include <core/logging/log.h>

#include <ytlib/hydra/peer_channel.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/file_writer.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/api/client.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NRpc;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkCache::TImpl
    : public TAsyncSlruCacheBase<TChunkId, TCachedBlobChunk>
{
public:
    TImpl(TDataNodeConfigPtr config, TBootstrap* bootstrap)
        : TAsyncSlruCacheBase(New<TSlruCacheConfig>(config->CacheLocation->Quota.Get(std::numeric_limits<i64>::max())))
        , Config_(config)
        , Bootstrap_(bootstrap)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);
    }

    void Initialize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Location_ = New<TLocation>(
            ELocationType::Cache,
            "cache",
            Config_->CacheLocation,
            Bootstrap_);

        Location_->SubscribeDisabled(
            BIND(&TImpl::OnLocationDisabled, Unretained(this))
                .Via(Bootstrap_->GetControlInvoker()));

        auto descriptors = Location_->Initialize();
        for (const auto& descriptor : descriptors) {
            auto chunk = New<TCachedBlobChunk>(
                Bootstrap_,
                Location_,
                descriptor);
            Put(chunk);
        }

        LOG_INFO("Chunk cache initialized, %v chunks total",
            GetSize());
    }

    void Register(TCachedBlobChunkPtr chunk)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto location = chunk->GetLocation();
        location->UpdateChunkCount(+1);
        location->UpdateUsedSpace(+chunk->GetInfo().disk_space());
    }

    void Unregister(TCachedBlobChunkPtr chunk)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto location = chunk->GetLocation();
        location->UpdateChunkCount(-1);
        location->UpdateUsedSpace(-chunk->GetInfo().disk_space());
    }

    void Put(TCachedBlobChunkPtr chunk)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TInsertCookie cookie(chunk->GetId());
        YCHECK(BeginInsert(&cookie));
        cookie.EndInsert(chunk);
        Register(chunk);
    }

    TFuture<IChunkPtr> Download(
        const TChunkId& chunkId,
        TNodeDirectoryPtr nodeDirectory,
        const TChunkReplicaList& seedReplicas)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Getting chunk from cache (ChunkId: %v)",
            chunkId);

        TInsertCookie cookie(chunkId);
        bool inserted = BeginInsert(&cookie);
        auto cookieValue = cookie.GetValue();
        if (inserted) {
            LOG_INFO("Loading chunk into cache (ChunkId: %v)",
                chunkId);

            auto invoker = CreateSerializedInvoker(Location_->GetWritePoolInvoker());
            invoker->Invoke(BIND(
                &TImpl::DoDownloadChunk,
                MakeStrong(this),
                chunkId,
                nodeDirectory ? std::move(nodeDirectory) : New<TNodeDirectory>(),
                seedReplicas,
                Passed(std::move(cookie))));
        } else {
            LOG_INFO("Chunk is already cached (ChunkId: %v)",
                chunkId);
        }

        return cookieValue.Apply(BIND(&TImpl::OnChunkDownloaded, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker()));
    }

    bool IsEnabled() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Location_->IsEnabled();
    }

    std::vector<IChunkPtr> GetChunks()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto chunks = GetAll();
        return std::vector<IChunkPtr>(chunks.begin(), chunks.end());
    }

private:
    TDataNodeConfigPtr Config_;
    TBootstrap* Bootstrap_;
    TLocationPtr Location_;

    DEFINE_SIGNAL(void(IChunkPtr), ChunkAdded);
    DEFINE_SIGNAL(void(IChunkPtr), ChunkRemoved);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    virtual i64 GetWeight(TCachedBlobChunk* chunk) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return chunk->GetInfo().disk_space();
    }

    virtual void OnAdded(TCachedBlobChunk* value) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TAsyncSlruCacheBase::OnAdded(value);
        ChunkAdded_.Fire(value);
    }

    virtual void OnRemoved(TCachedBlobChunk* value) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TAsyncSlruCacheBase::OnRemoved(value);
        ChunkRemoved_.Fire(value);
    }

    void OnLocationDisabled(const TError& reason)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_WARNING("Chunk cache disabled");
        Clear();

        // Register an alert and
        // schedule an out-of-order heartbeat to notify the master about the disaster.
        auto masterConnector = Bootstrap_->GetMasterConnector();
        masterConnector->RegisterAlert(Format("Chunk cache at %v is disabled\n%v",
            Location_->GetPath(),
            reason));
        masterConnector->ForceRegister();
    }


    void DoDownloadChunk(
        const TChunkId& chunkId,
        TNodeDirectoryPtr nodeDirectory,
        const TChunkReplicaList& seedReplicas,
        TInsertCookie cookie)
    {
        NLog::TLogger Logger(DataNodeLogger);
        Logger.AddTag("ChunkId: %v", chunkId);

        try {
            auto chunkReader = CreateReplicationReader(
                Config_->CacheRemoteReader,
                Bootstrap_->GetBlockStore()->GetCompressedBlockCache(),
                Bootstrap_->GetMasterClient()->GetMasterChannel(),
                nodeDirectory,
                Bootstrap_->GetLocalDescriptor(),
                chunkId,
                seedReplicas);

            auto fileName = Location_->GetChunkPath(chunkId);
            auto chunkWriter = New<TFileWriter>(fileName);

            try {
                NFS::ForcePath(NFS::GetDirectoryName(fileName));
                auto result = chunkWriter->Open();

                // File writer opens synchronously.
                YCHECK(result.IsSet());
                YCHECK(result.Get().IsOK());
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Error opening cached chunk for writing");
            }

            LOG_INFO("Getting chunk meta");
            auto chunkMetaOrError = WaitFor(chunkReader->GetMeta());
            THROW_ERROR_EXCEPTION_IF_FAILED(chunkMetaOrError);
            LOG_INFO("Chunk meta received");
            const auto& chunkMeta = chunkMetaOrError.Value();

            // Download all blocks.
            auto blocksExt = GetProtoExtension<TBlocksExt>(chunkMeta.extensions());
            int blockCount = blocksExt.blocks_size();
            std::vector<TSequentialReader::TBlockInfo> blocks;
            blocks.reserve(blockCount);
            for (int index = 0; index < blockCount; ++index) {
                blocks.push_back(TSequentialReader::TBlockInfo(
                    index,
                    blocksExt.blocks(index).size()));
            }

            auto sequentialReader = New<TSequentialReader>(
                Config_->CacheSequentialReader,
                std::move(blocks),
                chunkReader,
                GetNullBlockCache(),
                NCompression::ECodec::None);

            for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
                LOG_INFO("Downloading block (BlockIndex: %v)",
                    blockIndex);

                auto blockResult = WaitFor(sequentialReader->FetchNextBlock());
                THROW_ERROR_EXCEPTION_IF_FAILED(blockResult);

                LOG_INFO("Writing block (BlockIndex: %v)",
                    blockIndex);
                // NB: This is always done synchronously.
                auto block = sequentialReader->GetCurrentBlock();
                if (!chunkWriter->WriteBlock(block)) {
                    THROW_ERROR_EXCEPTION(chunkWriter->GetReadyEvent().Get());
                }
                LOG_INFO("Block written");
            }


            LOG_INFO("Closing chunk");
            auto closeResult = WaitFor(chunkWriter->Close(chunkMeta));
            THROW_ERROR_EXCEPTION_IF_FAILED(closeResult);
            LOG_INFO("Chunk is closed");

            LOG_INFO("Chunk is downloaded into cache");
            TChunkDescriptor descriptor;
            descriptor.Id = chunkId;
            descriptor.DiskSpace = chunkWriter->GetChunkInfo().disk_space();
            auto chunk = New<TCachedBlobChunk>(
                Bootstrap_,
                Location_,
                descriptor,
                &chunkMeta);
            cookie.EndInsert(chunk);
        } catch (const std::exception& ex) {
            auto error = TError("Error downloading chunk %v into cache",
                chunkId)
                << ex;
            cookie.Cancel(error);
            LOG_WARNING(error);
        }
    }

    IChunkPtr OnChunkDownloaded(TCachedBlobChunkPtr chunk)
    {
        Register(chunk);
        return chunk;
    }

};

////////////////////////////////////////////////////////////////////////////////

TChunkCache::TChunkCache(TDataNodeConfigPtr config, TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

void TChunkCache::Initialize()
{
    Impl_->Initialize();
}

TChunkCache::~TChunkCache()
{ }

IChunkPtr TChunkCache::FindChunk(const TChunkId& chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->Find(chunkId);
}

std::vector<IChunkPtr> TChunkCache::GetChunks()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->GetChunks();
}

int TChunkCache::GetChunkCount()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->GetSize();
}

TFuture<IChunkPtr> TChunkCache::DownloadChunk(
    const TChunkId& chunkId,
    TNodeDirectoryPtr nodeDirectory,
    const TChunkReplicaList& seedReplicas)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->Download(chunkId, nodeDirectory, seedReplicas);
}

bool TChunkCache::IsEnabled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->IsEnabled();
}

DELEGATE_SIGNAL(TChunkCache, void(IChunkPtr), ChunkAdded, *Impl_);
DELEGATE_SIGNAL(TChunkCache, void(IChunkPtr), ChunkRemoved, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
