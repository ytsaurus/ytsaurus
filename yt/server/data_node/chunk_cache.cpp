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

#include <core/logging/tagged_logger.h>

#include <ytlib/hydra/peer_channel.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/file_writer.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

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
    : public TWeightLimitedCache<TChunkId, TCachedBlobChunk>
{
public:
    TImpl(TDataNodeConfigPtr config, TBootstrap* bootstrap)
        : TWeightLimitedCache(config->CacheLocation->Quota.Get(std::numeric_limits<i64>::max()))
        , Config_(config)
        , Bootstrap_(bootstrap)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);
    }

    void Initialize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Chunk cache scan started");

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
                descriptor.Id,
                descriptor.Info);
            Put(chunk);
        }

        LOG_INFO("Chunk cache scan completed, %d chunks found",
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

    TAsyncDownloadResult Download(const TChunkId& chunkId)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        
        LOG_INFO("Getting chunk from cache (ChunkId: %s)",
            ~ToString(chunkId));

        TInsertCookie cookie(chunkId);
        bool inserted = BeginInsert(&cookie);
        auto cookieValue = cookie.GetValue();
        if (inserted) {
            LOG_INFO("Loading chunk into cache (ChunkId: %s)",
                ~ToString(chunkId));

            auto invoker = CreateSerializedInvoker(Location_->GetWriteInvoker());
            invoker->Invoke(BIND(
                &TImpl::DoDownloadChunk,
                MakeStrong(this),
                chunkId,
                Passed(std::move(cookie))));
        } else {    
            LOG_INFO("Chunk is already cached (ChunkId: %s)",
                ~ToString(chunkId));
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

        TWeightLimitedCache::OnAdded(value);
        ChunkAdded_.Fire(value);
    }

    virtual void OnRemoved(TCachedBlobChunk* value) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TWeightLimitedCache::OnRemoved(value);
        ChunkRemoved_.Fire(value);
    }

    void OnLocationDisabled()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_WARNING("Chunk cache disabled");
        Clear();

        // Register an alert and
        // schedule an out-of-order heartbeat to notify the master about the disaster.
        auto masterConnector = Bootstrap_->GetMasterConnector();
        masterConnector->RegisterAlert("Chunk cache is disabled");
        masterConnector->ForceRegister();
    }


    void DoDownloadChunk(const TChunkId& chunkId, TInsertCookie cookie)
    {
        NLog::TTaggedLogger Logger(DataNodeLogger);
        Logger.AddTag("ChunkId: %v", chunkId);

        try {
            auto nodeDirectory = New<TNodeDirectory>();

            auto chunkReader = CreateReplicationReader(
                Config_->CacheRemoteReader,
                Bootstrap_->GetBlockStore()->GetBlockCache(),
                Bootstrap_->GetMasterClient()->GetMasterChannel(),
                nodeDirectory,
                Bootstrap_->GetLocalDescriptor(),
                chunkId);

            auto fileName = Location_->GetChunkFileName(chunkId);
            auto chunkWriter = New<TFileWriter>(fileName);
            try {
                NFS::ForcePath(NFS::GetDirectoryName(fileName));
                chunkWriter->Open();
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
                NCompression::ECodec::None);

            for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
                LOG_INFO("Downloading block (BlockIndex: %d)",
                    blockIndex);

                auto blockResult = WaitFor(sequentialReader->AsyncNextBlock());
                THROW_ERROR_EXCEPTION_IF_FAILED(blockResult);

                LOG_INFO("Writing block (BlockIndex: %d)",
                    blockIndex);
                // NB: This is always done synchronously.
                auto block = sequentialReader->GetBlock();
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
            auto chunk = New<TCachedBlobChunk>(
                Bootstrap_,
                Location_,
                chunkId,
                chunkWriter->GetChunkInfo(),
                &chunkMeta);
            cookie.EndInsert(chunk);
        } catch (const std::exception& ex) {
            auto error = TError("Error downloading chunk %s into cache",
                ~ToString(chunkId))
                << ex;
            cookie.Cancel(error);
            LOG_WARNING(error);
        }
    }

    TDownloadResult OnChunkDownloaded(TErrorOr<TCachedBlobChunkPtr> result)
    {
        if (!result.IsOK()) {
            return TError(result);
        }

        auto chunk = result.Value();
        Register(chunk);
        return TDownloadResult(chunk);
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

TChunkCache::TAsyncDownloadResult TChunkCache::DownloadChunk(const TChunkId& chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->Download(chunkId);
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
