#include "erasure_reader.h"
#include "block_cache.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader.h"
#include "chunk_replica.h"
#include "chunk_writer.h"
#include "config.h"
#include "dispatcher.h"
#include "erasure_helpers.h"
#include "block.h"
#include "replication_reader.h"

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/config.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/erasure/codec.h>
#include <yt/core/erasure/helpers.h>

#include <numeric>

namespace NYT {
namespace NChunkClient {

using namespace NApi;
using namespace NErasure;
using namespace NConcurrency;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NErasureHelpers;

////////////////////////////////////////////////////////////////////////////////
// Non-repairing reader

class TNonRepairingReaderSession
    : public TRefCounted
{
public:
    TNonRepairingReaderSession(
        const std::vector<IChunkReaderPtr>& readers,
        const TErasurePlacementExt& placementExt,
        const std::vector<int>& blockIndexes,
        const TWorkloadDescriptor& workloadDescriptor)
        : Readers_(readers)
        , BlockIndexes_(blockIndexes)
        , WorkloadDescriptor_(workloadDescriptor)
        , DataBlocksPlacementInParts_(BuildDataBlocksPlacementInParts(BlockIndexes_, placementExt))
    { }

    TFuture<std::vector<TBlock>> Run()
    {
        std::vector<TFuture<std::vector<TBlock>>> readBlocksFutures;
        for (int readerIndex = 0; readerIndex < Readers_.size(); ++readerIndex) {
            auto blocksPlacementInPart = DataBlocksPlacementInParts_[readerIndex];
            auto reader = Readers_[readerIndex];
            readBlocksFutures.push_back(reader->ReadBlocks(WorkloadDescriptor_, blocksPlacementInPart.IndexesInPart));
        }

        return Combine(readBlocksFutures).Apply(
            BIND([=, this_ = MakeStrong(this)] (std::vector<std::vector<TBlock>> readBlocks) {
                std::vector<TBlock> resultBlocks(BlockIndexes_.size());
                for (int readerIndex = 0; readerIndex < readBlocks.size(); ++readerIndex) {
                    auto blocksPlacementInPart = DataBlocksPlacementInParts_[readerIndex];
                    for (int blockIndex = 0; blockIndex < readBlocks[readerIndex].size(); ++blockIndex) {
                        resultBlocks[blocksPlacementInPart.IndexesInRequest[blockIndex]] = readBlocks[readerIndex][blockIndex];
                    }
                }
                return resultBlocks;
            }));
    }

private:
    const std::vector<IChunkReaderPtr> Readers_;
    const std::vector<int> BlockIndexes_;
    const TWorkloadDescriptor WorkloadDescriptor_;

    TDataBlocksPlacementInParts DataBlocksPlacementInParts_;
};

////////////////////////////////////////////////////////////////////////////////

class TNonRepairingReader
    : public TErasureChunkReaderBase
{
public:
    TNonRepairingReader(ICodec* codec, const std::vector<IChunkReaderPtr>& readers)
        : TErasureChunkReaderBase(codec, readers)
    {
        YCHECK(!Readers_.empty());
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<int>& blockIndexes) override
    {
        return PreparePlacementMeta(workloadDescriptor).Apply(
            BIND([=, this_ = MakeStrong(this)] () {
                auto session = New<TNonRepairingReaderSession>(
                    Readers_,
                    PlacementExt_,
                    blockIndexes,
                    workloadDescriptor);
                return session->Run();
            }).AsyncVia(TDispatcher::Get()->GetReaderInvoker()));
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        int firstBlockIndex,
        int blockCount) override
    {
        std::vector<int> blockIndexes;
        blockIndexes.reserve(blockCount);
        for (int index = firstBlockIndex; index < firstBlockIndex + blockCount; ++index) {
            blockIndexes.push_back(index);
        }
        return ReadBlocks(workloadDescriptor, blockIndexes);
    }
};

IChunkReaderPtr CreateNonRepairingErasureReader(
    ICodec* codec,
    const std::vector<IChunkReaderPtr>& dataBlockReaders)
{
    return New<TNonRepairingReader>(codec, dataBlockReaders);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<IChunkReaderPtr> CreateErasurePartsReaders(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    INativeClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TChunkId& chunkId,
    const TChunkReplicaList& replicas,
    const ICodec* codec,
    int partCount,
    IBlockCachePtr blockCache,
    IThroughputThrottlerPtr throttler)
{
    YCHECK(IsErasureChunkId(chunkId));

    auto sortedReplicas = replicas;
    std::sort(
        sortedReplicas.begin(),
        sortedReplicas.end(),
        [] (TChunkReplica lhs, TChunkReplica rhs) {
            return lhs.GetReplicaIndex() < rhs.GetReplicaIndex();
        });

    std::vector<IChunkReaderPtr> readers;
    readers.reserve(partCount);

    {
        auto it = sortedReplicas.begin();
        while (it != sortedReplicas.end() && it->GetReplicaIndex() < partCount) {
            auto jt = it;
            while (jt != sortedReplicas.end() &&
                   it->GetReplicaIndex() == jt->GetReplicaIndex())
            {
                ++jt;
            }

            TChunkReplicaList partReplicas(it, jt);
            auto partId = ErasurePartIdFromChunkId(chunkId, it->GetReplicaIndex());
            auto reader = CreateReplicationReader(
                config,
                options,
                client,
                nodeDirectory,
                // Locality doesn't matter, since we typically have only one replica.
                TNodeDescriptor(),
                partId,
                partReplicas,
                blockCache,
                throttler);
            readers.push_back(reader);

            it = jt;
        }
    }
    YCHECK(readers.size() == partCount);

    return readers;
}

} // namespace

std::vector<IChunkReaderPtr> CreateErasureDataPartsReaders(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    INativeClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas,
    const ICodec* codec,
    const TString& networkName,
    IBlockCachePtr blockCache,
    NConcurrency::IThroughputThrottlerPtr throttler)
{
    return CreateErasurePartsReaders(
        config,
        options,
        client,
        nodeDirectory,
        chunkId,
        seedReplicas,
        codec,
        codec->GetDataPartCount(),
        blockCache,
        throttler);
}

std::vector<IChunkReaderPtr> CreateErasureAllPartsReaders(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    INativeClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas,
    const ICodec* codec,
    IBlockCachePtr blockCache,
    NConcurrency::IThroughputThrottlerPtr throttler)
{
    return CreateErasurePartsReaders(
        config,
        options,
        client,
        nodeDirectory,
        chunkId,
        seedReplicas,
        codec,
        codec->GetTotalPartCount(),
        blockCache,
        throttler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

