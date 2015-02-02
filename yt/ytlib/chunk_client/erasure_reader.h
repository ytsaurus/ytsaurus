#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/actions/signal.h>
#include <core/erasure/public.h>
#include <core/rpc/public.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateNonRepairingErasureReader(
    const std::vector<IChunkReaderPtr>& dataBlocksReaders);

typedef TCallback<void(double)> TRepairProgressHandler;

TFuture<void> RepairErasedParts(
    NErasure::ICodec* codec,
    const NErasure::TPartIndexList& erasedIndices,
    const std::vector<IChunkReaderPtr>& readers,
    const std::vector<IChunkWriterPtr>& writers,
    TRepairProgressHandler onProgress = TRepairProgressHandler());

std::vector<IChunkReaderPtr> CreateErasureDataPartsReaders(
    TReplicationReaderConfigPtr config,
    IBlockCachePtr compressedBlockCache,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas,
    const NErasure::ICodec* codec,
    const Stroka& networkName = NNodeTrackerClient::InterconnectNetworkName);

std::vector<IChunkReaderPtr> CreateErasureAllPartsReaders(
    TReplicationReaderConfigPtr config,
    IBlockCachePtr compressedBlockCache,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas,
    const NErasure::ICodec* codec,
    const Stroka& networkName = NNodeTrackerClient::InterconnectNetworkName);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

