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

IReaderPtr CreateNonRepairingErasureReader(
    const std::vector<IReaderPtr>& dataBlocksReaders);

typedef TCallback<void(double)> TRepairProgressHandler;

TAsyncError RepairErasedParts(
    NErasure::ICodec* codec,
    const NErasure::TPartIndexList& erasedIndices,
    const std::vector<IReaderPtr>& readers,
    const std::vector<IWriterPtr>& writers,
    TRepairProgressHandler onProgress = TRepairProgressHandler());

std::vector<IReaderPtr> CreateErasureDataPartsReaders(
    TReplicationReaderConfigPtr config,
    IBlockCachePtr compressedBlockCache,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas,
    const NErasure::ICodec* codec,
    const Stroka& networkName = NNodeTrackerClient::DefaultNetworkName);

std::vector<IReaderPtr> CreateErasureAllPartsReaders(
    TReplicationReaderConfigPtr config,
    IBlockCachePtr compressedBlockCache,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas,
    const NErasure::ICodec* codec,
    const Stroka& networkName = NNodeTrackerClient::DefaultNetworkName);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

