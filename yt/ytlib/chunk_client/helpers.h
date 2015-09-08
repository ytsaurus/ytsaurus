#include "public.h"
#include "chunk_owner_ypath_proxy.h"

#include <ytlib/api/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/logging/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Synchronously creates a new chunk at a given cell.
NChunkClient::TChunkId CreateChunk(
    NApi::IClientPtr client,
    NObjectClient::TCellTag cellTag,
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    NObjectClient::EObjectType chunkType,
    const NTransactionClient::TTransactionId& transactionId,
    const NLogging::TLogger& logger);

//! Synchronously parses #fetchResponse, populates #nodeDirectory,
//! issues additional |LocateChunks| requests for foreign chunks.
//! The resulting chunk specs are appended to #chunkSpecs.
void ProcessFetchResponse(
    NApi::IClientPtr client,
    TChunkOwnerYPathProxy::TRspFetchPtr fetchResponse,
    NObjectClient::TCellTag fetchCellTag,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    int maxChunksPerLocateRequest,
    const NLogging::TLogger& logger,
    std::vector<NProto::TChunkSpec>* chunkSpecs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
