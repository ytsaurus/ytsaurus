#include "public.h"
#include "chunk_owner_ypath_proxy.h"
#include "chunk_service_proxy.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/erasure/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/logging/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TChunkId CreateChunk(
    NApi::IClientPtr client,
    NObjectClient::TCellTag cellTag,
    TMultiChunkWriterOptionsPtr options,
    const NObjectClient::TTransactionId& transactionId,
    const TChunkListId& chunkListId,
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

//! Synchronously invokes TChunkServiceProxy::AllocateWriteTargets.
//! Populates #nodeDirectory with the returned node descriptors.
//! Throws if the server returns no replicas.
TChunkReplicaList AllocateWriteTargets(
    NApi::IClientPtr client,
    const TChunkId& chunkId,
    int desiredTargetCount,
    int minTargetCount,
    TNullable<int> replicationFactorOverride,
    bool preferLocalHost,
    const std::vector<Stroka>& forbiddenAddresses,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NLogging::TLogger& logger);

//! Returns the cumulative error for the whole batch.
/*!
 *  If the envelope request fails then the corresponding error is returned.
 *  Otherwise, subresponses are examined and a cumulative error
 *  is constructed (with individual errors attached as inner).
 *  If all subresponses were successful then OK is returned.
 */
TError GetCumulativeError(const TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError);

////////////////////////////////////////////////////////////////////////////////

i64 GetChunkReaderMemoryEstimate(const NProto::TChunkSpec& chunkSpec, TMultiChunkReaderConfigPtr config);

IChunkReaderPtr CreateRemoteReader(
    const NProto::TChunkSpec& chunkSpec, 
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NApi::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IBlockCachePtr blockCache,
    NConcurrency::IThroughputThrottlerPtr throttler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
