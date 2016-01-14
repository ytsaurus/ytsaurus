#pragma once

#include "public.h"
#include "chunk_owner_ypath_proxy.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/actions/public.h>

#include <yt/core/erasure/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/logging/public.h>

#include <yt/core/ytree/permission.h>

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

struct TUserObject
{
    NYPath::TRichYPath Path;
    NObjectClient::TObjectId ObjectId;
    NObjectClient::TCellTag CellTag;
    NObjectClient::EObjectType Type = NObjectClient::EObjectType::Null;

    void Persist(NPhoenix::TPersistenceContext& context);
};

template <class T>
void GetUserObjectBasicAttributes(
    NApi::IClientPtr client, 
    TMutableRange<T> objects,
    const NObjectClient::TTransactionId& transactionId,
    const NLogging::TLogger& logger,
    NYTree::EPermission permission,
    bool suppressAccessTracking = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
