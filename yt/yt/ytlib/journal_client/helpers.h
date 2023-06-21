#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/erasure/impl/public.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

void ValidateJournalAttributes(
    NErasure::ECodec codecId,
    int replicationFactor,
    int readQuorum,
    int writeQuorum);

////////////////////////////////////////////////////////////////////////////////

struct TChunkReplicaDescriptor
{
    NNodeTrackerClient::TNodeDescriptor NodeDescriptor;
    int ReplicaIndex = NChunkClient::GenericChunkReplicaIndex;
    int MediumIndex = NChunkClient::GenericMediumIndex;
};

void FormatValue(TStringBuilderBase* builder, const TChunkReplicaDescriptor& replica, TStringBuf spec);
TString ToString(const TChunkReplicaDescriptor& replica);

////////////////////////////////////////////////////////////////////////////////

TFuture<std::vector<TChunkReplicaDescriptor>> AbortSessionsQuorum(
    NChunkClient::TChunkId chunkId,
    std::vector<TChunkReplicaDescriptor> replicas,
    TDuration abortRequestTimeout,
    TDuration quorumSessionDelay,
    int quorum,
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory);

struct TChunkQuorumInfo
{
    //! The index (w.r.t. the whole journal) of the first row, for overlayed chunks.
    //! Null for non-overlayed chunks or overlayed chunks with all replicas empty.
    std::optional<i64> FirstOverlayedRowIndex;

    //! The quorum number of rows (across all chunk replicas).
    //! For overlayed chunks, this excludes the header row.
    i64 RowCount = 0;

    //! Some approximation for the uncompressed data size of the journal chunk.
    i64 UncompressedDataSize = 0;

    //! Some approximation for the compressed data size of the journal chunk.
    i64 CompressedDataSize = 0;

    //! Number of replicas with at least #RowCount rows.
    int RowCountConfirmedReplicaCount = 0;
};

TFuture<TChunkQuorumInfo> ComputeQuorumInfo(
    NChunkClient::TChunkId chunkId,
    bool overlayed,
    NErasure::ECodec codecId,
    int readQuorum,
    i64 replicaLagLimit,
    std::vector<TChunkReplicaDescriptor> replicas,
    TDuration requestTimeout,
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory);

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 1)

struct TErasureRowHeader
{
    i8 PaddingSize;
};

#pragma pack(pop)

std::vector<std::vector<TSharedRef>> EncodeErasureJournalRows(
    NErasure::ICodec* codec,
    const std::vector<TSharedRef>& rows);
std::vector<TSharedRef> EncodeErasureJournalRow(
    NErasure::ICodec* codec,
    const TSharedRef& row);

std::vector<TSharedRef> DecodeErasureJournalRows(
    NErasure::ICodec* codec,
    const std::vector<std::vector<TSharedRef>>& encodedRowLists);

std::vector<std::vector<TSharedRef>> RepairErasureJournalRows(
    NErasure::ICodec* codec,
    const NErasure::TPartIndexList& erasedIndices,
    const std::vector<std::vector<TSharedRef>>& repairRowLists);

////////////////////////////////////////////////////////////////////////////////

i64 GetPhysicalChunkRowCount(i64 logicalRowCount, bool overlayed);
i64 GetLogicalChunkRowCount(i64 physicalRowCount, bool overlayed);

////////////////////////////////////////////////////////////////////////////////

i64 GetJournalRowCount(
    i64 previousJournalRowCount,
    std::optional<i64> lastChunkFirstRowIndex,
    i64 lastChunkRowCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
