#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRemoteSortedDynamicStoreReader(
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    NTabletClient::TRemoteDynamicStoreReaderConfigPtr config,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp);

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRetryingRemoteSortedDynamicStoreReader(
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    NTabletClient::TRetryingRemoteDynamicStoreReaderConfigPtr config,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    NChunkClient::TChunkReaderMemoryManagerHolderPtr readerMemoryManagerHolder,
    TCallback<IVersionedReaderPtr(
        NChunkClient::NProto::TChunkSpec,
        NChunkClient::TChunkReaderMemoryManagerHolderPtr)> chunkReaderFactory);

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateRemoteOrderedDynamicStoreReader(
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    NTabletClient::TRemoteDynamicStoreReaderConfigPtr config,
    NTableClient::TChunkReaderOptionsPtr options,
    TNameTablePtr nameTable,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const std::optional<std::vector<TString>>& columns);

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateRetryingRemoteOrderedDynamicStoreReader(
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    NTabletClient::TRetryingRemoteDynamicStoreReaderConfigPtr config,
    NTableClient::TChunkReaderOptionsPtr options,
    TNameTablePtr nameTable,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const std::optional<std::vector<TString>>& columns,
    NChunkClient::TChunkReaderMemoryManagerHolderPtr readerMemoryManagerHolder,
    TCallback<TFuture<ISchemalessChunkReaderPtr>(
        NChunkClient::NProto::TChunkSpec,
        NChunkClient::TChunkReaderMemoryManagerHolderPtr)> chunkReaderFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT:NTabletClient
