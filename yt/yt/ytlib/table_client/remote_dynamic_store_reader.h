#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/client/node_tracker_client/public.h>

#include <yt/client/tablet_client/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRemoteSortedDynamicStoreReader(
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    NTabletClient::TRemoteDynamicStoreReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::TTrafficMeterPtr trafficMeter,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp);

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRetryingRemoteSortedDynamicStoreReader(
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    NTabletClient::TRetryingRemoteDynamicStoreReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::TTrafficMeterPtr trafficMeter,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    TCallback<IVersionedReaderPtr(NChunkClient::NProto::TChunkSpec)> chunkReaderFactory);

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateRemoteOrderedDynamicStoreReader(
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    NTabletClient::TRemoteDynamicStoreReaderConfigPtr config,
    NTableClient::TChunkReaderOptionsPtr options,
    TNameTablePtr nameTable,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::TTrafficMeterPtr trafficMeter,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const std::optional<std::vector<TString>>& columns);

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateRetryingRemoteOrderedDynamicStoreReader(
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TTableSchemaPtr schema,
    NTabletClient::TRetryingRemoteDynamicStoreReaderConfigPtr config,
    NTableClient::TChunkReaderOptionsPtr options,
    TNameTablePtr nameTable,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::TTrafficMeterPtr trafficMeter,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const std::optional<std::vector<TString>>& columns,
    NChunkClient::TChunkReaderMemoryManagerPtr readerMemoryManager,
    TCallback<TFuture<ISchemalessChunkReaderPtr>(
        NChunkClient::NProto::TChunkSpec,
        NChunkClient::TChunkReaderMemoryManagerPtr)> chunkReaderFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT:NTabletClient
