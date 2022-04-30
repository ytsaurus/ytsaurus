#pragma once

#include "public.h"

#include <yt/yt/ytlib/tablet_client/public.h>
#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Executes a bunch of row lookup requests. Request parameters are parsed via #reader,
//! response is written into #writer.
void LookupRows(
    const TTabletSnapshotPtr& tabletSnapshot,
    NTabletClient::TReadTimestampRange timestampRange,
    std::optional<bool> useLookupCache,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    NTableClient::IWireProtocolReader* reader,
    NTableClient::IWireProtocolWriter* writer);

void VersionedLookupRows(
    const TTabletSnapshotPtr& tabletSnapshot,
    TTimestamp timestamp,
    std::optional<bool> useLookupCache,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const NTableClient::TRetentionConfigPtr& retentionConfig,
    NTableClient::IWireProtocolReader* reader,
    NTableClient::IWireProtocolWriter* writer);

void LookupRead(
    const TTabletSnapshotPtr& tabletSnapshot,
    NTabletClient::TReadTimestampRange timestampRange,
    std::optional<bool> useLookupCache,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const NTableClient::TRetentionConfigPtr& retentionConfig,
    NTableClient::IWireProtocolReader* reader,
    NTableClient::IWireProtocolWriter* writer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
