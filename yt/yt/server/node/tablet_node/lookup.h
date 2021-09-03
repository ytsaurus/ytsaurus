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
    bool useLookupCache,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    NTableClient::TWireProtocolReader* reader,
    NTableClient::TWireProtocolWriter* writer);

void VersionedLookupRows(
    const TTabletSnapshotPtr& tabletSnapshot,
    TTimestamp timestampRange,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const NTableClient::TRetentionConfigPtr& retentionConfig,
    NTableClient::TWireProtocolReader* reader,
    NTableClient::TWireProtocolWriter* writer);

void LookupRead(
    const TTabletSnapshotPtr& tabletSnapshot,
    NTabletClient::TReadTimestampRange timestampRange,
    bool useLookupCache,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const NTableClient::TRetentionConfigPtr& retentionConfig,
    NTableClient::TWireProtocolReader* reader,
    NTableClient::TWireProtocolWriter* writer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
