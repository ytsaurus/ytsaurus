#pragma once

#include "public.h"

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/actions/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Executes a bunch of row lookup requests. Request parameters are parsed via #reader,
//! response is written into #writer.
void LookupRows(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    const TString& user,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    NTabletClient::TWireProtocolReader* reader,
    NTabletClient::TWireProtocolWriter* writer);

void VersionedLookupRows(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    const TString& user,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    NTableClient::TRetentionConfigPtr retentionConfig,
    NTabletClient::TWireProtocolReader* reader,
    NTabletClient::TWireProtocolWriter* writer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
