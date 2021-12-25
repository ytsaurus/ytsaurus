#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

i64 GetLogRowIndex(NTableClient::TUnversionedRow logRow);
TTimestamp GetLogRowTimestamp(NTableClient::TUnversionedRow logRow);

TLegacyOwningKey MakeRowBound(i64 rowIndex);

TUnversionedRow BuildLogRow(
    NTableClient::TUnversionedRow row,
    NApi::ERowModificationType changeType,
    const NTableClient::TTableSchemaPtr& tableSchema,
    NTableClient::TUnversionedRowBuilder* rowBuilder);

TUnversionedRow BuildLogRow(
    NTableClient::TVersionedRow row,
    const NTableClient::TTableSchemaPtr& tableSchema,
    NTableClient::TUnversionedRowBuilder* rowBuilder);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IReplicationLogParser)

struct IReplicationLogParser
    : public TRefCounted
{
    virtual std::optional<int> GetTimestampColumnId() = 0;

    virtual void ParseLogRow(
        const TTabletSnapshotPtr& tabletSnapshot,
        NTableClient::TUnversionedRow logRow,
        const NTableClient::TRowBufferPtr& rowBuffer,
        NTableClient::TTypeErasedRow* replicationRow,
        NApi::ERowModificationType* modificationType,
        i64* rowIndex,
        TTimestamp* timestamp,
        bool isVersioned) = 0;

    virtual i64 ComputeStartRowIndex(
        const TTabletSnapshotPtr& tabletSnapshot,
        NTransactionClient::TTimestamp startReplicationTimestamp,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions) = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicationLogParser)

IReplicationLogParserPtr CreateReplicationLogParser(
    NTableClient::TTableSchemaPtr tableSchema,
    TTableMountConfigPtr mountConfig,
    EWorkloadCategory workloadCategory,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
