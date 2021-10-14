#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

i64 GetLogRowIndex(NTableClient::TUnversionedRow logRow);
TTimestamp GetLogRowTimestamp(NTableClient::TUnversionedRow logRow);

TUnversionedRow BuildLogRow(
    NTableClient::TUnversionedRow row,
    NApi::ERowModificationType changeType,
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
};

DEFINE_REFCOUNTED_TYPE(IReplicationLogParser)

IReplicationLogParserPtr CreateReplicationLogParser(
    NTableClient::TTableSchemaPtr tableSchema,
    TTableMountConfigPtr mountConfig,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
