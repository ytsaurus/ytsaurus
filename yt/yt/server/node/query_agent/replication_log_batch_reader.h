#pragma once

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/client/table_client/public.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

struct IReplicationLogBatchFetcher
{
    virtual ~IReplicationLogBatchFetcher() = default;
    virtual NTableClient::IUnversionedRowBatchPtr ReadNextRowBatch(i64 currentRowIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TReplicationLogBatchDescriptor
{
    i64 ReadRowCount = 0;
    i64 ResponseRowCount = 0;
    i64 ResponseDataWeight = 0;
    NTransactionClient::TTimestamp MaxTimestamp = 0;
    bool ReadAllRows = true;
    i64 EndReplicationRowIndex = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationLogBatchReaderBase
{
public:
    TReplicationLogBatchReaderBase(
        NTabletNode::TTableMountConfigPtr mountConfig,
        NTabletClient::TTabletId tabletId,
        NLogging::TLogger logger);

    virtual ~TReplicationLogBatchReaderBase() = default;

    TReplicationLogBatchDescriptor ReadReplicationBatch(
        i64 startRowIndex,
        NTransactionClient::TTimestamp upperTimestamp,
        i64 maxDataWeight);

protected:
    const NTabletNode::TTableMountConfigPtr TableMountConfig_;
    const NTabletClient::TTabletId TabletId_;
    const NLogging::TLogger Logger;

    virtual NTableClient::TColumnFilter CreateColumnFilter() const;

    virtual NTableClient::TLegacyOwningKey MakeBoundKey(i64 currentRowIndex) const = 0;

    virtual std::unique_ptr<IReplicationLogBatchFetcher> MakeBatchFetcher(
        NTableClient::TLegacyOwningKey lower,
        NTableClient::TLegacyOwningKey upper,
        const NTableClient::TColumnFilter& columnFilter) const = 0;

    virtual bool ToTypeErasedRow(
        const NTableClient::TUnversionedRow& row,
        const NTableClient::TRowBufferPtr& rowBuffer,
        NTableClient::TTypeErasedRow* replicationRow,
        NTableClient::TTimestamp* timestamp,
        i64* rowDataWeight) const = 0;

    virtual void WriteTypeErasedRow(NTableClient::TTypeErasedRow row) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
