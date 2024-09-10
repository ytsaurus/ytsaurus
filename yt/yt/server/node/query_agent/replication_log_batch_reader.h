#pragma once

#include <yt/yt/server/lib/tablet_node/public.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

struct IReplicationLogBatchFetcher
{
    virtual ~IReplicationLogBatchFetcher() = default;
    virtual NTableClient::IUnversionedRowBatchPtr ReadNextRowBatch(i64 currentRowIndex) = 0;
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

    void ReadReplicationBatch(
        i64* currentRowIndex,
        NTransactionClient::TTimestamp upperTimestamp,
        i64 maxDataWeight,
        i64* totalRowCount,
        i64* batchRowCount,
        i64* batchDataWeight,
        NTransactionClient::TTimestamp* maxTimestamp,
        bool* readAllRows);

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
        NTableClient::TTypeErasedRow* replicationRow,
        NTableClient::TTimestamp* timestamp,
        i64* rowDataWeight) const = 0;

    virtual void WriteTypeErasedRow(NTableClient::TTypeErasedRow row) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
