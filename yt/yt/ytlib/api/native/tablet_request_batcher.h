#pragma once

#include "public.h"

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/wire_protocol.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TTabletRequestBatcherOptions
{
    std::optional<i64> MaxRowsPerBatch;
    std::optional<i64> MaxDataWeightPerBatch;
    std::optional<i64> MaxRowsPerTablet;
};

struct TUnversionedSubmittedRow
{
    NTableClient::EWireProtocolCommand Command;
    NTableClient::TUnversionedRow Row;
    NTableClient::TLockMask Locks;
    int SequentialId;
};

////////////////////////////////////////////////////////////////////////////////

struct ITabletRequestBatcher
    : public TRefCounted
{
    virtual void SubmitUnversionedRow(
        NTableClient::EWireProtocolCommand command,
        NTableClient::TUnversionedRow row,
        NTableClient::TLockMask lockMask) = 0;

    virtual void SubmitVersionedRow(
        NTableClient::TTypeErasedRow row) = 0;

    struct TBatch
    {
        std::unique_ptr<NTableClient::IWireProtocolWriter> Writer = NTableClient::CreateWireProtocolWriter();
        TSharedRef RequestData;
        i64 RowCount = 0;
        i64 DataWeight = 0;

        // COMPAT(ponasenko-rs)
        bool HasSharedWriteLocks = false;

        void Materialize(NCompression::ECodec codec);
    };
    struct TBatchesAndMergedRows
    {
        std::vector<std::unique_ptr<TBatch>> Batches;
        TSharedRange<TUnversionedSubmittedRow> MergedRows;
    };
    virtual TBatchesAndMergedRows PrepareBatches() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletRequestBatcher)

////////////////////////////////////////////////////////////////////////////////

ITabletRequestBatcherPtr CreateTabletRequestBatcher(
    NTabletClient::TTabletId tabletId,
    TTabletRequestBatcherOptions options,
    NTableClient::TTableSchemaPtr tableSchema,
    NQueryClient::TColumnEvaluatorPtr columnEvaluator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
