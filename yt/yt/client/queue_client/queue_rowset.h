#pragma once

#include "public.h"

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

struct TQueueRowBatchReadOptions
{
    i64 MaxRowCount = 1000;
    //! Currently only used if DataWeightPerRowHint is set.
    i64 MaxDataWeight = 16_MB;

    //! If set, this value is used to compute the number of rows to read considering the given MaxDataWeight.
    std::optional<i64> DataWeightPerRowHint;
};

////////////////////////////////////////////////////////////////////////////////

void VerifyRowBatchReadOptions(const TQueueRowBatchReadOptions& options);

i64 ComputeRowsToRead(const TQueueRowBatchReadOptions& options);

i64 GetStartOffset(const NApi::IUnversionedRowsetPtr& rowset);

////////////////////////////////////////////////////////////////////////////////

class TQueueRowset
    : public NApi::IUnversionedRowset
{
public:
    TQueueRowset(NApi::IUnversionedRowsetPtr rowset, i64 startOffset);

    const NTableClient::TTableSchemaPtr& GetSchema() const override;
    const NTableClient::TNameTablePtr& GetNameTable() const override;

    TRange<NTableClient::TUnversionedRow> GetRows() const override;

    TSharedRange<NTableClient::TUnversionedRow> GetSharedRange() const override;

    i64 GetStartOffset() const;
    i64 GetFinishOffset() const;

private:
    const NApi::IUnversionedRowsetPtr Rowset_;
    const i64 StartOffset_;
};

DEFINE_REFCOUNTED_TYPE(TQueueRowset)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
