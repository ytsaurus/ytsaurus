#pragma once

#include "public.h"

#include <yt/core/misc/ref.h>
#include <yt/core/misc/range.h>

#include <vector>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// XXX:columnar: merge with NApi?
struct IUnversionedRowBatch
    : public virtual TRefCounted
{
    //! Returns the number of rows in the batch.
    //! This call is cheap (in contrast to #GetRows).
    virtual int GetRowCount() const = 0;

    //! A helper method that returns |true| iff #GetRowCount is zero.
    bool IsEmpty() const;

    //! Returns the rows representing the batch.
    //! If the batch is columnar then the rows are materialized on first
    //! call to #MaterializeRows. This call could be heavy.
    virtual TRange<TUnversionedRow> MaterializeRows() = 0;

    //! Returns |true| if the batch is internally represeted by a set of columns.
    //! In this case #GetColumns provides access to columnar data.
    virtual bool IsColumnar() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUnversionedRowBatch)

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowBatchPtr CreateBatchFromUnversionedRows(
    TRange<TUnversionedRow> rows,
    TIntrusivePtr<TRefCounted> holder);

IUnversionedRowBatchPtr CreateBatchFromUnversionedRows(
    std::vector<TUnversionedRow>&& rows,
    TIntrusivePtr<TRefCounted> holder);

IUnversionedRowBatchPtr CreateEmptyUnversionedRowBatch();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
