#pragma once

#include "public.h"
#include "key.h"

#include <ytlib/misc/ref_counted.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/error.h>
#include <ytlib/misc/blob_output.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter
    : public virtual TRefCounted
{
    //! Opens the writer.
    virtual TAsyncError AsyncOpen() = 0;

    //! Appends a new row.
    /*!
     *  Both parameters could be modified.
     *  Sort order of rows is not validated.
     */
    virtual TAsyncError AsyncWriteRow(TRow& row, const TKey<TFakeStrbufStore>& key) = 0;

    //! Closes the writer.
    virtual TAsyncError AsyncClose() = 0;

    //! Returns the last key added to the writer.
    /*! 
     *  Returns non-const reference on the internal key field.
     *  One can swap it out to avoid excessive copying after (!)
     *  the writer is closed by calling #AsyncClose.
     */
    virtual const TKey<TBlobOutput>& GetLastKey() = 0;

    //! Returns key column names if rows are added in ``sorted'' mode
    //! or |Null| otherwise.
    virtual const TNullable<TKeyColumns>& GetKeyColumns() const = 0;

    //! Returns the current row count.
    virtual i64 GetRowCount() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
