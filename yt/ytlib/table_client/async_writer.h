#pragma once

#include "public.h"
#include "key.h"

#include <ytlib/misc/ref_counted.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/error.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter
    : public virtual TRefCounted
{
    virtual TAsyncError AsyncOpen() = 0;

    /*!
     *  Both parameters could be modified.
     *  Sort order of written rows is not validated.
     */
    virtual TAsyncError AsyncWriteRow(TRow& row, TKey& key) = 0;
    virtual TAsyncError AsyncClose() = 0;

    /*! 
     *  Returns non-const reference on the internal key field.
     *  One can swap it out to avoid unnecessary copying, but you 
     *  should never want to do it before calling #AsyncClose.
     */
    virtual TKey& GetLastKey() = 0;

    virtual const TNullable<TKeyColumns>& GetKeyColumns() const = 0;

    //! Current row count.
    virtual i64 GetRowCount() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
