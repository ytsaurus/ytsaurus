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
    virtual TAsyncError AsyncWriteRow(const TRow& row, const TKey& key = TKey()) = 0;
    virtual TAsyncError AsyncClose() = 0;

    virtual const TNullable<TKeyColumns>& GetKeyColumns() const = 0;

    //! Current row count.
    virtual i64 GetRowCount() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
