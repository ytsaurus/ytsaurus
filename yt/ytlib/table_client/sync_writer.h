#pragma once

#include "public.h"
#include "key.h"

#include <ytlib/misc/ref_counted.h>
#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISyncWriter
    : public virtual TRefCounted
{
    virtual void Open() = 0;
    virtual void Close() = 0;

    /*! 
     *  \param key is used only if the table is sorted, e.g. GetKeyColumns
     *  returns not null.
     */
    virtual void WriteRow(TRow& row, const TNonOwningKey& key) = 0;

    //! Returns all key columns seen so far.
    virtual const TNullable<TKeyColumns>& GetKeyColumns() const = 0;

    //! Returns the current row count (starting from 0).
    virtual i64 GetRowCount() const = 0;

    //! Returns the last key written so far.
    virtual const TOwningKey& GetLastKey() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSyncWriterAdapter
    : public ISyncWriter
{
public:
    TSyncWriterAdapter(IAsyncWriterPtr writer);

    void Open();
    void WriteRow(TRow& row, const TNonOwningKey& key);
    void Close();

    const TNullable<TKeyColumns>& GetKeyColumns() const;

    i64 GetRowCount() const;
    const TOwningKey& GetLastKey() const;

private:
    IAsyncWriterPtr Writer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
