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

    /*! 
     *  \param key - is used only if table is sorted, e.g. GetKeyColumns
     *  returns not null.
     */
    virtual void WriteRow(TRow& row, TKey& key) = 0;
    virtual void Close() = 0;

    virtual const TNullable<TKeyColumns>& GetKeyColumns() const = 0;

    //! Current row count.
    virtual i64 GetRowCount() const = 0;

    virtual TKey& GetLastKey() = 0;

};

////////////////////////////////////////////////////////////////////////////////

class TSyncWriterAdapter
    : public ISyncWriter
{
public:
    TSyncWriterAdapter(IAsyncWriterPtr writer);

    void Open();
    void WriteRow(TRow& row, TKey& key);
    void Close();

    const TNullable<TKeyColumns>& GetKeyColumns() const;

    i64 GetRowCount() const;
    TKey& GetLastKey();

private:
    IAsyncWriterPtr Writer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
