#pragma once

#include "public.h"
#include "key.h"

#include <ytlib/misc/sync.h>

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISyncReader 
    : public virtual TRefCounted
{
    //! Called to initialize the reader.
    virtual void Open() = 0;

    //! Returns True if end-of-stream is not reached yet and thus
    //! calling #GetRow returns a valid current row.
    virtual bool IsValid() const = 0;

    //! Returns the current row.
    virtual const TRow& GetRow() const = 0;

    //! Returns the key of the current row.
    //! Not all implementations support this call.
    virtual const TNonOwningKey& GetKey() const = 0;

    //! Must be called after the row returned from #GetRow has been examines
    //! and another one is needed.
    virtual void NextRow() = 0;

    virtual i64 GetRowIndex() const = 0;
    virtual i64 GetRowCount() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

template <class TAsyncReader>
class TSyncReaderAdapter 
    : public ISyncReader
{
public:
    explicit TSyncReaderAdapter(TIntrusivePtr<TAsyncReader> asyncReader)
        : AsyncReader(asyncReader)
    { }

    virtual void Open() override
    {
        Sync(~AsyncReader, &TAsyncReader::AsyncOpen);
    }

    virtual void NextRow() override
    {
        if (!AsyncReader->FetchNextItem()) {
            Sync(~AsyncReader, &TAsyncReader::GetReadyEvent);
        }
    }

    virtual bool IsValid() const override
    {
        return AsyncReader->IsValid();
    }

    virtual const TRow& GetRow() const override
    {
        return AsyncReader->CurrentReader()->GetRow();
    }

    virtual const TNonOwningKey& GetKey() const override
    {
        return AsyncReader->CurrentReader()->GetKey();
    }

    virtual i64 GetRowIndex() const
    {
        return AsyncReader->GetItemIndex();
    }

    virtual i64 GetRowCount() const
    {
        return AsyncReader->GetItemCount();
    }

    /*
    const NYTree::TYsonString& GetRowAttributes() const
    {
        return AsyncReader->GetRowAttributes();
    }*/

private:
    TIntrusivePtr<TAsyncReader> AsyncReader;

};

////////////////////////////////////////////////////////////////////////////////

template <class TAsyncReader>
ISyncReaderPtr CreateSyncReader(TIntrusivePtr<TAsyncReader> asyncReader)
{
    return New< TSyncReaderAdapter<TAsyncReader> >(asyncReader);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
