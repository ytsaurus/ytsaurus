#pragma once

#include "public.h"
#include "key.h"

#include <ytlib/misc/sync.h>
#include <ytlib/misc/nullable.h>

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISyncReader 
    : public virtual TRefCounted
{
    //! Called to initialize the reader.
    virtual void Open() = 0;

    //! Returns the current row.
    virtual const TRow* GetRow() = 0;

    //! Returns the key of the current row.
    //! Not all implementations support this call.
    virtual const TNonOwningKey& GetKey() const = 0;

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
        , IsReadingStarted(false)
    { }

    virtual void Open() override
    {
        Sync(~AsyncReader, &TAsyncReader::AsyncOpen);
    }

    virtual const TRow* GetRow() override
    {
        if (IsReadingStarted && AsyncReader->IsValid()) {
            if (!AsyncReader->FetchNextItem()) {
                Sync(~AsyncReader, &TAsyncReader::GetReadyEvent);
            }
        }
        IsReadingStarted = true;
        return AsyncReader->IsValid() ? &AsyncReader->CurrentReader()->GetRow() : NULL;
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

    bool IsReadingStarted;
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
