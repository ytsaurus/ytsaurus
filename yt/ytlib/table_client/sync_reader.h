#pragma once

#include "public.h"
#include <ytlib/ytree/public.h>
#include <ytlib/misc/ref_counted.h>
#include <ytlib/misc/sync.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISyncReader 
    : public virtual TRefCounted
{
    virtual void Open() = 0;

    virtual void NextRow() = 0;
    virtual bool IsValid() const = 0;

    virtual const TRow& GetRow() = 0;
    //virtual const NYTree::TYsonString& GetRowAttributes() const = 0;
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

    void Open()
    {
        Sync(~AsyncReader, &TAsyncReader::AsyncOpen);
    }

    void NextRow()
    {
        if (!AsyncReader->FetchNextItem()) {
            Sync(~AsyncReader, &TAsyncReader::GetReadyEvent);
        }
    }

    bool IsValid() const
    {
        return AsyncReader->IsValid();
    }

    const TRow& GetRow()
    {
        return AsyncReader->GetRow();
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
