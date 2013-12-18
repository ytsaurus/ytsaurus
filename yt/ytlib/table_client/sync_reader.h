#pragma once

#include "public.h"

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/chunk_client/data_statistics.h>

#include <core/misc/sync.h>
#include <core/misc/nullable.h>

#include <core/ytree/public.h>
#include <ytlib/chunk_client/public.h>

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
    virtual const NVersionedTableClient::TKey& GetKey() const = 0;

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const = 0;
    virtual int GetTableIndex() const = 0;
    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const = 0;

    virtual i64 GetSessionRowIndex() const = 0;
    virtual i64 GetSessionRowCount() const = 0;
    virtual i64 GetTableRowIndex() const = 0;
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
        if (IsReadingStarted && AsyncReader->GetFacade() != nullptr) {
            if (!AsyncReader->FetchNext()) {
                Sync(~AsyncReader, &TAsyncReader::GetReadyEvent);
            }
        }
        IsReadingStarted = true;
        auto* facade = AsyncReader->GetFacade();
        return facade ? &facade->GetRow() : nullptr;
    }

    virtual const NVersionedTableClient::TKey& GetKey() const override
    {
        return AsyncReader->GetFacade()->GetKey();
    }

    virtual i64 GetSessionRowIndex() const
    {
        return AsyncReader->GetProvider()->GetRowIndex();
    }

    virtual i64 GetSessionRowCount() const
    {
        return AsyncReader->GetProvider()->GetRowCount();
    }

    virtual i64 GetTableRowIndex() const
    {
        return AsyncReader->GetFacade()->GetTableRowIndex();
    }

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const
    {
        return AsyncReader->GetFailedChunkIds();
    }

    virtual int GetTableIndex() const override
    {
        return AsyncReader->GetFacade()->GetTableIndex();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return AsyncReader->GetProvider()->GetDataStatistics();
    }

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
