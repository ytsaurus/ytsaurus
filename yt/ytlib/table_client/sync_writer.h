#pragma once

#include "public.h"
#include "async_writer.h"

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <core/misc/ref_counted.h>
#include <core/misc/nullable.h>
#include <core/misc/sync.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISyncWriter
    : public IWriterBase
{
    virtual void Close() = 0;
};

//////////////////////////////////////////////////////////////////////////////

struct ISyncWriterUnsafe
    : public ISyncWriter
{
    virtual void WriteRowUnsafe(const TRow& row) = 0;
    virtual void WriteRowUnsafe(const TRow& row, const NVersionedTableClient::TKey& key) = 0;

    virtual const std::vector<NChunkClient::NProto::TChunkSpec>& GetWrittenChunks() const = 0;

    virtual NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const= 0;

    virtual void SetProgress(double progress) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TProvider>
class TSyncWriterAdapter
    : public ISyncWriterUnsafe
{
public:
    typedef NChunkClient::TMultiChunkSequentialWriter<TProvider> TAsyncWriter;
    typedef TIntrusivePtr<TAsyncWriter> TAsyncWriterPtr;

    TSyncWriterAdapter(TAsyncWriterPtr writer)
        : Writer(writer)
        , IsOpen(false)
    { }

    inline void EnsureOpen()
    {
        if (!IsOpen) {
            Sync(Writer.Get(), &TAsyncWriter::Open);
            IsOpen = true;
        }
    }

    virtual void WriteRow(const TRow& row) override
    {
        EnsureOpen();
        GetCurrentWriter()->WriteRow(row);
    }

    virtual void WriteRowUnsafe(const TRow& row) override
    {
        EnsureOpen();
        GetCurrentWriter()->WriteRowUnsafe(row);
    }

    virtual void WriteRowUnsafe(const TRow& row, const NVersionedTableClient::TKey& key) override
    {
        EnsureOpen();
        GetCurrentWriter()->WriteRowUnsafe(row, key);
    }

    virtual void Close() override
    {
        Sync(Writer.Get(), &TAsyncWriter::Close);
    }

    virtual const TNullable<TKeyColumns>& GetKeyColumns() const override
    {
        return Writer->GetProvider()->GetKeyColumns();
    }

    virtual i64 GetRowCount() const override
    {
        return Writer->GetProvider()->GetRowCount();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return Writer->GetProvider()->GetDataStatistics();
    }

    virtual const std::vector<NChunkClient::NProto::TChunkSpec>& GetWrittenChunks() const override
    {
        return Writer->GetWrittenChunks();
    }

    virtual NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const override
    {
        return Writer->GetNodeDirectory();
    }

     virtual void SetProgress(double progress)
     {
        Writer->SetProgress(progress);
     }

private:
    typename TProvider::TFacade* GetCurrentWriter()
    {
        typename TProvider::TFacade* facade = nullptr;

        while ((facade = Writer->GetCurrentWriter()) == nullptr) {
            Sync(Writer.Get(), &TAsyncWriter::GetReadyEvent);
        }
        return facade;
    }

    TAsyncWriterPtr Writer;
    bool IsOpen;

};

////////////////////////////////////////////////////////////////////////////////

template <class TProvider>
ISyncWriterUnsafePtr CreateSyncWriter(
    typename TSyncWriterAdapter<TProvider>::TAsyncWriterPtr asyncWriter)
{
    return New< TSyncWriterAdapter<TProvider> >(asyncWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
