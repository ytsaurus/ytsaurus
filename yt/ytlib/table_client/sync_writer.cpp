#include "stdafx.h"

#include "sync_writer.h"
#include "async_writer.h"

#include <ytlib/misc/sync.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TSyncWriterAdapter::TSyncWriterAdapter(IAsyncWriterPtr writer)
    : Writer(writer)
{ }

void TSyncWriterAdapter::Open()
{
    Sync(~Writer, &IAsyncWriter::AsyncOpen);
}

void TSyncWriterAdapter::WriteRow(TRow& row, const TNonOwningKey& key)
{
    Sync(~Writer, &IAsyncWriter::AsyncWriteRow, row, key);
}

void TSyncWriterAdapter::Close()
{
    Sync(~Writer, &IAsyncWriter::AsyncClose);
}

const TNullable<TKeyColumns>& TSyncWriterAdapter::GetKeyColumns() const
{
    return Writer->GetKeyColumns();
}

i64 TSyncWriterAdapter::GetRowCount() const
{
    return Writer->GetRowCount();
}

const TOwningKey& TSyncWriterAdapter::GetLastKey() const
{
    return Writer->GetLastKey();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
