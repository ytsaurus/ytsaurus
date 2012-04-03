#include "stdafx.h"

#include "sync_reader.h"
#include "async_reader.h"

#include <ytlib/misc/sync.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TSyncReaderAdapter::TSyncReaderAdapter(IAsyncReader* asyncReader)
    : AsyncReader(asyncReader)
{ }

void TSyncReaderAdapter::Open()
{
    Sync(AsyncReader.Get(), &IAsyncReader::AsyncOpen);
}

void TSyncReaderAdapter::NextRow()
{
    Sync(AsyncReader.Get(), &IAsyncReader::AsyncNextRow);
}

bool TSyncReaderAdapter::IsValid() const
{
    return AsyncReader->IsValid();
}

const TRow& TSyncReaderAdapter::GetRow() const
{
    return AsyncReader->GetCurrentRow();
}

const TKey& TSyncReaderAdapter::GetKey() const
{
    return AsyncReader->GetCurrentKey();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
