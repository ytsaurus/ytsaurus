#include "stdafx.h"

#include "sync_reader.h"
#include "async_reader.h"

#include <ytlib/misc/sync.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TSyncReader::TSyncReader(IAsyncReader* asyncReader)
    : AsyncReader(asyncReader)
{ }

void TSyncReader::Open()
{
    Sync(AsyncReader.Get(), &IAsyncReader::AsyncOpen);
}

void TSyncReader::NextRow()
{
    Sync(AsyncReader.Get(), &IAsyncReader::AsyncNextRow);
}

bool TSyncReader::IsValid() const
{
    return AsyncReader->IsValid();
}

const TRow& TSyncReader::GetRow() const
{
    return AsyncReader->GetCurrentRow();
}

const TKey& TSyncReader::GetKey() const
{
    return AsyncReader->GetCurrentKey();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
