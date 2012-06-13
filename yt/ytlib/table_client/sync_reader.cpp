#include "stdafx.h"

#include "sync_reader.h"
#include "async_reader.h"

#include <ytlib/misc/sync.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TSyncReaderAdapter::TSyncReaderAdapter(IAsyncReaderPtr asyncReader)
    : AsyncReader(asyncReader)
{ }

void TSyncReaderAdapter::Open()
{
    Sync(~AsyncReader, &IAsyncReader::AsyncOpen);
}

void TSyncReaderAdapter::NextRow()
{
    Sync(~AsyncReader, &IAsyncReader::AsyncNextRow);
}

bool TSyncReaderAdapter::IsValid() const
{
    return AsyncReader->IsValid();
}

TRow& TSyncReaderAdapter::GetRow()
{
    return AsyncReader->GetRow();
}

const NYTree::TYsonString& TSyncReaderAdapter::GetRowAttributes() const
{
    return AsyncReader->GetRowAttributes();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
