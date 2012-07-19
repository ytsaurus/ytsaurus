#include "stdafx.h"

#include "sync_reader.h"
#include "async_reader.h"

#include <ytlib/misc/sync.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TAsyncReader>
ISyncReaderPtr CreateSyncReader(TIntrusivePtr<TAsyncReader> asyncReader)
{
    return New< TSyncReaderAdapter<TAsyncReader> >(asyncReader);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
