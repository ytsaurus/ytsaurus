#pragma once

#include "common.h"

#include <ytlib/misc/lazy_ptr.h>
#include <ytlib/actions/action_queue.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

/*!
 * This thread is used for background operations in #TRemoteChunkReader
 * #TSequentialChunkReader, #TTableChunkReader and #TableReader
 */
extern TLazyPtr<TActionQueue> ReaderThread;

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

