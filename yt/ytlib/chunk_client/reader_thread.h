#pragma once

#include "common.h"

#include "../misc/lazy_ptr.h"
#include "../actions/action_queue.h"

namespace NYT {

/*!
 * This thread is used for background operations in 
 * #TSequentialChunkReader, #TTableChunkReader and #TableReader
 */
extern TLazyPtr<TActionQueue> ReaderThread;

} // namespace NYT

