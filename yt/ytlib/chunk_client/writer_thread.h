#pragma once
#include "../misc/lazy_ptr.h"
#include "../actions/action_queue.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  This thread is used for background operations in 
 *  #TRemoteChunkWriter, #NTableClient::TChunkWriter and 
 *  #NTableClient::TChunkSetReader
 */
extern TLazyPtr<TActionQueue> WriterThread; // TODO: name this queue

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
