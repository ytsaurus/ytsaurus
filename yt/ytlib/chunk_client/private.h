#pragma once

#include <ytlib/misc/lazy_ptr.h>

#include <ytlib/actions/action_queue.h>

#include <ytlib/logging/log.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkReaderLogger;
extern NLog::TLogger ChunkWriterLogger;

////////////////////////////////////////////////////////////////////////////////

/*!
 * This thread is used for background operations in #TRemoteChunkReader
 * #TSequentialChunkReader, #TTableChunkReader and #TableReader
 */
extern TLazyPtr<TActionQueue> ReaderThread;

/*!
 *  This thread is used for background operations in 
 *  #TRemoteChunkWriter, #NTableClient::TChunkWriter and 
 *  #NTableClient::TChunkSetReader
 */
extern TLazyPtr<TActionQueue> WriterThread;

extern TLazyPtr<TThreadPool> CompressionThreadPool;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

