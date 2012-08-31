#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkReaderLogger("ChunkReader");
NLog::TLogger ChunkWriterLogger("ChunkWriter");

// One queue (1) is for compression tasks and another one (0) is for
// serving RemoteWriter RPC requests and responses.

TLazyPtr<TActionQueue> WriterThread(TActionQueue::CreateFactory("ChunkWriter"));

TLazyPtr<TActionQueue> ReaderThread(TActionQueue::CreateFactory("ChunkReader"));

TLazyPtr<TThreadPool> CompressionThreadPool(TThreadPool::CreateFactory(8, "Compression"));

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

