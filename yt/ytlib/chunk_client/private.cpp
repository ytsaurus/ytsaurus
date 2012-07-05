#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkReaderLogger("ChunkReader");
NLog::TLogger ChunkWriterLogger("ChunkWriter");

// The first queue is for compression tasks and the second one is for 
// serving RemoteWriter RPC requests and responses.
TLazyPtr<TActionQueue> WriterThread(TMultiActionQueue::CreateFactory(2, "ChunkWriter"));

TLazyPtr<TActionQueue> ReaderThread(TActionQueue::CreateFactory("ChunkReader"));

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

