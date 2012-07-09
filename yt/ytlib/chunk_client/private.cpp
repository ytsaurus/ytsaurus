#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkReaderLogger("ChunkReader");
NLog::TLogger ChunkWriterLogger("ChunkWriter");

// One queue (1) is for compression tasks and another one (0) is for 
// serving RemoteWriter RPC requests and responses.
TLazyPtr<TMultiActionQueue> WriterThread(TMultiActionQueue::CreateFactory(2, "ChunkWriter"));

TLazyPtr<TActionQueue> ReaderThread(TActionQueue::CreateFactory("ChunkReader"));

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

