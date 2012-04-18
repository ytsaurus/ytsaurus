#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkClientLogger("ChunkClient");

TLazyPtr<TActionQueue> WriterThread(TActionQueue::CreateFactory("ChunkWriter"));

TLazyPtr<TActionQueue> ReaderThread(TActionQueue::CreateFactory("ChunkReader"));

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

