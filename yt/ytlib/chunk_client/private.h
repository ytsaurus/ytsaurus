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

} // namespace NChunkClient
} // namespace NYT

