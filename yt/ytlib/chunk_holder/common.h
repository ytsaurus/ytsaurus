#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/chunk_server/id.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkHolderLogger;

using NChunkServer::TChunkId;
using NChunkServer::TJobId;
using NChunkServer::EJobType;
using NChunkServer::EJobState;
using NChunkServer::TBlockId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
