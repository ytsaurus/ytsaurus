#pragma once

#include <core/misc/common.h>

#include <ytlib/chunk_client/public.h>

#include <server/chunk_server/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

using NChunkClient::TChunkId;
using NChunkClient::EWriteSessionType;

using NChunkServer::TBlockId;

using NNodeTrackerClient::TNodeId;

////////////////////////////////////////////////////////////////////////////////

struct TChunkDescriptor;

DECLARE_REFCOUNTED_CLASS(TMasterConnector)

DECLARE_REFCOUNTED_CLASS(TChunkStore)
DECLARE_REFCOUNTED_CLASS(TChunkCache)
DECLARE_REFCOUNTED_CLASS(TChunkRegistry)

DECLARE_REFCOUNTED_CLASS(TBlockStore)
DECLARE_REFCOUNTED_CLASS(TReaderCache)

DECLARE_REFCOUNTED_CLASS(TLocation)

DECLARE_REFCOUNTED_STRUCT(IChunk)
DECLARE_REFCOUNTED_CLASS(TCachedBlock)
DECLARE_REFCOUNTED_CLASS(TStoredBlobChunk)
DECLARE_REFCOUNTED_CLASS(TCachedBlobChunk)

DECLARE_REFCOUNTED_CLASS(TRefCountedChunkMeta)

DECLARE_REFCOUNTED_CLASS(TSessionManager)
DECLARE_REFCOUNTED_CLASS(TSession)

DECLARE_REFCOUNTED_CLASS(TPeerBlockTable)
DECLARE_REFCOUNTED_CLASS(TPeerBlockUpdater)

DECLARE_REFCOUNTED_CLASS(TDiskHealthChecker)

DECLARE_REFCOUNTED_CLASS(TPeerBlockTableConfig)
DECLARE_REFCOUNTED_CLASS(TLocationConfig)
DECLARE_REFCOUNTED_CLASS(TDiskHealthCheckerConfig)
DECLARE_REFCOUNTED_CLASS(TDataNodeConfig)
DECLARE_REFCOUNTED_CLASS(TPeerBlockTableConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
