#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/logging/log.h>

#include <ytlib/chunk_client/common.h>
#include <ytlib/transaction_server/common.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

typedef i32 THolderId;
const i32 InvalidHolderId = -1;

typedef TGuid TChunkListId;
extern TChunkListId NullChunkListId;

using NChunkClient::TChunkId;
using NChunkClient::NullChunkId;

using NTransactionServer::TTransactionId;
using NTransactionServer::NullTransactionId;

typedef TGuid TChunkTreeId;
extern TChunkTreeId NullChunkTreeId;

extern ui64 ChunkIdSeed;
extern ui64 ChunkListIdSeed;

DECLARE_ENUM(EChunkTreeKind,
    (Chunk)
    (ChunkList)
);

EChunkTreeKind GetChunkTreeKind(const TChunkTreeId& treeId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
