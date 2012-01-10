#pragma once

#include "../misc/common.h"
#include "../misc/configurable.h"
#include "../logging/log.h"

#include "../chunk_client/common.h"
#include "../transaction_server/common.h"

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

struct TChunkManagerConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TChunkManagerConfig> TPtr;

    TDuration HolderLeaseTimeout;

    TChunkManagerConfig()
    {
        Register("holder_lease_timeout", HolderLeaseTimeout).Default(TDuration::Seconds(10));
    }
};

// TODO: make configurable
extern int MaxReplicationFanOut;
extern int MaxReplicationFanIn;
extern int MaxRemovalJobsPerHolder;
extern TDuration ChunkRefreshDelay;
extern TDuration ChunkRefreshQuantum;
extern int MaxChunksPerRefresh;
extern double MinChunkBalancingFillCoeffDiff;
extern double MinChunkBalancingFillCoeff;
extern double MaxHolderFillCoeff;
extern i64 MinHolderFreeSpace;
extern double ActiveSessionsPenalityCoeff;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

