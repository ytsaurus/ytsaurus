#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/object_server/id.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunk;  
class TChunkList;
class TJob;
class TJobList;
class THolder;
class TReplicationSink;

struct TChunkTreeStatistics;
struct TTotalHolderStatistics;

class TChunkTreeRef;

class TChunkManager;
typedef TIntrusivePtr<TChunkManager> TChunkManagerPtr;

struct IHolderAuthority;
typedef TIntrusivePtr<IHolderAuthority> IHolderAuthorityPtr;

class THolderLeaseTracker;
typedef TIntrusivePtr<THolderLeaseTracker> THolderLeaseTrackerPtr;

class TJobScheduler;
typedef TIntrusivePtr<TJobScheduler> TJobSchedulerPtr;

class TChunkPlacement;
typedef TIntrusivePtr<TChunkPlacement> TChunkPlacementPtr;

class TChunkService;
typedef TIntrusivePtr<TChunkService> TChunkServicePtr;

struct TJobSchedulerConfig;
typedef TIntrusivePtr<TJobSchedulerConfig> TJobSchedulerConfigPtr;

struct TChunkManagerConfig;
typedef TIntrusivePtr<TChunkManagerConfig> TChunkManagerConfigPtr;

using NObjectServer::TTransactionId;
using NObjectServer::NullTransactionId;

typedef i32 THolderId;
const i32 InvalidHolderId = -1;

typedef TGuid TIncarnationId;

typedef NObjectServer::TObjectId TChunkId;
extern TChunkId NullChunkId;

typedef NObjectServer::TObjectId TChunkListId;
extern TChunkListId NullChunkListId;

typedef NObjectServer::TObjectId TChunkTreeId;
extern TChunkTreeId NullChunkTreeId;

typedef TGuid TJobId;

DECLARE_ENUM(EJobState,
    (Running)
    (Completed)
    (Failed)
);

DECLARE_ENUM(EJobType,
    (Replicate)
    (Remove)
);

//! Represents an offset inside a chunk.
typedef i64 TBlockOffset;

//! A |(chunkId, blockIndex)| pair.
struct TBlockId;

DECLARE_ENUM(EChunkType,
    ((Unknown)(0))
    ((File)(1))
    ((Table)(2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNChunkServer
} // namespace NYT
