#pragma once

#include <ytlib/misc/common.h>

#include <ytlib/object_client/public.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/block_id.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

using NChunkClient::TIncarnationId;
using NChunkClient::TChunkId;
using NChunkClient::TChunkListId;
using NChunkClient::TChunkTreeId;
using NChunkClient::NullChunkId;
using NChunkClient::NullChunkListId;
using NChunkClient::NullChunkTreeId;
using NChunkClient::TJobId;
using NChunkClient::EJobState;
using NChunkClient::EJobType;
using NChunkClient::TBlockOffset;
using NChunkClient::EChunkType;
using NChunkClient::TBlockId;

////////////////////////////////////////////////////////////////////////////////

class TChunk;  
class TChunkList;
class TJob;
class TJobList;
class TDataNode;
class TReplicationSink;

struct TVersionedChunkListId;
struct TChunkTreeStatistics;
struct TTotalNodeStatistics;

class TChunkTreeRef;

class TChunkManager;
typedef TIntrusivePtr<TChunkManager> TChunkManagerPtr;

struct INodeAuthority;
typedef TIntrusivePtr<INodeAuthority> INodeAuthorityPtr;

class TNodeLeaseTracker;
typedef TIntrusivePtr<TNodeLeaseTracker> TNodeLeaseTrackerPtr;

class TChunkReplicator;
typedef TIntrusivePtr<TChunkReplicator> TChunkReplicatorPtr;

class TChunkPlacement;
typedef TIntrusivePtr<TChunkPlacement> TChunkPlacementPtr;

class TChunkService;
typedef TIntrusivePtr<TChunkService> TChunkServicePtr;

struct TChunkReplicatorConfig;
typedef TIntrusivePtr<TChunkReplicatorConfig> TChunkReplicatorConfigPtr;

struct TChunkManagerConfig;
typedef TIntrusivePtr<TChunkManagerConfig> TChunkManagerConfigPtr;

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

typedef i32 TNodeId;
const TNodeId InvalidNodeId = -1;

//! Used as an expected upper bound in TSmallVector.
const int TypicalReplicationFactor = 4;

//! The number of supported replication priorities.
//! The smaller the more urgent.
/*! current RF == 1 -> priority = 0
 *  current RF == 2 -> priority = 1
 *  current RF >= 3 -> priority = 2
 */
const int ReplicationPriorities = 3;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
