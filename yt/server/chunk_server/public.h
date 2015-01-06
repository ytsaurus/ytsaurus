#pragma once

#include <core/misc/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/block_id.h>

#include <ytlib/job_tracker_client/public.h>

#include <ytlib/node_tracker_client/public.h>

#include <server/node_tracker_server/public.h>

#include <map>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

using NChunkClient::TChunkId;
using NChunkClient::TChunkListId;
using NChunkClient::TChunkTreeId;
using NChunkClient::NullChunkId;
using NChunkClient::NullChunkListId;
using NChunkClient::NullChunkTreeId;
using NChunkClient::TBlockOffset;
using NChunkClient::EChunkType;
using NChunkClient::TBlockId;
using NChunkClient::TypicalReplicaCount;

using NJobTrackerClient::TJobId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::InvalidNodeId;
using NNodeTrackerClient::MaxNodeId;

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

using NNodeTrackerServer::TNode;
using NNodeTrackerServer::TNodeList;
using NNodeTrackerServer::TSortedNodeList;

////////////////////////////////////////////////////////////////////////////////

class TChunkTree;
class TChunkReplica;
class TChunk;
class TChunkList;
class TChunkOwnerBase;
class TDataNode;

template <class T>
class TPtrWithIndex;

struct TChunkTreeStatistics;
struct TTotalNodeStatistics;

DECLARE_REFCOUNTED_CLASS(TJob)
DECLARE_REFCOUNTED_CLASS(TJobList)

DECLARE_REFCOUNTED_CLASS(TChunkManager)
DECLARE_REFCOUNTED_CLASS(TChunkReplicator)
DECLARE_REFCOUNTED_CLASS(TChunkSealer)
DECLARE_REFCOUNTED_CLASS(TChunkPlacement)

DECLARE_REFCOUNTED_CLASS(TChunkManagerConfig)

//! Used as an expected upper bound in SmallVector.
const int TypicalChunkParentCount = 2;

//! The number of supported replication priorities.
//! The smaller the more urgent.
/*! current RF == 1 -> priority = 0
 *  current RF == 2 -> priority = 1
 *  current RF >= 3 -> priority = 2
 */
const int ReplicationPriorityCount = 3;

DEFINE_BIT_ENUM(EChunkStatus,
    ((None)              (0x0000))
    ((Underreplicated)   (0x0001))
    ((Overreplicated)    (0x0002))
    ((Lost)              (0x0004))
    ((DataMissing)       (0x0008))
    ((ParityMissing)     (0x0010))
    ((QuorumMissing)     (0x0020))
    ((Safe)              (0x0040))
    ((Sealed)            (0x0080))
    ((UnsafelyPlaced)    (0x0100))
);

typedef std::list<TChunk*> TChunkRepairQueue;
typedef TChunkRepairQueue::iterator TChunkRepairQueueIterator;

typedef std::multimap<double, NNodeTrackerServer::TNode*> TFillFactorToNodeMap;
typedef TFillFactorToNodeMap::iterator TFillFactorToNodeIterator;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
