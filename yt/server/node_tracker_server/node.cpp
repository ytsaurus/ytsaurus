#include "stdafx.h"
#include "node.h"

#include <server/chunk_server/job.h>
#include <server/chunk_server/chunk.h>

#include <server/transaction_server/transaction.h>

#include <server/tablet_server/tablet_cell.h>

#include <server/node_tracker_server/config.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

void TNode::TTabletSlot::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Cell);
    Persist(context, PeerState);
    Persist(context, PeerId);
}

////////////////////////////////////////////////////////////////////////////////

TNode::TNode(
    TNodeId id,
    const TNodeDescriptor& descriptor,
    TNodeConfigPtr config)
    : Id_(id)
    , Descriptor_(descriptor)
    , Config_(config)
{
    Init();
}

TNode::TNode(TNodeId id)
    : Id_(id)
    , Config_(New<TNodeConfig>())
{
    Init();
}

void TNode::Init()
{
    UnregisterPending_ = false;
    VisitMark_ = 0;
    LoadRank_ = -1;
    Transaction_ = nullptr;
    Decommissioned_ = Config_->Decommissioned;
    ChunkReplicationQueues_.resize(ReplicationPriorityCount);
    ResetHints();
}

TNode::~TNode()
{ }

const TNodeDescriptor& TNode::GetDescriptor() const
{
    return Descriptor_;
}

const Stroka& TNode::GetAddress() const
{
    return Descriptor_.Address;
}

const TNodeConfigPtr& TNode::GetConfig() const
{
    return Config_;
}

void TNode::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Descriptor_.Address);
    Save(context, State_);
    Save(context, Statistics_);
    Save(context, Alerts_);
    Save(context, Transaction_);
    Save(context, StoredReplicas_);
    Save(context, CachedReplicas_);
    Save(context, UnapprovedReplicas_);
    Save(context, TabletSlots_);
    Save(context, TabletCellCreateQueue_);
}

void TNode::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, Descriptor_.Address);
    Load(context, State_);
    Load(context, Statistics_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 27) {
        Load(context, Alerts_);
    }
    Load(context, Transaction_);
    Load(context, StoredReplicas_);
    Load(context, CachedReplicas_);
    Load(context, UnapprovedReplicas_);
    Load(context, TabletSlots_);
    Load(context, TabletCellCreateQueue_);
}

void TNode::AddReplica(TChunkPtrWithIndex replica, bool cached)
{
    if (cached) {
        YCHECK(CachedReplicas_.insert(replica).second);
    } else {
        YCHECK(StoredReplicas_.insert(replica).second);
    }
}

void TNode::RemoveReplica(TChunkPtrWithIndex replica, bool cached)
{
    if (cached) {
        YCHECK(CachedReplicas_.erase(replica) == 1);
    } else {
        YCHECK(StoredReplicas_.erase(replica) == 1);
        UnapprovedReplicas_.erase(replica);
        ChunkRemovalQueue_.erase(TChunkIdWithIndex(replica.GetPtr()->GetId(), replica.GetIndex()));
        for (auto& queue : ChunkReplicationQueues_) {
            queue.erase(replica);
        }
    }
}

bool TNode::HasReplica(TChunkPtrWithIndex replica, bool cached) const
{
    if (cached) {
        return CachedReplicas_.find(replica) != CachedReplicas_.end();
    } else {
        return StoredReplicas_.find(replica) != StoredReplicas_.end();
    }
}

void TNode::MarkReplicaUnapproved(TChunkPtrWithIndex replica, TInstant timestamp)
{
    YASSERT(HasReplica(replica, false));
    YCHECK(UnapprovedReplicas_.insert(std::make_pair(replica, timestamp)).second);
}

bool TNode::HasUnapprovedReplica(TChunkPtrWithIndex replica) const
{
    return UnapprovedReplicas_.find(replica) != UnapprovedReplicas_.end();
}

void TNode::ApproveReplica(TChunkPtrWithIndex replica)
{
    YASSERT(HasReplica(replica, false));
    YCHECK(UnapprovedReplicas_.erase(replica) == 1);
}

void TNode::ResetHints()
{
    HintedUserSessionCount_ = 0;
    HintedReplicationSessionCount_ = 0;
    HintedRepairSessionCount_ = 0;
    HintedTabletSlots_ = 0;
}

void TNode::AddSessionHint(EWriteSessionType sessionType)
{
    switch (sessionType) {
        case EWriteSessionType::User:
            ++HintedUserSessionCount_;
            break;
        case EWriteSessionType::Replication:
            ++HintedReplicationSessionCount_;
            break;
        case EWriteSessionType::Repair:
            ++HintedRepairSessionCount_;
            break;
        default:
            YUNREACHABLE();
    }
}

bool TNode::HasSpareSession(EWriteSessionType sessionType) const
{
    switch (sessionType) {
        case EWriteSessionType::User:
            return true;
        case EWriteSessionType::Replication:
            return Statistics_.total_replication_session_count() + HintedReplicationSessionCount_ <
                   Statistics_.max_replication_session_count();
        case EWriteSessionType::Repair:
            return Statistics_.total_repair_session_count() + HintedRepairSessionCount_ <
                   Statistics_.max_repair_session_count();
        default:
            YUNREACHABLE();
    }
}

int TNode::GetTotalSessionCount() const
{
    return
        Statistics_.total_user_session_count() + HintedUserSessionCount_ +
        Statistics_.total_replication_session_count() + HintedReplicationSessionCount_ +
        Statistics_.total_repair_session_count() + HintedRepairSessionCount_;
}

TNode::TTabletSlot* TNode::FindTabletSlot(TTabletCell* cell)
{
    for (auto& slot : TabletSlots_) {
        if (slot.Cell == cell) {
            return &slot;
        }
    }
    return nullptr;
}

TNode::TTabletSlot* TNode::GetTabletSlot(TTabletCell* cell)
{
    auto* slot = FindTabletSlot(cell);
    YCHECK(slot);
    return slot;
}

bool TNode::IsTabletCellStartScheduled(TTabletCell* cell) const
{
    return TabletCellCreateQueue_.find(cell) != TabletCellCreateQueue_.end();
}

void TNode::ScheduleTabletCellStart(TTabletCell* cell)
{
    YCHECK(TabletCellCreateQueue_.insert(cell).second);
}

void TNode::CancelTabletCellStart(TTabletCell* cell)
{
    // NB: Need not be there.
    TabletCellCreateQueue_.erase(cell);
}

void TNode::DetachTabletCell(TTabletCell* cell)
{
    TabletCellCreateQueue_.erase(cell);

    auto* slot = FindTabletSlot(cell);
    if (slot) {
        *slot = TTabletSlot();
    }
}

TAtomic TNode::GenerateVisitMark()
{
    static TAtomic result = 0;
    return AtomicIncrement(result);
}

void TNode::AddTabletSlotHint()
{
    ++HintedTabletSlots_;
}

int TNode::GetTotalUsedTabletSlots() const
{
    return
        Statistics_.used_tablet_slots() +
        TabletCellCreateQueue_.size() +
        HintedTabletSlots_;
}

int TNode::GetTotalTabletSlots() const
{
    return
        Statistics_.used_tablet_slots() +
        Statistics_.available_tablet_slots();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

