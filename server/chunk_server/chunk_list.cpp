#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "helpers.h"

#include <yt/server/cell_master/serialize.h>

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

void TChunkList::TCumulativeStatisticsEntry::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, RowCount);
    Persist(context, ChunkCount);
    Persist(context, DataSize);
}

////////////////////////////////////////////////////////////////////////////////

TChunkList::TChunkList(const TChunkListId& id)
    : TChunkTree(id)
{
    ResetChunkListStatistics(this);
}

TChunkListDynamicData* TChunkList::GetDynamicData() const
{
    return GetTypedDynamicData<TChunkListDynamicData>();
}

void TChunkList::IncrementVersion()
{
    ++Version_;
}

void TChunkList::ValidateSealed()
{
    if (!Statistics_.Sealed) {
        THROW_ERROR_EXCEPTION("Chunk list %v is not sealed",
            GetId());
    }
}

void TChunkList::ValidateUniqueAncestors()
{
    const auto* current = this;
    while (true) {
        const auto& parents = current->Parents();
        if (parents.Size() > 1) {
            THROW_ERROR_EXCEPTION("Chunk list %v has more than one parent",
                current->GetId());
        }
        if (parents.Empty()) {
            break;
        }
        current = parents[0];
    }
}

void TChunkList::Save(NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);

    using NYT::Save;
    Save(context, Children_);
    Save(context, Parents_);
    Save(context, TrunkOwningNodes_);
    Save(context, BranchedOwningNodes_);
    Save(context, Statistics_);
    Save(context, CumulativeStatistics_);
    Save(context, Kind_);
    Save(context, TrimmedChildCount_);
    Save(context, PivotKey_);
}

void TChunkList::Load(NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);

    using NYT::Load;
    Load(context, Children_);
    Load(context, Parents_);
    // COMPAT(babenko)
    if (context.GetVersion() < 355) {
        auto owningNodes = Load<std::vector<TChunkOwnerBase*>>(context);
        // NB: Node is not fully loaded yet, its IsTrunk method may not work properly.
        for (auto* node : owningNodes) {
            if (node->GetVersionedId().TransactionId) {
                BranchedOwningNodes_.PushBack(node);
            } else {
                TrunkOwningNodes_.PushBack(node);
            }
        }
    } else {
        Load(context, TrunkOwningNodes_);
        Load(context, BranchedOwningNodes_);
    }
    Load(context, Statistics_);

    // COMPAT(babenko)
    if (context.GetVersion() >= 400) {
        Load(context, CumulativeStatistics_);
    } else {
        std::vector<i64> rowCountSums, chunkCountSums, dataSizeSums;
        Load(context, rowCountSums);
        Load(context, chunkCountSums);
        Load(context, dataSizeSums);
        YCHECK(rowCountSums.size() == chunkCountSums.size() && rowCountSums.size() == dataSizeSums.size());
        for (int index = 0; index < rowCountSums.size(); ++index) {
            CumulativeStatistics_.push_back({
                rowCountSums[index],
                chunkCountSums[index],
                dataSizeSums[index]
            });
        }
    }

    // COMPAT(babenko)
    if (context.GetVersion() >= 400) {
        // COMPAT(savrus)
        if (context.GetVersion() >= 600) {
            Load(context, Kind_);
        } else {
            Load<bool>(context);
        }
        Load(context, TrimmedChildCount_);
    }

    // COMPAT(savrus)
    if (context.GetVersion() >= 600) {
        Load(context, PivotKey_);
    }

    if (!IsOrdered()) {
        for (int index = 0; index < Children_.size(); ++index) {
            YCHECK(ChildToIndex_.emplace(Children_[index], index).second);
        }
    }
}

TRange<TChunkList*> TChunkList::Parents() const
{
    return MakeRange(Parents_.begin(), Parents_.end());
}

void TChunkList::AddParent(TChunkList* parent)
{
    Parents_.PushBack(parent);
}

void TChunkList::RemoveParent(TChunkList* parent)
{
    Parents_.Remove(parent);
}

void TChunkList::AddOwningNode(TChunkOwnerBase* node)
{
    if (node->IsTrunk()) {
        TrunkOwningNodes_.PushBack(node);
    } else {
        BranchedOwningNodes_.PushBack(node);
    }
}

void TChunkList::RemoveOwningNode(TChunkOwnerBase* node)
{
    if (node->IsTrunk()) {
        TrunkOwningNodes_.Remove(node);
    } else {
        BranchedOwningNodes_.Remove(node);
    }
}

TRange<TChunkOwnerBase*> TChunkList::TrunkOwningNodes() const
{
    return MakeRange(TrunkOwningNodes_.begin(), TrunkOwningNodes_.end());
}

TRange<TChunkOwnerBase*> TChunkList::BranchedOwningNodes() const
{
    return MakeRange(BranchedOwningNodes_.begin(), BranchedOwningNodes_.end());
}

ui64 TChunkList::GenerateVisitMark()
{
    static std::atomic<ui64> counter(0);
    return ++counter;
}

int TChunkList::GetGCWeight() const
{
    return TObjectBase::GetGCWeight() + Children_.size();
}

void TChunkList::SetKind(EChunkListKind kind)
{
    if (Kind_ == kind) {
        return;
    }

    Kind_ = kind;
    RecomputeChunkListStatistics(this);
}

bool TChunkList::IsOrdered() const
{
    return Kind_ == EChunkListKind::Static || Kind_ == EChunkListKind::OrderedDynamicTablet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
