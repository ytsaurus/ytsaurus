#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "helpers.h"

#include <yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TChunkList::TChunkList(TChunkListId id)
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

TString TChunkList::GetLowercaseObjectName() const
{
    return Format("chunk list %v", GetId());
}

TString TChunkList::GetCapitalizedObjectName() const
{
    return Format("Chunk list %v", GetId());
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

void TChunkList::LoadCumulativeStatisticsCompat(NCellMaster::TLoadContext& context)
{
    struct TDummyStatisticsEntry
    {
        i64 RowCount;
        i64 ChunkCount;
        i64 DataSize;

        void Load(NCellMaster::TLoadContext& context)
        {
            using NYT::Load;
            Load(context, RowCount);
            Load(context, ChunkCount);
            Load(context, DataSize);
        }
    };

    auto dummyStatistics = NYT::Load<std::vector<TDummyStatisticsEntry>>(context);

    if (Children_.empty() || !HasAppendableCumulativeStatistics()) {
        return;
    }

    dummyStatistics.push_back(TDummyStatisticsEntry{
        Statistics_.LogicalRowCount,
        Statistics_.LogicalChunkCount,
        Statistics_.UncompressedDataSize});

    CumulativeStatistics_.Clear();
    CumulativeStatistics_.DeclareAppendable();

    TCumulativeStatisticsEntry previousSum;
    for (const auto& entry : dummyStatistics) {
        TCumulativeStatisticsEntry currentSum{
            entry.RowCount,
            entry.ChunkCount,
            entry.DataSize
        };
        CumulativeStatistics_.PushBack(currentSum - previousSum);
        previousSum = currentSum;
    }
}

void TChunkList::Load(NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);

    using NYT::Load;
    Load(context, Children_);
    Load(context, Parents_);
    Load(context, TrunkOwningNodes_);
    Load(context, BranchedOwningNodes_);
    Load(context, Statistics_);
    Load(context, CumulativeStatistics_);
    Load(context, Kind_);
    Load(context, TrimmedChildCount_);
    Load(context, PivotKey_);

    if (!IsOrdered()) {
        for (int index = 0; index < Children_.size(); ++index) {
            YT_VERIFY(ChildToIndex_.emplace(Children_[index], index).second);
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
    return TObject::GetGCWeight() + Children_.size();
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

bool TChunkList::HasCumulativeStatistics() const
{
    return HasAppendableCumulativeStatistics() ||
        HasModifyableCumulativeStatistics() ||
        HasTrimmableCumulativeStatistics();
}

bool TChunkList::HasAppendableCumulativeStatistics() const
{
    return Kind_ == EChunkListKind::Static;
}

bool TChunkList::HasModifyableCumulativeStatistics() const
{
    return Kind_ == EChunkListKind::SortedDynamicRoot ||
        Kind_ == EChunkListKind::OrderedDynamicRoot ||
        Kind_ == EChunkListKind::SortedDynamicTablet ||
        Kind_ == EChunkListKind::SortedDynamicSubtablet;
}

bool TChunkList::HasTrimmableCumulativeStatistics() const
{
    return Kind_ == EChunkListKind::OrderedDynamicTablet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
