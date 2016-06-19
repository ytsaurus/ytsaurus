#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "helpers.h"

#include <yt/server/cell_master/serialize.h>

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TChunkList::TChunkList(const TChunkListId& id)
    : TChunkTree(id)
    , Ordered_(true)
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

void TChunkList::Save(NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);

    using NYT::Save;
    Save(context, Children_);
    Save(context, Parents_);
    Save(context, OwningNodes_);
    Save(context, Statistics_);
    Save(context, RowCountSums_);
    Save(context, ChunkCountSums_);
    Save(context, DataSizeSums_);
    Save(context, Ordered_);
    Save(context, TrimmedChildCount_);
}

void TChunkList::Load(NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);

    using NYT::Load;
    Load(context, Children_);
    Load(context, Parents_);
    Load(context, OwningNodes_);
    Load(context, Statistics_);
    Load(context, RowCountSums_);
    Load(context, ChunkCountSums_);
    Load(context, DataSizeSums_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 401) {
        Load(context, Ordered_);
        Load(context, TrimmedChildCount_);
    }

    for (int index = 0; index < Parents_.size(); ++index) {
        YCHECK(ParentToIndex_.insert(std::make_pair(Parents_[index], index)).second);
    }

    if (!Ordered_) {
        for (int index = 0; index < OwningNodes_.size(); ++index) {
            YCHECK(OwningNodeToIndex_.insert(std::make_pair(OwningNodes_[index], index)).second);
        }
    }
}

void TChunkList::AddParent(TChunkList* parent)
{
    int index = Parents_.size();
    Parents_.push_back(parent);
    YCHECK(ParentToIndex_.insert(std::make_pair(parent, index)).second);
}

void TChunkList::RemoveParent(TChunkList* parent)
{
    auto it = ParentToIndex_.find(parent);
    Y_ASSERT(it != ParentToIndex_.end());
    int index = it->second;
    if (index != ParentToIndex_.size() - 1) {
        std::swap(Parents_[index], Parents_.back());
        ParentToIndex_[Parents_[index]] = index;
    }
    Parents_.pop_back();
    ParentToIndex_.erase(it);
}

void TChunkList::AddOwningNode(TChunkOwnerBase* node)
{
    int index = OwningNodes_.size();
    OwningNodes_.push_back(node);
    YCHECK(OwningNodeToIndex_.insert(std::make_pair(node, index)).second);
}

void TChunkList::RemoveOwningNode(TChunkOwnerBase* node)
{
    auto it = OwningNodeToIndex_.find(node);
    Y_ASSERT(it != OwningNodeToIndex_.end());
    int index = it->second;
    if (index != OwningNodes_.size() - 1) {
        std::swap(OwningNodes_[index], OwningNodes_.back());
        OwningNodeToIndex_[OwningNodes_[index]] = index;
    }
    OwningNodes_.pop_back();
    OwningNodeToIndex_.erase(it);
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

void TChunkList::SetOrdered(bool value)
{
    if (Ordered_ == value) {
        return;
    }

    Ordered_ = value;
    RecomputeChunkListStatistics(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
