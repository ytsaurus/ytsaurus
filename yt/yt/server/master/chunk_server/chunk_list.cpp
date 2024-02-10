#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;
using namespace NCellMaster;
using namespace NChunkClient;
using namespace NTableClient;

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

void TChunkList::ValidateLastChunkSealed()
{
    if (Kind_ != EChunkListKind::JournalRoot) {
        return;
    }

    if (Children_.empty()) {
        return;
    }

    const auto* lastChunk = Children_.back();
    YT_VERIFY(IsJournalChunkType(lastChunk->GetType()));
    if (!lastChunk->IsSealed()) {
        THROW_ERROR_EXCEPTION("Last chunk %v of chunk list %v is not sealed",
            lastChunk->GetId(),
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

TString TChunkList::GetObjectPath() const
{
    return Format("//sys/chunk_lists/%v", GetId());
}

void TChunkList::CheckInvariants(TBootstrap* bootstrap) const
{
    TChunkTree::CheckInvariants(bootstrap);

    auto kind = GetKind();
    if (kind == EChunkListKind::SortedDynamicRoot ||
        kind == EChunkListKind::OrderedDynamicRoot ||
        kind == EChunkListKind::JournalRoot)
    {
        YT_VERIFY(Parents_.IsEmpty());
    }
    if (kind == EChunkListKind::SortedDynamicTablet || kind == EChunkListKind::OrderedDynamicTablet) {
        for (auto* parent : Parents_) {
            if (kind == EChunkListKind::SortedDynamicTablet) {
                YT_VERIFY(parent->GetKind() == EChunkListKind::SortedDynamicRoot);
            } else {
                YT_VERIFY(parent->GetKind() == EChunkListKind::OrderedDynamicRoot);
            }
        }
    }
    if (kind == EChunkListKind::Static) {
        for (auto* parent : Parents_) {
            YT_VERIFY(parent->GetKind() == EChunkListKind::Static);
        }
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
    Load(context, TrunkOwningNodes_);
    Load(context, BranchedOwningNodes_);
    Load(context, Statistics_);
    Load(context, CumulativeStatistics_);
    Load(context, Kind_);
    Load(context, TrimmedChildCount_);
    Load(context, PivotKey_);

    for (int index = 0; index < std::ssize(Children_); ++index) {
        auto* child = Children_[index];
        if (HasChildToIndexMapping()) {
            YT_VERIFY(ChildToIndex_.emplace(child, index).second);
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
    static std::atomic<ui64> counter;
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

TKeyBound TChunkList::GetPivotKeyBound() const
{
    return PivotKey_
        ? TKeyBound::FromRow() >= PivotKey_
        : TKeyBound::MakeUniversal(/*isUpper*/ false);
}

bool TChunkList::IsSealed() const
{
    if (Children_.empty()) {
        return true;
    }
    const auto* lastChild = Children_.back();
    // NB: nulls are possible in ordered tablets.
    return !lastChild || lastChild->IsSealed();
}

bool TChunkList::HasCumulativeStatistics() const
{
    return
        HasAppendableCumulativeStatistics() ||
        HasModifiableCumulativeStatistics() ||
        HasTrimmableCumulativeStatistics();
}

bool TChunkList::HasAppendableCumulativeStatistics() const
{
    return
        Kind_ == EChunkListKind::Static ||
        Kind_ == EChunkListKind::JournalRoot;
}

bool TChunkList::HasModifiableCumulativeStatistics() const
{
    return
        Kind_ == EChunkListKind::SortedDynamicRoot ||
        Kind_ == EChunkListKind::OrderedDynamicRoot ||
        Kind_ == EChunkListKind::SortedDynamicTablet ||
        Kind_ == EChunkListKind::SortedDynamicSubtablet ||
        Kind_ == EChunkListKind::HunkRoot ||
        Kind_ == EChunkListKind::Hunk ||
        Kind_ == EChunkListKind::HunkStorageRoot ||
        Kind_ == EChunkListKind::HunkTablet;
}

bool TChunkList::HasTrimmableCumulativeStatistics() const
{
    return
        Kind_ == EChunkListKind::OrderedDynamicTablet;
}

bool TChunkList::HasChildToIndexMapping() const
{
    return
        Kind_ == EChunkListKind::SortedDynamicRoot ||
        Kind_ == EChunkListKind::SortedDynamicTablet ||
        Kind_ == EChunkListKind::SortedDynamicSubtablet ||
        Kind_ == EChunkListKind::OrderedDynamicRoot ||
        Kind_ == EChunkListKind::HunkRoot ||
        Kind_ == EChunkListKind::Hunk ||
        Kind_ == EChunkListKind::JournalRoot ||
        Kind_ == EChunkListKind::HunkStorageRoot ||
        Kind_ == EChunkListKind::HunkTablet;
}

bool TChunkList::HasChild(TChunkTree* child) const
{
    YT_ASSERT(HasChildToIndexMapping());

    return ChildToIndex_.contains(child);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
