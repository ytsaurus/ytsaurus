#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;
using namespace NCellMaster;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

void TChunkList::TMainTreeChunkListTraits::Persist(const NCellMaster::TPersistenceContext& context)
{
    YT_VERIFY(context.GetVersion() >= NCellMaster::EMasterReign::HunkChunkTreeStatisticsOverhaul);

    using NYT::Persist;
    Persist(context, Statistics);
    Persist(context, PivotKey);
}

void TChunkList::THunkTreeChunkListTraits::Persist(const NCellMaster::TPersistenceContext& context)
{
    YT_VERIFY(context.GetVersion() >= NCellMaster::EMasterReign::HunkChunkTreeStatisticsOverhaul);

    using NYT::Persist;
    Persist(context, Statistics);
    Persist(context, HunkChunkIdToRefCount);
}

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

    auto lastChunk = Children_.back();
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

std::string TChunkList::GetLowercaseObjectName() const
{
    return Format("chunk list %v", GetId());
}

std::string TChunkList::GetCapitalizedObjectName() const
{
    return Format("Chunk list %v", GetId());
}

TYPath TChunkList::GetObjectPath() const
{
    return Format("//sys/chunk_lists/%v", GetId());
}

void TChunkList::CheckInvariants(TBootstrap* bootstrap) const
{
    TChunkTree::CheckInvariants(bootstrap);

    auto kind = GetKind();
    if (kind == EChunkListKind::SortedDynamicRoot ||
        kind == EChunkListKind::OrderedDynamicRoot ||
        kind == EChunkListKind::JournalRoot ||
        kind == EChunkListKind::HunkRoot ||
        kind == EChunkListKind::HunkStorageRoot)
    {
        YT_VERIFY(Parents_.IsEmpty());
    }
    if (kind == EChunkListKind::SortedDynamicTablet || kind == EChunkListKind::OrderedDynamicTablet) {
        for (auto parent : Parents_) {
            if (kind == EChunkListKind::SortedDynamicTablet) {
                auto parentKind = parent->GetKind();
                YT_VERIFY(parentKind == EChunkListKind::SortedDynamicRoot || parentKind == EChunkListKind::SortedDynamicTablet);
            } else {
                YT_VERIFY(parent->GetKind() == EChunkListKind::OrderedDynamicRoot);
            }
        }
    }
    if (kind == EChunkListKind::Static) {
        for (auto parent : Parents_) {
            YT_VERIFY(parent->GetKind() == EChunkListKind::Static);
        }
    }
    if (kind == EChunkListKind::Hunk) {
        for (auto parent : Parents_) {
            YT_VERIFY(parent->GetKind() == EChunkListKind::HunkRoot);
        }
    }
    if (kind == EChunkListKind::HunkTablet) {
        for (auto parent : Parents_) {
            YT_VERIFY(parent->GetKind() == EChunkListKind::HunkStorageRoot);
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
    Save(context, CumulativeStatistics_);
    Save(context, Kind_);
    Save(context, TrimmedChildCount_);
    Save(context, ChunkListTraits_);
}

void TChunkList::Load(NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);

    using NYT::Load;
    Load(context, Children_);
    Load(context, Parents_);
    Load(context, TrunkOwningNodes_);
    Load(context, BranchedOwningNodes_);

    // COMPAT(akozhikhov)
    bool applyHunkTreeStatisticsOverhaulCompat = context.GetVersion() < NCellMaster::EMasterReign::HunkChunkTreeStatisticsOverhaul ||
        (context.GetVersion() >= NCellMaster::EMasterReign::Start_26_2 &&
         context.GetVersion() < NCellMaster::EMasterReign::HunkChunkTreeStatisticsOverhaul_26_2);

    // COMPAT(akozhikhov)
    TChunkTreeStatistics statistics;
    if (applyHunkTreeStatisticsOverhaulCompat) {
        Load(context, statistics);
    }

    Load(context, CumulativeStatistics_);
    Load(context, Kind_);
    Load(context, TrimmedChildCount_);

    // COMPAT(akozhikhov)
    TLegacyOwningKey pivotKey;
    if (applyHunkTreeStatisticsOverhaulCompat) {
        Load(context, pivotKey);
    }

    if (!applyHunkTreeStatisticsOverhaulCompat) {
        Load(context, ChunkListTraits_);
    }

    // COMPAT(akozhikhov)
    if (applyHunkTreeStatisticsOverhaulCompat) {
        if (IsHunkRelatedChunkList(this)) {
            // NB: We will recalculate it from scratch.
            ChunkListTraits_ = THunkTreeChunkListTraits{};
        } else {
            TMainTreeChunkListTraits chunkListTraits;
            chunkListTraits.Statistics = std::move(statistics);
            chunkListTraits.PivotKey = pivotKey;
            ChunkListTraits_ = std::move(chunkListTraits);
        }
    }

    if (HasChildToIndexMapping()) {
        for (int index = 0; index < std::ssize(Children_); ++index) {
            auto child = Children_[index];
            YT_VERIFY(ChildToIndex_.emplace(child, index).second);
        }
    }
}

TRange<TChunkListRawPtr> TChunkList::Parents() const
{
    return TRange(Parents_.begin(), Parents_.end());
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

TRange<TChunkOwnerBaseRawPtr> TChunkList::TrunkOwningNodes() const
{
    return TRange(TrunkOwningNodes_.begin(), TrunkOwningNodes_.end());
}

TRange<TChunkOwnerBaseRawPtr> TChunkList::BranchedOwningNodes() const
{
    return TRange(BranchedOwningNodes_.begin(), BranchedOwningNodes_.end());
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

    if (IsHunkRelatedChunkList(this)) {
        ChunkListTraits_ = THunkTreeChunkListTraits{};
    } else {
        ChunkListTraits_ = TMainTreeChunkListTraits{};
    }

    RecomputeChunkListStatistics(this);
}

TKeyBound TChunkList::GetPivotKeyBound() const
{
    if (IsHunkRelatedChunkList(this)) [[unlikely]] {
        YT_LOG_ALERT("Accessed chunk list method GetPivotKeyBound that requires it to be of a different kind "
            "(ChunkListId: %v, Kind: %v)",
            GetId(),
            GetKind());
        return TKeyBound::MakeUniversal(/*isUpper*/ false);
    }

    const auto& pivotKey = std::get<TMainTreeChunkListTraits>(ChunkListTraits_).PivotKey;
    return pivotKey
        ? TKeyBound::FromRow() >= pivotKey
        : TKeyBound::MakeUniversal(/*isUpper*/ false);
}

bool TChunkList::IsNewAppendTabletChunkList() const
{
    if (Children_.empty()) {
        return false;
    }

    const auto& child = Children_.front();
    return child->GetType() == EObjectType::ChunkList &&
        child->AsChunkList()->GetKind() == EChunkListKind::SortedDynamicTablet;
}

TChunkList::TAppendTabletChunkLists TChunkList::GetAppendTabletChunkLists() const
{
    YT_VERIFY(Kind_ == EChunkListKind::SortedDynamicTablet);
    YT_VERIFY(Children_.size() == 2);

    TAppendTabletChunkLists appendTabletChunkLists{
        .OriginatingChunkList = Children_[0]->AsChunkList(),
        .DeltaChunkList = Children_[1]->AsChunkList(),
    };

    YT_VERIFY(appendTabletChunkLists.OriginatingChunkList->Kind_ == EChunkListKind::SortedDynamicTablet);
    YT_VERIFY(appendTabletChunkLists.DeltaChunkList->Kind_ == EChunkListKind::SortedDynamicSubtablet);

    return appendTabletChunkLists;
}

TChunkTreeStatistics& TChunkList::Statistics()
{
    if (IsHunkRelatedChunkList(this)) [[unlikely]] {
        YT_LOG_ALERT("Accessed chunk list method Statistics that requires it to be of a different kind "
            "(ChunkListId: %v, Kind: %v)",
            GetId(),
            GetKind());
        static TChunkTreeStatistics nullStatistics;
        return nullStatistics;
    }

    return std::get<TMainTreeChunkListTraits>(ChunkListTraits_).Statistics;
}

const TChunkTreeStatistics& TChunkList::Statistics() const
{
    if (IsHunkRelatedChunkList(this)) [[unlikely]] {
        YT_LOG_ALERT("Accessed chunk list method Statistics that requires it to be of a different kind "
            "(ChunkListId: %v, Kind: %v)",
            GetId(),
            GetKind());
        static TChunkTreeStatistics nullStatistics;
        return nullStatistics;
    }

    return std::get<TMainTreeChunkListTraits>(ChunkListTraits_).Statistics;
}

TLegacyOwningKey TChunkList::GetPivotKey() const
{
    if (IsHunkRelatedChunkList(this)) [[unlikely]] {
        YT_LOG_ALERT("Accessed chunk list method GetPivotKey that requires it to be of a different kind "
            "(ChunkListId: %v, Kind: %v)",
            GetId(),
            GetKind());
        return {};
    }

    return std::get<TMainTreeChunkListTraits>(ChunkListTraits_).PivotKey;
}

void TChunkList::SetPivotKey(TLegacyOwningKey pivotKey)
{
    if (IsHunkRelatedChunkList(this)) [[unlikely]] {
        YT_LOG_ALERT("Accessed chunk list method SetPivotKey that requires it to be of a different kind "
            "(ChunkListId: %v, Kind: %v)",
            GetId(),
            GetKind());
        return;
    }

    std::get<TMainTreeChunkListTraits>(ChunkListTraits_).PivotKey = pivotKey;
}

bool TChunkList::IsSealed() const
{
    if (Children_.empty()) {
        return true;
    }
    auto lastChild = Children_.back();
    // NB: Nulls are possible in ordered tablets.
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
    if (!HasChildToIndexMapping()) {
        YT_LOG_ALERT("Accessed chunk list method HasChild that requires it to be of a different kind "
            "(ChunkListId: %v, Kind: %v)",
            GetId(),
            GetKind());
        return false;
    }

    return ChildToIndex_.contains(child);
}

const THunkChunkTreeStatistics& TChunkList::HunkStatistics() const
{
    if (!IsHunkRelatedChunkList(this)) [[unlikely]] {
        YT_LOG_ALERT("Accessed chunk list method HunkStatistics that requires it to be of a different kind "
            "(ChunkListId: %v, Kind: %v)",
            GetId(),
            GetKind());
        static THunkChunkTreeStatistics nullStatistics;
        return nullStatistics;
    }

    return std::get<THunkTreeChunkListTraits>(ChunkListTraits_).Statistics;
}

void TChunkList::AccumulateHunkStatistics(TChunk* chunk)
{
    if (!IsHunkRelatedChunkList(this)) [[unlikely]] {
        YT_LOG_ALERT("Accessed chunk list method AccumulateHunkStatistics that requires it to be of a different kind "
            "(ChunkListId: %v, Kind: %v)",
            GetId(),
            GetKind());
        return;
    }

    // NB: Hunk statistics of an unsealed chunk are incomplete.
    // Hence they will be propagated to all the parents later upon seal.
    if (!chunk->IsSealed()) {
        return;
    }

    auto& hunkTraits = std::get<THunkTreeChunkListTraits>(ChunkListTraits_);
    auto [it, emplaced] = hunkTraits.HunkChunkIdToRefCount.try_emplace(chunk->GetId());
    auto newRefCounter = ++(it->second);
    if (newRefCounter <= 0) {
        YT_LOG_ALERT("Encountered non-positive hunk chunk counter upon accumulating hunk statistics"
            "(ChunkListId: %v, ChunkId: %v, NewRefCounter: %v)",
            GetId(),
            chunk->GetId(),
            newRefCounter);

        hunkTraits.HunkChunkIdToRefCount.erase(it);
        return;
    }

    if (emplaced) {
        hunkTraits.Statistics.Accumulate(chunk->GetHunkStatistics());
    }
}

void TChunkList::DeaccumulateHunkStatistics(TChunk* chunk)
{
    if (!IsHunkRelatedChunkList(this)) [[unlikely]] {
        YT_LOG_ALERT("Accessed chunk list method DeaccumulateHunkStatistics that requires it to be of a different kind "
            "(ChunkListId: %v, Kind: %v)",
            GetId(),
            GetKind());
        return;
    }

    if (!chunk->IsSealed()) {
        return;
    }

    auto& hunkTraits = std::get<THunkTreeChunkListTraits>(ChunkListTraits_);

    auto it = hunkTraits.HunkChunkIdToRefCount.find(chunk->GetId());
    if (it == hunkTraits.HunkChunkIdToRefCount.end()) {
        YT_LOG_ALERT("Chunk is missing from chunk list statistics upon deaccumulating hunk statistics; skipping it "
            "(ChunkListId: %v, ChunkId: %v)",
            GetId(),
            chunk->GetId());
        return;
    }

    auto newRefCounter = --(it->second);
    if (newRefCounter < 0) {
        YT_LOG_ALERT("Encountered negative hunk chunk counter upon deaccumulating hunk statistics"
            "(ChunkListId: %v, ChunkId: %v, NewRefCounter: %v)",
            GetId(),
            chunk->GetId(),
            newRefCounter);

        hunkTraits.HunkChunkIdToRefCount.erase(it);
        return;
    }

    if (newRefCounter == 0) {
        hunkTraits.Statistics.Deaccumulate(chunk->GetHunkStatistics());
        hunkTraits.HunkChunkIdToRefCount.erase(it);
    }
}

void TChunkList::ResetHunkStatistics()
{
    if (!IsHunkRelatedChunkList(this)) [[unlikely]] {
        YT_LOG_ALERT("Accessed chunk list method ResetHunkStatistics that requires it to be of a different kind "
            "(ChunkListId: %v, Kind: %v)",
            GetId(),
            GetKind());
        return;
    }

    auto& hunkTraits = std::get<THunkTreeChunkListTraits>(ChunkListTraits_);
    hunkTraits.Statistics = THunkChunkTreeStatistics();
    hunkTraits.HunkChunkIdToRefCount = {};
}

void TChunkList::AccumulateNewlyReferencedHunkDataSize(TChunk* chunk, i64 dataSizeDelta)
{
    if (!IsHunkRelatedChunkList(this)) [[unlikely]] {
        YT_LOG_ALERT(
            "Accessed chunk list method AccumulateNewlyReferencedHunkDataSize that requires it to be of a different kind "
            "(ChunkListId: %v, Kind: %v)",
            GetId(),
            GetKind());
        return;
    }

    auto& hunkTraits = std::get<THunkTreeChunkListTraits>(ChunkListTraits_);

    if (!hunkTraits.HunkChunkIdToRefCount.contains(chunk->GetId())) {
        if (!chunk->IsSealed()) {
            // NB: Statistics will be accumulated upon seal.
            return;
        }

        YT_LOG_ALERT("Chunk is missing from chunk list statistics upon referencing hunk data; skipping it "
            "(ChunkListId: %v, ChunkId: %v, DataSizeDelta: %v)",
            GetId(),
            chunk->GetId(),
            dataSizeDelta);
        return;
    }

    THunkChunkTreeStatistics deltaStatistics;
    // NB: Include size of parity parts to disk space statistics.
    auto diskSpaceDelta = ComputeDiskSpaceFromDataSize(dataSizeDelta, chunk->GetErasureCodec());
    if (chunk->GetErasureCodec() == NErasure::ECodec::None) {
        deltaStatistics.ReferencedRegularDiskSpace += diskSpaceDelta;
    } else {
        deltaStatistics.ReferencedErasureDiskSpace += diskSpaceDelta;
    }

    hunkTraits.Statistics.Accumulate(deltaStatistics);

    if (hunkTraits.Statistics.ReferencedRegularDiskSpace < 0 ||
        hunkTraits.Statistics.ReferencedErasureDiskSpace < 0)
    {
        YT_LOG_ALERT("Encountered inconsistent referenced disk space upon referencing hunk data in a chunk list "
            "(ChunkListId: %v, ChunkId: %v, DataSizeDelta: %v, Statistics: %v)",
            GetId(),
            chunk->GetId(),
            dataSizeDelta,
            hunkTraits.Statistics);
    }
}

int TChunkList::GetRank() const
{
    static constexpr int HunkRootChunkListRank = 2;
    static constexpr int HunkIntermediateChunkListRank = 1;

    if (!IsHunkRelatedChunkList(this)) {
        return Statistics().Rank;
    }

    switch (Kind_) {
        case EChunkListKind::HunkRoot:
        case EChunkListKind::HunkStorageRoot:
            return HunkRootChunkListRank;
        case EChunkListKind::Hunk:
        case EChunkListKind::HunkTablet:
            return HunkIntermediateChunkListRank;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
