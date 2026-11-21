#include "chunk_list.h"

#include "chunk_owner_base.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;
using namespace NCellMaster;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

void TChunkList::TMainTreeChunkListTraits::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Statistics);
    PHOENIX_REGISTER_FIELD(2, PivotKey);

    registrar.AfterLoad([] (TThis* /*this_*/, auto& context) {
        YT_VERIFY(context.GetVersion() >= EMasterReign::HunkChunkTreeStatisticsOverhaul);
    });
}

PHOENIX_DEFINE_TYPE(TChunkList::TMainTreeChunkListTraits);

void TChunkList::THunkTreeChunkListTraits::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Statistics);

    // COMPAT(akozhikhov).
    registrar.template VirtualField<2>("HunkChunkIdToRefCount", [] (TThis* /*this_*/, auto& context) {
        NYT::Load<THashMap<TChunkId, int>>(context);
    })
        .InVersions([] (EMasterReign version) {
            return
                version < EMasterReign::NewWayToStoreHunkChunkListStatistics_26_1 ||
                (version >= EMasterReign::Start_26_2 && version < EMasterReign::NewWayToStoreHunkChunkListStatistics);
        })();

    registrar.AfterLoad([] (TThis* /*this_*/, auto& context) {
        YT_VERIFY(context.GetVersion() >= EMasterReign::HunkChunkTreeStatisticsOverhaul);
    });
}

PHOENIX_DEFINE_TYPE(TChunkList::THunkTreeChunkListTraits);

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
    const TChunkTree* current = this;
    while (true) {
        if (current->IsChunkList()) {
            auto parents = current->AsChunkList()->Parents();
            if (parents.Size() > 1) {
                THROW_ERROR_EXCEPTION("Chunk list %v has more than one parent",
                    current->GetId());
            }
            if (parents.Empty()) {
                break;
            }
            current = parents[0];
        } else {
            YT_TLOG_ALERT_AND_THROW("Chunk list has ancestor of invalid type")
                .With("ChunkListId", GetId())
                .With("AncestorId", current->GetId())
                .With("AncestorType", current->GetType());
        }
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
            YT_VERIFY(parent->IsChunkList());
            auto parentKind = parent->AsChunkList()->GetKind();
            if (kind == EChunkListKind::SortedDynamicTablet) {
                YT_VERIFY(parentKind == EChunkListKind::SortedDynamicRoot || parentKind == EChunkListKind::SortedDynamicTablet);
            } else {
                YT_VERIFY(parentKind == EChunkListKind::OrderedDynamicRoot);
            }
        }
    }
    if (kind == EChunkListKind::Static) {
        for (auto parent : Parents_) {
            YT_VERIFY(parent->IsChunkList());
            YT_VERIFY(parent->AsChunkList()->GetKind() == EChunkListKind::Static);
        }
    }
    if (kind == EChunkListKind::Hunk) {
        for (auto parent : Parents_) {
            YT_VERIFY(parent->IsChunkList());
            YT_VERIFY(parent->AsChunkList()->GetKind() == EChunkListKind::HunkRoot);
        }
    }
    if (kind == EChunkListKind::HunkTablet) {
        for (auto parent : Parents_) {
            YT_VERIFY(parent->IsChunkList());
            YT_VERIFY(parent->AsChunkList()->GetKind() == EChunkListKind::HunkStorageRoot);
        }
    }
}

namespace {

// COMPAT(akozhikhov)
bool ShouldApplyHunkTreeStatisticsOverhaulCompat(EMasterReign version)
{
    return
        version < EMasterReign::HunkChunkTreeStatisticsOverhaul ||
        (version >= EMasterReign::Start_26_2 && version < EMasterReign::HunkChunkTreeStatisticsOverhaul_26_2);
}

// COMPAT(akozhikhov)
bool ShouldApplyHunkTreeStatisticsOverhaulCompatAgain(EMasterReign version)
{
    return
        version < EMasterReign::RecomputeHunkRelatedChunkStatisticsAgain ||
        (version >= EMasterReign::Start_26_2 && version < EMasterReign::RecomputeHunkRelatedChunkStatisticsAgain_26_2);
}

} // namespace

void TChunkList::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkTree>();

    PHOENIX_REGISTER_FIELD(1, Children_);

    registrar.template VirtualField<2>("Parents_", [] (TThis* this_, auto& context) {
        // COMPAT(grphil)
        if (context.GetVersion() < EMasterReign::ChunkListRefactoring) {
            auto parentChunkLists = NYT::Load<std::vector<TChunkListRawPtr>>(context);
            this_->Parents_.Reserve(parentChunkLists.size());
            for (auto chunkList : parentChunkLists) {
                this_->Parents_.PushBack(chunkList);
            }
        } else {
            NYT::Load(context, this_->Parents_);
        }
    }, [] (const TThis* this_, auto& context) {
        NYT::Save(context, this_->Parents_);
    })();

    PHOENIX_REGISTER_FIELD(3, TrunkOwningNodes_);
    PHOENIX_REGISTER_FIELD(4, BranchedOwningNodes_);

    // COMPAT(akozhikhov): Legacy statistics are stashed into main tree traits
    // and finalized in AfterLoad (once Kind_ is known).
    registrar.template VirtualField<5>("LegacyStatistics", [] (TThis* this_, auto& context) {
        TMainTreeChunkListTraits chunkListTraits;
        NYT::Load(context, chunkListTraits.Statistics);
        this_->ChunkListTraits_ = std::move(chunkListTraits);
    })
        .InVersions(ShouldApplyHunkTreeStatisticsOverhaulCompat)();

    PHOENIX_REGISTER_FIELD(6, CumulativeStatistics_);
    PHOENIX_REGISTER_FIELD(7, Kind_);
    PHOENIX_REGISTER_FIELD(8, TrimmedChildCount_);

    // COMPAT(akozhikhov)
    registrar.template VirtualField<9>("LegacyPivotKey", [] (TThis* this_, auto& context) {
        NYT::Load(context, std::get<TMainTreeChunkListTraits>(this_->ChunkListTraits_).PivotKey);
    })
        .InVersions(ShouldApplyHunkTreeStatisticsOverhaulCompat)();

    PHOENIX_REGISTER_FIELD(10, ChunkListTraits_,
        .InVersions([] (EMasterReign version) {
            return !ShouldApplyHunkTreeStatisticsOverhaulCompat(version);
        })
        // NB: Set by LegacyStatistics and LegacyPivotKey.
        .KeepWhenMissing());

    registrar.AfterLoad([] (TThis* this_, auto& context) {
        // COMPAT(akozhikhov)
        if (ShouldApplyHunkTreeStatisticsOverhaulCompat(context.GetVersion())) {
            if (this_->IsHunkRelated()) {
                // NB: We will recalculate it from scratch.
                this_->ChunkListTraits_ = THunkTreeChunkListTraits{};
            }
        }

        // COMPAT(akozhikhov)
        if (ShouldApplyHunkTreeStatisticsOverhaulCompatAgain(context.GetVersion())) {
            if (!this_->IsHunkRelated()) {
                std::get<TMainTreeChunkListTraits>(this_->ChunkListTraits_).Statistics.HunkErasureDiskSpace = 0;
            }
        }

        if (this_->HasChildToIndexMapping()) {
            for (int index = 0; index < std::ssize(this_->Children_); ++index) {
                auto child = this_->Children_[index];
                YT_VERIFY(this_->ChildToIndex_.emplace(child, index).second);
            }
        }
    });
}

PHOENIX_DEFINE_TYPE(TChunkList);

TRange<TChunkTreeRawPtr> TChunkList::Parents() const
{
    return TRange(Parents_.begin(), Parents_.end());
}

void TChunkList::AddParent(TChunkTree* parent)
{
    YT_VERIFY(parent->IsChunkList());
    Parents_.PushBack(parent);
}

void TChunkList::RemoveParent(TChunkTree* parent)
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

    if (IsHunkRelated()) {
        ChunkListTraits_ = THunkTreeChunkListTraits{};
    } else {
        ChunkListTraits_ = TMainTreeChunkListTraits{};
    }

    RecomputeChunkListStatistics(this);
}

TKeyBound TChunkList::GetPivotKeyBound() const
{
    if (IsHunkRelated()) [[unlikely]] {
        YT_TLOG_ALERT("Accessed chunk list method GetPivotKeyBound that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind());
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
        .AppendDeltaChunkList = Children_[1]->AsChunkList(),
    };

    YT_VERIFY(appendTabletChunkLists.OriginatingChunkList->Kind_ == EChunkListKind::SortedDynamicTablet);
    YT_VERIFY(appendTabletChunkLists.AppendDeltaChunkList->Kind_ == EChunkListKind::SortedDynamicSubtablet);

    return appendTabletChunkLists;
}

TChunkTreeStatistics& TChunkList::Statistics()
{
    if (IsHunkRelated()) [[unlikely]] {
        YT_TLOG_ALERT("Accessed chunk list method Statistics that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind());
        static TChunkTreeStatistics nullStatistics;
        return nullStatistics;
    }

    return std::get<TMainTreeChunkListTraits>(ChunkListTraits_).Statistics;
}

const TChunkTreeStatistics& TChunkList::Statistics() const
{
    if (IsHunkRelated()) [[unlikely]] {
        YT_TLOG_ALERT("Accessed chunk list method Statistics that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind());
        static TChunkTreeStatistics nullStatistics;
        return nullStatistics;
    }

    return std::get<TMainTreeChunkListTraits>(ChunkListTraits_).Statistics;
}

TLegacyOwningKey TChunkList::GetPivotKey() const
{
    if (IsHunkRelated()) [[unlikely]] {
        YT_TLOG_ALERT("Accessed chunk list method GetPivotKey that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind());
        return {};
    }

    return std::get<TMainTreeChunkListTraits>(ChunkListTraits_).PivotKey;
}

void TChunkList::SetPivotKey(TLegacyOwningKey pivotKey)
{
    if (IsHunkRelated()) [[unlikely]] {
        YT_TLOG_ALERT("Accessed chunk list method SetPivotKey that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind());
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

bool TChunkList::HasStatistics() const
{
    return Kind_ != EChunkListKind::Scratch;
}

bool TChunkList::IsHunkRoot() const
{
    return
        Kind_ == EChunkListKind::HunkRoot ||
        Kind_ == EChunkListKind::HunkStorageRoot;
}

bool TChunkList::IsHunkRelated() const
{
    return
        IsHunkRoot() ||
        Kind_ == EChunkListKind::Hunk ||
        Kind_ == EChunkListKind::HunkTablet;
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
        Kind_ == EChunkListKind::HunkTablet ||
        Kind_ == EChunkListKind::Scratch;
}

bool TChunkList::HasChild(TChunkTree* child) const
{
    if (!HasChildToIndexMapping()) {
        YT_TLOG_ALERT("Accessed chunk list method HasChild that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind());
        return false;
    }

    return ChildToIndex_.contains(child);
}

const THunkChunkTreeStatistics& TChunkList::HunkStatistics() const
{
    if (!IsHunkRelated()) [[unlikely]] {
        YT_TLOG_ALERT("Accessed chunk list method HunkStatistics that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind());
        static THunkChunkTreeStatistics nullStatistics;
        return nullStatistics;
    }

    return std::get<THunkTreeChunkListTraits>(ChunkListTraits_).Statistics;
}

void TChunkList::AccumulateHunkStatistics(TChunk* chunk, bool force)
{
    if (!IsHunkRelated()) [[unlikely]] {
        YT_TLOG_ALERT("Accessed chunk list method AccumulateHunkStatistics that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind());
        return;
    }

    // NB: Hunk statistics of an unsealed chunk are incomplete.
    // Hence they will be propagated to all the parents later upon seal.
    if (!chunk->IsSealed()) {
        return;
    }

    if (!force && !IsHunkChunkUniquelyPresentInChunkList(this, chunk)) {
        return;
    }

    auto& hunkTraits = std::get<THunkTreeChunkListTraits>(ChunkListTraits_);
    hunkTraits.Statistics.Accumulate(chunk->GetHunkStatistics());
}

void TChunkList::DeaccumulateHunkStatistics(TChunk* chunk)
{
    if (!IsHunkRelated()) [[unlikely]] {
        YT_TLOG_ALERT("Accessed chunk list method DeaccumulateHunkStatistics that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind());
        return;
    }

    if (!chunk->IsSealed()) {
        return;
    }

    if (!IsHunkChunkUniquelyPresentInChunkList(this, chunk)) {
        return;
    }

    auto& hunkTraits = std::get<THunkTreeChunkListTraits>(ChunkListTraits_);
    hunkTraits.Statistics.Deaccumulate(chunk->GetHunkStatistics());
}

void TChunkList::ResetHunkStatistics()
{
    if (!IsHunkRelated()) [[unlikely]] {
        YT_TLOG_ALERT("Accessed chunk list method ResetHunkStatistics that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind());
        return;
    }

    auto& hunkTraits = std::get<THunkTreeChunkListTraits>(ChunkListTraits_);
    hunkTraits.Statistics = THunkChunkTreeStatistics();
}

void TChunkList::CopyHunkStatistics(TChunkList* other)
{
    if (!IsHunkRelated() ||
        other->GetKind() != GetKind()) [[unlikely]]
    {
        YT_TLOG_ALERT("Accessed chunk list method CopyHunkStatistics that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind())
            .With("OtherChunkListId", other->GetId())
            .With("OtherChunkListKind", other->GetKind());
        return;
    }

    auto& hunkTraits = std::get<THunkTreeChunkListTraits>(ChunkListTraits_);
    const auto& otherHunkTraits = std::get<THunkTreeChunkListTraits>(other->ChunkListTraits_);

    hunkTraits.Statistics = otherHunkTraits.Statistics;
}

void TChunkList::AccumulateNewlyReferencedHunkDataSize(TChunk* chunk, i64 dataSizeDelta)
{
    if (!IsHunkRelated()) [[unlikely]] {
        YT_TLOG_ALERT("Accessed chunk list method AccumulateNewlyReferencedHunkDataSize that requires it to be of a different kind")
            .With("ChunkListId", GetId())
            .With("Kind", GetKind());
        return;
    }

    if (!chunk->IsSealed()) {
        // NB: Statistics will be accumulated upon seal.
        return;
    }

    // NB: Just check chunk tree invariants and chunk presence.
    Y_UNUSED(IsHunkChunkUniquelyPresentInChunkList(this, chunk));

    THunkChunkTreeStatistics deltaStatistics;
    // NB: Include size of parity parts to disk space statistics.
    auto diskSpaceDelta = ComputeDiskSpaceFromDataSize(dataSizeDelta, chunk->GetErasureCodec());
    if (chunk->GetErasureCodec() == NErasure::ECodec::None) {
        deltaStatistics.ReferencedRegularDiskSpace += diskSpaceDelta;
    } else {
        deltaStatistics.ReferencedErasureDiskSpace += diskSpaceDelta;
    }

    auto& hunkTraits = std::get<THunkTreeChunkListTraits>(ChunkListTraits_);
    hunkTraits.Statistics.Accumulate(deltaStatistics);

    // NB: We do not check ReferencedErasureDiskSpace field because it is unreliable
    // due to integer arithmetics in ComputeDiskSpaceFromDataSize.
    if (hunkTraits.Statistics.ReferencedRegularDiskSpace < 0) {
        YT_TLOG_ALERT("Encountered inconsistent referenced disk space upon referencing hunk data in a chunk list")
            .With("ChunkListId", GetId())
            .With("ChunkId", chunk->GetId())
            .With("DataSizeDelta", dataSizeDelta)
            .With("Statistics", hunkTraits.Statistics);
    }
}

int TChunkList::GetRank() const
{
    static constexpr int HunkRootChunkListRank = 2;
    static constexpr int HunkIntermediateChunkListRank = 1;

    if (!IsHunkRelated()) {
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
