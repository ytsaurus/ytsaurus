#include "chunk_view.h"

#include "helpers.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NCellMaster;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TChunkViewModifier TChunkViewModifier::WithReadRange(NChunkClient::TLegacyReadRange readRange) &&
{
    SetReadRange(std::move(readRange));
    return *this;
}

TChunkViewModifier TChunkViewModifier::WithTransactionId(NObjectClient::TTransactionId transactionId) &&
{
    SetTransactionId(transactionId);
    return *this;
}

TChunkViewModifier TChunkViewModifier::WithMaxClipTimestamp(TTimestamp maxClipTimestamp) &&
{
    SetMaxClipTimestamp(maxClipTimestamp);
    return *this;
}

void TChunkViewModifier::SetReadRange(NChunkClient::TLegacyReadRange readRange)
{
    YT_VERIFY(!readRange.LowerLimit().HasOffset());
    YT_VERIFY(!readRange.UpperLimit().HasOffset());
    YT_VERIFY(!readRange.LowerLimit().HasChunkIndex());
    YT_VERIFY(!readRange.UpperLimit().HasChunkIndex());
    YT_VERIFY(!readRange.LowerLimit().HasRowIndex());
    YT_VERIFY(!readRange.UpperLimit().HasRowIndex());

    if (readRange.UpperLimit().HasLegacyKey()) {
        const auto& key = readRange.UpperLimit().GetLegacyKey();
        YT_VERIFY(key != NTableClient::MaxKey());
    }

    ReadRange_ = std::move(readRange);
}

TChunkViewModifier TChunkViewModifier::RestrictedWith(const TChunkViewModifier& other) const
{
    TChunkViewModifier copy(*this);

    if (!other.ReadRange().LowerLimit().IsTrivial() || !other.ReadRange().UpperLimit().IsTrivial()) {
        copy.ReadRange_ = TLegacyReadRange(
            GetAdjustedLowerReadLimit(other.ReadRange().LowerLimit()),
            GetAdjustedUpperReadLimit(other.ReadRange().UpperLimit())
        );
    }

    if (other.TransactionId_) {
        copy.TransactionId_ = other.TransactionId_;
    }

    if (other.MaxClipTimestamp_) {
        copy.MaxClipTimestamp_ = other.MaxClipTimestamp_;
    }

    return copy;
}

TLegacyReadLimit TChunkViewModifier::GetAdjustedLowerReadLimit(TLegacyReadLimit readLimit) const
{
    if (ReadRange_.LowerLimit().HasLegacyKey()) {
        readLimit.MergeLowerLegacyKey(ReadRange_.LowerLimit().GetLegacyKey());
    }
    return readLimit;
}

TLegacyReadLimit TChunkViewModifier::GetAdjustedUpperReadLimit(TLegacyReadLimit readLimit) const
{
    if (ReadRange_.UpperLimit().HasLegacyKey()) {
        readLimit.MergeUpperLegacyKey(ReadRange_.UpperLimit().GetLegacyKey());
    }
    return readLimit;
}

void TChunkViewModifier::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, ReadRange_);
    Save(context, TransactionId_);
    Save(context, MaxClipTimestamp_);
}

void TChunkViewModifier::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, ReadRange_);
    Load(context, TransactionId_);
    Load(context, MaxClipTimestamp_);
}

int CompareButForReadRange(const TChunkViewModifier& lhs, const TChunkViewModifier& rhs)
{
    // When chunk view gets new attributes one should consider them here
    // so that adjacent chunk views are merged only if their attributes
    // are otherwise identical.

    if (lhs.GetTransactionId() != rhs.GetTransactionId()) {
        return lhs.GetTransactionId() < rhs.GetTransactionId() ? -1 : 1;
    }

    if (lhs.GetMaxClipTimestamp() != rhs.GetMaxClipTimestamp()) {
        return lhs.GetMaxClipTimestamp() < rhs.GetMaxClipTimestamp() ? -1 : 1;
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

const TLegacyReadRange& TChunkView::ReadRange() const
{
    return Modifier_.ReadRange();
}

TTransactionId TChunkView::GetTransactionId() const
{
    return Modifier_.GetTransactionId();
}

TTimestamp TChunkView::GetMaxClipTimestamp() const
{
    return Modifier_.GetMaxClipTimestamp();
}

void TChunkView::SetUnderlyingTree(TChunkTree* underlyingTree)
{
    YT_VERIFY(underlyingTree);

    switch (underlyingTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            YT_VERIFY(underlyingTree->AsChunk()->GetChunkType() == EChunkType::Table);
            break;

        case EObjectType::SortedDynamicTabletStore:
        case EObjectType::OrderedDynamicTabletStore:
            break;

        default:
            YT_ABORT();
    }

    UnderlyingTree_ = underlyingTree;
}

TString TChunkView::GetLowercaseObjectName() const
{
    return Format("chunk view %v", GetId());
}

TString TChunkView::GetCapitalizedObjectName() const
{
    return Format("Chunk view %v", GetId());
}

TString TChunkView::GetObjectPath() const
{
    return Format("//sys/chunk_views/%v", GetId());
}

void TChunkView::Save(NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);

    using NYT::Save;

    Save(context, UnderlyingTree_);
    Save(context, Parents_);
    Save(context, Modifier_);
}

void TChunkView::Load(NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);

    using NYT::Load;

    Load(context, UnderlyingTree_);
    Load(context, Parents_);
    Load(context, Modifier_);
}

TLegacyReadRange TChunkView::GetCompleteReadRange() const
{
    return {
        Modifier_.GetAdjustedLowerReadLimit(TLegacyReadLimit(GetMinKeyOrThrow(UnderlyingTree_))),
        Modifier_.GetAdjustedUpperReadLimit(TLegacyReadLimit(GetUpperBoundKeyOrThrow(UnderlyingTree_)))
    };
}

void TChunkView::AddParent(TChunkList* parent)
{
    Parents_.push_back(parent);
}

void TChunkView::RemoveParent(TChunkList* parent)
{
    auto it = std::find(Parents_.begin(), Parents_.end(), parent);
    YT_VERIFY(it != Parents_.end());
    Parents_.erase(it);
}

TChunkTreeStatistics TChunkView::GetStatistics() const
{
    return GetChunkTreeStatistics(UnderlyingTree_);
}

int CompareButForReadRange(const TChunkView* lhs, const TChunkView* rhs)
{
    const auto& lhsChunkId = lhs->GetUnderlyingTree()->GetId();
    const auto& rhsChunkId = rhs->GetUnderlyingTree()->GetId();

    if (lhsChunkId != rhsChunkId) {
        return lhsChunkId < rhsChunkId ? -1 : 1;
    } else {
        return CompareButForReadRange(lhs->Modifier(), rhs->Modifier());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
