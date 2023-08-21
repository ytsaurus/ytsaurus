#pragma once

#include "chunk.h"
#include "chunk_tree_statistics.h"
#include "chunk_tree.h"
#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/client/chunk_client/read_limit.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkViewModifier
{
public:
    //! Denotes the portion of the chunk to be read. May contain only keys.
    //! Lower bound inclusive, upper bound exclusive.
    DEFINE_BYREF_RO_PROPERTY(NChunkClient::TLegacyReadRange, ReadRange);

    //! Denotes the id of the transaction that references override timestamp.
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TTransactionId, TransactionId);

    //! Denotes that all values with timestamp > MaxClipTimestamp should be ignored.
    //! Only applicable to sorted tables.
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, MaxClipTimestamp);

public:
    void SetReadRange(NChunkClient::TLegacyReadRange readRange);

    TChunkViewModifier WithReadRange(NChunkClient::TLegacyReadRange readRange) &&;
    TChunkViewModifier WithTransactionId(NObjectClient::TTransactionId transactionId) &&;
    TChunkViewModifier WithMaxClipTimestamp(NTransactionClient::TTimestamp maxClipTimestamp) &&;

    TChunkViewModifier RestrictedWith(const TChunkViewModifier& other) const;

    NChunkClient::TLegacyReadLimit GetAdjustedLowerReadLimit(NChunkClient::TLegacyReadLimit readLimit) const;
    NChunkClient::TLegacyReadLimit GetAdjustedUpperReadLimit(NChunkClient::TLegacyReadLimit readLimit) const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    friend int CompareButForReadRange(const TChunkViewModifier& lhs, const TChunkViewModifier& rhs);
};

////////////////////////////////////////////////////////////////////////////////

class TChunkView
    : public TChunkTree
    , public TRefTracked<TChunkView>
{
    DEFINE_BYVAL_RO_PROPERTY(TChunkTree*, UnderlyingTree);

    using TParents = TCompactVector<TChunkList*, TypicalChunkParentCount>;
    DEFINE_BYREF_RO_PROPERTY(TParents, Parents);

    DEFINE_BYREF_RW_PROPERTY(TChunkViewModifier, Modifier);

    DECLARE_BYREF_RO_PROPERTY(NChunkClient::TLegacyReadRange, ReadRange);
    DECLARE_BYVAL_RO_PROPERTY(NObjectClient::TTransactionId, TransactionId);
    DECLARE_BYVAL_RO_PROPERTY(NTransactionClient::TTimestamp, MaxClipTimestamp);

public:
    using TChunkTree::TChunkTree;

    void SetUnderlyingTree(TChunkTree* underlyingTree);

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    NChunkClient::TLegacyReadRange GetCompleteReadRange() const;

    void AddParent(TChunkList* parent);
    void RemoveParent(TChunkList* parent);

    TChunkTreeStatistics GetStatistics() const;

    //! Compares two chunk views not considering read range.
    //! Returns -1, 0 or 1.
    //! NB: comparison is deterministic.
    friend int CompareButForReadRange(const TChunkView* lhs, const TChunkView* rhs);
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkViewMergeResult
{
    TChunkView* FirstChunkView;
    TChunkView* LastChunkView;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
