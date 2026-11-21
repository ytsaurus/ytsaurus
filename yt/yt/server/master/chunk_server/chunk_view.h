#pragma once

#include "private.h"

#include "chunk_tree.h"
#include "chunk_tree_statistics.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/core/phoenix/type_decl.h>

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
    TChunkViewModifier WithReadRange(NChunkClient::TReadRange readRange) &&;
    TChunkViewModifier WithTransactionId(NObjectClient::TTransactionId transactionId) &&;
    TChunkViewModifier WithMaxClipTimestamp(NTransactionClient::TTimestamp maxClipTimestamp) &&;

    TChunkViewModifier RestrictedWith(const TChunkViewModifier& other) const;

    NChunkClient::TLegacyReadLimit GetAdjustedLowerReadLimit(NChunkClient::TLegacyReadLimit readLimit) const;
    NChunkClient::TLegacyReadLimit GetAdjustedUpperReadLimit(NChunkClient::TLegacyReadLimit readLimit) const;

    friend int CompareButForReadRange(const TChunkViewModifier& lhs, const TChunkViewModifier& rhs);

public:
    // For tests.
    void SetReadRange(NChunkClient::TLegacyReadRange readRange);

    using TSaveContext = NCellMaster::TSaveContext;
    using TLoadContext = NCellMaster::TLoadContext;

    PHOENIX_DECLARE_TYPE(TChunkViewModifier, 0x8a171820);
};

////////////////////////////////////////////////////////////////////////////////

class TChunkView
    : public TChunkTree
    , public TRefTracked<TChunkView>
{
    DEFINE_BYVAL_RO_PROPERTY(TChunkTreeRawPtr, UnderlyingTree);

    using TParents = TCompactVector<TChunkListRawPtr, TypicalChunkParentCount>;
    DEFINE_BYREF_RO_PROPERTY(TParents, Parents);

    DEFINE_BYREF_RW_PROPERTY(TChunkViewModifier, Modifier);

    DECLARE_BYREF_RO_PROPERTY(NChunkClient::TLegacyReadRange, ReadRange);
    DECLARE_BYVAL_RO_PROPERTY(NObjectClient::TTransactionId, TransactionId);
    DECLARE_BYVAL_RO_PROPERTY(NTransactionClient::TTimestamp, MaxClipTimestamp);

public:
    using TChunkTree::TChunkTree;

    void SetUnderlyingTree(TChunkTree* underlyingTree);

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

    NChunkClient::TReadRange GetCompleteReadRange(NTableClient::TComparator comparator);

    void AddParent(TChunkList* parent);
    void RemoveParent(TChunkList* parent);

    TChunkTreeStatistics GetStatistics() const;
    THunkChunkTreeStatistics GetHunkStatistics() const;

    //! Compares two chunk views not considering read range.
    //! Returns -1, 0 or 1.
    //! NB: Comparison is deterministic.
    friend int CompareButForReadRange(const TChunkView* lhs, const TChunkView* rhs);

private:
    PHOENIX_DECLARE_TYPE(TChunkView, 0x8ef7bf76);
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkViewMergeResult
{
    TChunkView* FirstChunkView;
    TChunkView* LastChunkView;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
