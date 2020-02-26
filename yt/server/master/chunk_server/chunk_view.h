#pragma once

#include "chunk.h"
#include "chunk_tree_statistics.h"
#include "chunk_tree.h"
#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/client/chunk_client/read_limit.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkView
    : public TChunkTree
    , public TRefTracked<TChunkView>
{
    DEFINE_BYVAL_RO_PROPERTY(TChunk*, UnderlyingChunk);

    //! Denotes the portion of the chunk to be read. May contain only keys.
    //! Lower bound inclusive, upper bound exclusive.
    DEFINE_BYREF_RO_PROPERTY(NChunkClient::TReadRange, ReadRange);

    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TTransactionId, TransactionId);

    using TParents = SmallVector<TChunkList*, TypicalChunkParentCount>;
    DEFINE_BYREF_RO_PROPERTY(TParents, Parents);

public:
    explicit TChunkView(const TChunkViewId& id);

    void SetUnderlyingChunk(TChunk* underlyingChunk);
    void SetReadRange(NChunkClient::TReadRange readRange);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    NChunkClient::TReadLimit GetAdjustedLowerReadLimit(NChunkClient::TReadLimit readLimit) const;
    NChunkClient::TReadLimit GetAdjustedUpperReadLimit(NChunkClient::TReadLimit readLimit) const;

    NChunkClient::TReadRange GetCompleteReadRange() const;

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
