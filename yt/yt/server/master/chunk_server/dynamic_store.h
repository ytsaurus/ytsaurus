#pragma once

#include "chunk_tree.h"
#include "chunk_tree_statistics.h"
#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/tablet_server/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicStore
    : public TChunkTree
    , public TRefTracked<TDynamicStore>
{
public:
    using TParents = TCompactVector<TChunkTree*, TypicalChunkParentCount>;

    DECLARE_BYVAL_RW_PROPERTY(NTabletServer::TTablet*, Tablet);
    DEFINE_BYVAL_RO_PROPERTY(TChunk*, FlushedChunk);
    DEFINE_BYREF_RO_PROPERTY(TParents, Parents);
    //! Used for flushed ordered dynamic stores. Denotes the (tablet-wise) row index
    //! of the first row in the chunk.
    DEFINE_BYVAL_RW_PROPERTY(i64, TableRowIndex);

public:
    using TChunkTree::TChunkTree;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    // May be nullptr in case if no rows were flushed.
    void SetFlushedChunk(TChunk* chunk);
    bool IsFlushed() const;

    // Dynamic store is abandoned if it was removed without flush if
    // the tablet was forcefully removed or experienced overwrite bulk insert.
    void Abandon();
    bool IsAbandoned() const;

    void AddParent(TChunkTree* parent);
    void RemoveParent(TChunkTree* parent);

    TChunkTreeStatistics GetStatistics() const;

    // COMPAT(ifsmirnov)
    void ResetTabletCompat();

private:
    NTabletServer::TTablet* Tablet_ = nullptr;
    bool Flushed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
