#include "stdafx.h"
#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "helpers.h"

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TChunkList::TChunkList(const TChunkListId& id)
    : TChunkTree(id)
    , Version_(0)
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
}

ui64 TChunkList::GetVisitMark() const
{
    return GetDynamicData()->VisitMark;
}

void TChunkList::SetVisitMark(ui64 value)
{
    GetDynamicData()->VisitMark = value;
}

ui64 TChunkList::GenerateVisitMark()
{
    static std::atomic<ui64> counter(0);
    return ++counter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
