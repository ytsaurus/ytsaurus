#include "dynamic_store.h"

#include "chunk.h"
#include "chunk_list.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/tablet_server/tablet.h>

namespace NYT::NChunkServer {

using namespace NTabletClient;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

TString TDynamicStore::GetLowercaseObjectName() const
{
    return Format(
        "%v dynamic store %v",
        GetType() == EObjectType::SortedDynamicTabletStore ? "sorted" : "ordered",
        GetId());
}

TString TDynamicStore::GetCapitalizedObjectName() const
{
    return Format(
        "%v dynamic store %v",
        GetType() == EObjectType::SortedDynamicTabletStore ? "Sorted" : "Ordered",
        GetId());
}

void TDynamicStore::Save(TSaveContext& context) const
{
    TChunkTree::Save(context);

    using NYT::Save;

    Save(context, Tablet_);
    Save(context, FlushedChunk_);
    Save(context, Parents_);
    Save(context, Flushed_);
    Save(context, TableRowIndex_);
}

void TDynamicStore::Load(TLoadContext& context)
{
    TChunkTree::Load(context);

    using NYT::Load;

    Load(context, Tablet_);
    Load(context, FlushedChunk_);
    Load(context, Parents_);
    Load(context, Flushed_);
    Load(context, TableRowIndex_);
}

void TDynamicStore::AddParent(TChunkTree* parent)
{
    Parents_.push_back(parent);
}

void TDynamicStore::RemoveParent(TChunkTree* parent)
{
    auto it = std::find(Parents_.begin(), Parents_.end(), parent);
    YT_VERIFY(it != Parents_.end());
    Parents_.erase(it);
}

TChunkTreeStatistics TDynamicStore::GetStatistics() const
{
    // NB: We cannot provide any reasonable estimate here since dynamic store
    // can be attached to several chunk lists while statistics are updated
    // and their statistics cannot be changed.

    // TODO(ifsmirnov): For reasonable estimates for chunk fetcher see YT-12212.

    TChunkTreeStatistics statistics;
    statistics.ChunkCount = 1;
    statistics.LogicalChunkCount = 1;
    statistics.RowCount = 0;
    statistics.LogicalRowCount = 0;
    statistics.CompressedDataSize = 0;
    statistics.UncompressedDataSize = 0;
    statistics.DataWeight = 0;
    statistics.LogicalDataWeight = 0;
    return statistics;
}

void TDynamicStore::SetTablet(TTablet* tablet)
{
    if (Tablet_) {
        EraseOrCrash(Tablet_->DynamicStores(), this);
    }

    Tablet_ = tablet;

    if (Tablet_) {
        InsertOrCrash(Tablet_->DynamicStores(), this);
    }
}

TTablet* TDynamicStore::GetTablet() const
{
    return Tablet_;
}

void TDynamicStore::ResetTabletCompat()
{
    Tablet_ = nullptr;
}

void TDynamicStore::SetFlushedChunk(TChunk* chunk)
{
    YT_VERIFY(!IsFlushed());
    Flushed_ = true;
    FlushedChunk_ = chunk;
    SetTablet(nullptr);
    if (chunk) {
        chunk->RefObject();
    }
}

bool TDynamicStore::IsFlushed() const
{
    return Flushed_;
}

void TDynamicStore::Abandon()
{
    SetTablet(nullptr);
}

bool TDynamicStore::IsAbandoned() const
{
    return Tablet_ == nullptr && !Flushed_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
