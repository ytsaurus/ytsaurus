#include "dynamic_store.h"

#include "chunk.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/tablet_server/tablet.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NChunkServer {

using namespace NTabletClient;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

std::string TDynamicStore::GetLowercaseObjectName() const
{
    return Format(
        "%v dynamic store %v",
        GetType() == EObjectType::SortedDynamicTabletStore ? "sorted" : "ordered",
        GetId());
}

std::string TDynamicStore::GetCapitalizedObjectName() const
{
    return Format(
        "%v dynamic store %v",
        GetType() == EObjectType::SortedDynamicTabletStore ? "Sorted" : "Ordered",
        GetId());
}

void TDynamicStore::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkTree>();

    PHOENIX_REGISTER_FIELD(1, Tablet_);
    PHOENIX_REGISTER_FIELD(2, FlushedChunk_);
    PHOENIX_REGISTER_FIELD(3, Parents_);
    PHOENIX_REGISTER_FIELD(4, Flushed_);
    PHOENIX_REGISTER_FIELD(5, TableRowIndex_);
}

PHOENIX_DEFINE_TYPE(TDynamicStore);

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

void TDynamicStore::SetFlushedChunk(TChunk* chunk)
{
    YT_VERIFY(!IsFlushed());
    Flushed_ = true;
    FlushedChunk_.Assign(chunk);
    SetTablet(nullptr);
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
