#include "stdafx.h"
#include "partition.h"
#include "automaton.h"
#include "store.h"
#include "tablet.h"

#include <core/misc/serialize.h>

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

TPartition::TPartition(TTablet* tablet, int index)
    : Tablet_(tablet)
    , Index_(index)
    , PivotKey_(MinKey())
{ }

TPartition::~TPartition()
{ }

void TPartition::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, PivotKey_);

    Save(context, Stores_.size());
    for (auto store : Stores_) {
        Save(context, store->GetId());
    }
}

void TPartition::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, PivotKey_);

    size_t storeCount = Load<size_t>(context);
    for (size_t index = 0; index < storeCount; ++index) {
        auto storeId = Load<TStoreId>(context);
        auto store = Tablet_->GetStore(storeId);
        YCHECK(Stores_.insert(store).second);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

