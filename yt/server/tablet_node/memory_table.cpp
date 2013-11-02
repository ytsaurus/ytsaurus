#include "stdafx.h"
#include "memory_table.h"
#include "tablet.h"
#include "config.h"

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

class TMemoryTable::TRowGroupComparer
{
public:
    explicit TRowGroupComparer(int keyColumnCount)
        : KeyColumnCount_(keyColumnCount)
    { }

    int operator () (TRowGroup lhs, TRowGroup rhs) const
    {
        for (int index = 0; index < KeyColumnCount_; ++index) {
            int result = CompareSameTypeValues(lhs[index], rhs[index]);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

private:
    int KeyColumnCount_;

};

////////////////////////////////////////////////////////////////////////////////

TMemoryTable::TMemoryTable(
    TTabletManagerConfigPtr config,
    TTablet* tablet)
    : Config_(config)
    , Tablet_(tablet)
    , TreePool_(Config_->TreePoolChunkSize, Config_->PoolMaxSmallBlockRatio)
    , RowPool_(Config_->RowPoolChunkSize, Config_->PoolMaxSmallBlockRatio)
    , StringPool_(Config_->StringPoolChunkSize, Config_->PoolMaxSmallBlockRatio)
    , Tree_(new TRcuTree<TRowGroup, TRowGroupComparer>(
        &TreePool_,
        TRowGroupComparer(Tablet_->Schema().Columns().size())))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
