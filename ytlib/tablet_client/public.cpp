#include "public.h"

namespace NYT {
namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

const Stroka TReplicationLogTable::ChangeTypeColumnName("change_type");
const Stroka TReplicationLogTable::KeyColumnNamePrefix("key:");
const Stroka TReplicationLogTable::ValueColumnNamePrefix("value:");
const Stroka TReplicationLogTable::FlagsColumnNamePrefix("flags:");

////////////////////////////////////////////////////////////////////////////////

const TTabletCellId NullTabletCellId;
const TTabletId NullTabletId;
const TStoreId NullStoreId;
const TPartitionId NullPartitionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

