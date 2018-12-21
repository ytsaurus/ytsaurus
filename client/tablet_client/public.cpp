#include "public.h"

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

const TTabletCellId NullTabletCellId;
const TTabletId NullTabletId;
const TStoreId NullStoreId;
const TPartitionId NullPartitionId;

const TString TReplicationLogTable::ChangeTypeColumnName("change_type");
const TString TReplicationLogTable::KeyColumnNamePrefix("key:");
const TString TReplicationLogTable::ValueColumnNamePrefix("value:");
const TString TReplicationLogTable::FlagsColumnNamePrefix("flags:");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

