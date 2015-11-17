#include "stdafx.h"
#include "public.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

const Stroka PrimaryLockName("<primary>");

const Stroka SystemColumnNamePrefix("$");
const Stroka TableIndexColumnName = SystemColumnNamePrefix + "table_index";
const Stroka RowIndexColumnName = SystemColumnNamePrefix + "row_index";
const Stroka RangeIndexColumnName = SystemColumnNamePrefix + "range_index";

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
