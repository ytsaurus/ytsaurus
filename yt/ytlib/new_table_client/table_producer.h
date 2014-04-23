#pragma once

#include "public.h"

#include "unversioned_row.h"

#include <core/yson/public.h>
#include <core/misc/nullable.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

void ProduceRow(NYson::IYsonConsumer* consumer, TUnversionedRow row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
