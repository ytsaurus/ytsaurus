#pragma once

#include <yt/client/table_client/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TLogicalType& type, std::ostream* os);
void PrintTo(const TLogicalTypePtr& type, std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
