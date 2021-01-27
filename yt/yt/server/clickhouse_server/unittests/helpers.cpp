#include "helpers.h"

#include <yt/client/table_client/logical_type.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TLogicalType& type, std::ostream* os)
{
    *os << ToString(type);
}

void PrintTo(const TLogicalTypePtr& type, std::ostream* os)
{
    PrintTo(*type, os);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
