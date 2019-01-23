#include "logging_helpers.h"

#include "type_helpers.h"

#include <util/generic/yexception.h>

namespace NYT {
namespace NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::string CurrentExceptionText()
{
    return ToStdString(::CurrentExceptionMessage());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseServer
} // namespace NYT
