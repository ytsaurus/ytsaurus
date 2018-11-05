#include "logging_helpers.h"

#include "type_helpers.h"

#include <util/generic/yexception.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

std::string CurrentExceptionText()
{
    return ToStdString(::CurrentExceptionMessage());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
