#include "logging_helpers.h"

#include "type_helpers.h"

#include <util/generic/yexception.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

std::string CurrentExceptionText()
{
    return ToStdString(::CurrentExceptionMessage());
}

} // namespace NClickHouse
} // namespace NYT
