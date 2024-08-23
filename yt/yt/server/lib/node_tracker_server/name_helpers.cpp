#include "name_helpers.h"

#include <yt/yt/core/misc/error.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ValidateHostName(const std::string& name)
{
    if (name.empty()) {
        THROW_ERROR_EXCEPTION("Host name cannot be empty");
    }
}

void ValidateDataCenterName(const std::string& name)
{
    if (name.empty()) {
        THROW_ERROR_EXCEPTION("Data center name cannot be empty");
    }
}

void ValidateRackName(const std::string& name)
{
    if (name.empty()) {
        THROW_ERROR_EXCEPTION("Rack name cannot be empty");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
