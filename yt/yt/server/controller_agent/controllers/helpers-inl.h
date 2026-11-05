#ifndef HELPERS_INL_H
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

void TControllerFeatures::AddTag(const std::string& name, auto value)
{
    Tags_[name] = NYson::ConvertToYsonString(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
