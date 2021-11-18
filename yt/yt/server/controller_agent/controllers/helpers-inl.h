#ifndef HELPERS_INL_H
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

void TControllerFeatures::AddTag(TString name, auto value)
{
    Tags_[name] = NYson::ConvertToYsonString(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
