#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<TString> ToNames(const std::vector<T>& objects)
{
    std::vector<TString> names;
    names.reserve(objects.size());
    for (const auto* object : objects) {
        names.push_back(object->GetName());
    }
    return names;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
