#pragma once
#ifndef OBJECT_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include object_helpers.h"
// For the sake of sane code completion.
#include "object_helpers.h"
#endif

namespace NYT {
namespace {

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

} // namespace
} // namespace NYT

