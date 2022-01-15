#pragma once

#include <util/generic/string.h>

#include <vector>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<TString> ToNames(const std::vector<T>& objects);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_

