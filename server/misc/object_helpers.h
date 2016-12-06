#pragma once

#include <util/generic/stroka.h>

#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<Stroka> ToNames(const std::vector<T>& objects);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

#define OBJECT_HELPERS_INL_H_
#include "object_helpers-inl.h"
#undef OBJECT_HELPERS_INL_H_

