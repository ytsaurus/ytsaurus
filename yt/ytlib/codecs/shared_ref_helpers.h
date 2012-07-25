#pragma once

#include <ytlib/misc/ref.h>

#include <vector>

namespace NYT {

size_t TotalLength(const std::vector<TSharedRef>& refs);

TSharedRef MergeRefs(const std::vector<TSharedRef>& refs);

} //namespace NYT
