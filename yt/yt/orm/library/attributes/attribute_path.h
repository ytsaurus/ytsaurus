#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

void ValidateAttributePath(const NYPath::TYPath& path);

//! One of them is an ancestor of the another.
//! The attribute paths are supposed to be valid.
bool AreAttributesRelated(const NYPath::TYPath& lhs, const NYPath::TYPath& rhs);

//! |pattern| may contain asterisks.
//! |path| must be free of asterisks.
EAttributePathMatchResult MatchAttributePathToPattern(const NYPath::TYPath& pattern, const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
