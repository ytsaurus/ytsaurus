#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

bool IsAttributePath(TStringBuf path);

void ValidateAttributePath(NYPath::TYPathBuf path);

//! One of them is an ancestor of the another.
//! The attribute paths are supposed to be valid.
bool AreAttributesRelated(NYPath::TYPathBuf lhs, NYPath::TYPathBuf rhs);

//! |pattern| may contain asterisks.
//! |path| must be free of asterisks.
EAttributePathMatchResult MatchAttributePathToPattern(NYPath::TYPathBuf pattern, NYPath::TYPathBuf path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
