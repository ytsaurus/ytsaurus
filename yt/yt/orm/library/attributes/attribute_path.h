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

using TSplitResult = std::tuple<NYPath::TYPath, std::optional<NYPath::TYPath>>;
// return first literal from the path (with starting slash) if any.
TSplitResult GetAttributePathRoot(const NYPath::TYPath& path, int rootLength = 1);
// split pattern by asterisk
TSplitResult SplitPatternByAsterisk(const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
