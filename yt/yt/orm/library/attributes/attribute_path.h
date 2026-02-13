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

using TSplitResult = std::pair<std::optional<NYPath::TYPath>, NYPath::TYPath>;
// If the path is a full prefix of the pattern (up to the list-index indicators on asterisk places)
// returns the prefix(from patterm) and remaining pattern suffix.
TSplitResult TryConsumePrefix(const NYPath::TYPath& pattern, const NYPath::TYPath& path);
// Return first literal from the path (with starting slash) if any.
TSplitResult GetAttributePathRoot(const NYPath::TYPath& path, int rootLength = 1);

// Split pattern by asterisk. Always return first part, optional part is set if there is asterisk.
std::pair<NYPath::TYPath, std::optional<NYPath::TYPath>> SplitPatternByAsterisk(const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
