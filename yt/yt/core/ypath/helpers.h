#pragma once

#include "public.h"

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

std::optional<TYPath> TryComputeYPathSuffix(const TYPath& path, const TYPath& prefix);

//! Split path into dirname part and basename part (a-la corresponding bash commands).
//! BaseName part is considered to be the last non-empty YPath token.
//! DirName part is stripped off trailing slash (if any).
std::pair<TYPath, TString> DirNameAndBaseName(const TYPath& path);

//! Check if path contains attribute designation by looking for @ token in it.
bool IsPathPointingToAttributes(const TYPath& path);

TYPath YPathJoin(const TYPath& path, TStringBuf literal);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
