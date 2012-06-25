#pragma once

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

NYTree::TYPath PreprocessYPath(const NYTree::TYPath& path);
std::vector<NYTree::TYPath> PreprocessYPaths(const std::vector<NYTree::TYPath>& paths);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
