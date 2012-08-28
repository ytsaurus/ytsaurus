#pragma once

#include <ytlib/ytree/ypath.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

NYTree::TRichYPath PreprocessYPath(const NYTree::TRichYPath& path);
std::vector<NYTree::TRichYPath> PreprocessYPaths(const std::vector<NYTree::TRichYPath>& paths);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
