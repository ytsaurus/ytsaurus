#pragma once

#include <ytlib/ypath/rich.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

NYPath::TRichYPath PreprocessYPath(const NYPath::TRichYPath& path);
std::vector<NYPath::TRichYPath> PreprocessYPaths(const std::vector<NYPath::TRichYPath>& paths);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
