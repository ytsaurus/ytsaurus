#include "private.h"

#include <yt/yt/server/tools/public.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

const std::string ShellToolDirectory("/yt_runtime");
const std::string ShellToolPath(NFS::CombinePaths(ShellToolDirectory, std::string(NTools::ToolsProgramName)));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
