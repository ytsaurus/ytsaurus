#include "private.h"

#include <yt/yt/server/tools/public.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

const TString ShellToolDirectory("/yt_runtime");
const TString ShellToolPath(NFS::CombinePaths(ShellToolDirectory, TString(NTools::ToolsProgramName)));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell

