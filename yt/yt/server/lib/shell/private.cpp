#include "private.h"

#include <yt/yt/ytlib/tools/public.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

const TString ShellToolDirectory("/yt_runtime");
const TString ShellToolPath(NFS::CombinePaths(ShellToolDirectory, NTools::ToolsProgramName));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell

