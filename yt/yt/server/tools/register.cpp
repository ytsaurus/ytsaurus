#include "registry.h"
#include "proc.h"
#include "signaler.h"

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

REGISTER_TOOL(TSignalerTool)
REGISTER_TOOL(TReadProcessSmapsTool)
REGISTER_TOOL(TKillAllByUidTool)
REGISTER_TOOL(TRemoveDirAsRootTool)
REGISTER_TOOL(TCreateDirectoryAsRootTool)
REGISTER_TOOL(TSpawnShellTool)
REGISTER_TOOL(TRemoveDirContentAsRootTool)
REGISTER_TOOL(TMountTmpfsAsRootTool)
REGISTER_TOOL(TUmountAsRootTool)
REGISTER_TOOL(TSetThreadPriorityAsRootTool)
REGISTER_TOOL(TFSQuotaTool)
REGISTER_TOOL(TChownChmodTool)
REGISTER_TOOL(TCopyDirectoryContentTool)
REGISTER_TOOL(TGetDirectorySizesAsRootTool)
REGISTER_TOOL(TRootDirectoryBuilderTool)
REGISTER_TOOL(TMkFsAsRootTool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools
