#include "preparation_options.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TSandboxNbdRootVolumeData& data, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{Size: %v, MediumIndex: %v, FsType: %v, DataNodeAddress: %v, "
        "MinDataNodeCount: %v, MaxDataNodeCount: %v, "
        "DataNodeRpcTimeout: %v, DataNodeNbdServiceRpcTimeout: %v, "
        "DataNodeNbdServiceMakeTimeout: %v, MasterRpcTimeout: %v}",
        data.Size,
        data.MediumIndex,
        data.FsType,
        data.DataNodeAddress,
        data.MinDataNodeCount,
        data.MaxDataNodeCount,
        data.DataNodeRpcTimeout,
        data.DataNodeNbdServiceRpcTimeout,
        data.DataNodeNbdServiceMakeTimeout,
        data.MasterRpcTimeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
