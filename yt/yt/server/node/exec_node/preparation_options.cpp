#include "preparation_options.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TChunkNbdVolumeSpec& volumeSpec, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{Kind: Chunk, MediumIndex: %v, DataNodeAddress: %v, MinDataNodeCount: %v, MaxDataNodeCount: %v, "
        "DataNodeRpcTimeout: %v, DataNodeNbdServiceRpcTimeout: %v, "
        "DataNodeNbdServiceMakeTimeout: %v, MasterRpcTimeout: %v, MultiplexingParallelism: %v}",
        volumeSpec.MediumIndex,
        volumeSpec.DataNodeAddress,
        volumeSpec.MinDataNodeCount,
        volumeSpec.MaxDataNodeCount,
        volumeSpec.DataNodeRpcTimeout,
        volumeSpec.DataNodeNbdServiceRpcTimeout,
        volumeSpec.DataNodeNbdServiceMakeTimeout,
        volumeSpec.MasterRpcTimeout,
        volumeSpec.MultiplexingParallelism);
}

void FormatValue(TStringBuilderBase* builder, const TSandboxNbdRootVolumeSpec& volumeSpec, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{DeviceSize: %v, FilesystemType: %v, BackendSpec: %v}",
        volumeSpec.DeviceSize,
        volumeSpec.FilesystemType,
        volumeSpec.BackendSpec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
