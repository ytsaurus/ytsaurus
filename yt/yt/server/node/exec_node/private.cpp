#include "private.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TTmpfsVolumeParams& params, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{Size: %v, UserId: %v, VolumeId: %v, Index: %v}",
        params.Size,
        params.UserId,
        params.VolumeId,
        params.Index);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
