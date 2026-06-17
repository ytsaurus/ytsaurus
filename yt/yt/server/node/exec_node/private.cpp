#include "private.h"

#include <yt/yt/core/ytree/fluent.h>

using namespace NYT::NExecNode;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeMount::operator==(const TVolumeMount& rhs) const
{
    return VolumeId == rhs.VolumeId && MountPath == rhs.MountPath && ReadOnly == rhs.ReadOnly;
}

bool TVolumeMount::operator!=(const TVolumeMount& rhs) const
{
    return !(*this == rhs);
}

////////////////////////////////////////////////////////////////////////////////
