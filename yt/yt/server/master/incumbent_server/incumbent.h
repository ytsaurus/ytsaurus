#pragma once

#include "public.h"

#include <yt/yt/ytlib/incumbent_client/incumbent_descriptor.h>

namespace NYT::NIncumbentServer {

////////////////////////////////////////////////////////////////////////////////

struct IIncumbent
    : public virtual TRefCounted
{
    virtual EIncumbentType GetType() const = 0;

    virtual void OnIncumbencyStarted(int shardIndex) = 0;
    virtual void OnIncumbencyFinished(int shardIndex) = 0;
};

DEFINE_REFCOUNTED_TYPE(IIncumbent)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
