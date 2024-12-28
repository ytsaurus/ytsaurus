#pragma once

#include "public.h"

#include <yt/yt/library/fusion/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IDaemonBootstrap
    : public virtual TRefCounted
{
    virtual TFuture<void> Run() = 0;
};

DEFINE_REFCOUNTED_TYPE(IDaemonBootstrap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
