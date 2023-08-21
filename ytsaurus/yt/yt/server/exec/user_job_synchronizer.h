#pragma once

#include "public.h"

#include <yt/yt/core/bus/tcp/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/server/lib/user_job/public.h>

namespace NYT::NUserJob {

////////////////////////////////////////////////////////////////////////////////

//! Represents the "client side", where user job can send notifications.
struct IUserJobSynchronizerClient
    : public TRefCounted
{
    virtual void NotifyExecutorPrepared() = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobSynchronizerClient)

////////////////////////////////////////////////////////////////////////////////

IUserJobSynchronizerClientPtr CreateUserJobSynchronizerClient(TUserJobSynchronizerConnectionConfigPtr connectionConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJob
