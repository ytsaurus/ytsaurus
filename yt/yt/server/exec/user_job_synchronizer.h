#pragma once

#include "public.h"

#include <yt/yt/server/lib/user_job/public.h>

namespace NYT::NExec {

////////////////////////////////////////////////////////////////////////////////

//! Represents the "client side", where user job can send notifications.
struct IUserJobSynchronizerClient
    : public TRefCounted
{
    virtual void NotifyExecutorPrepared() = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobSynchronizerClient)

////////////////////////////////////////////////////////////////////////////////

IUserJobSynchronizerClientPtr CreateUserJobSynchronizerClient(
    NUserJob::TUserJobSynchronizerConnectionConfigPtr connectionConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExec
