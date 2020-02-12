#pragma once

#include <yt/server/lib/user_job_synchronizer_client/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateUserJobSynchronizerService(
    const NLogging::TLogger& logger,
    NUserJobSynchronizerClient::IUserJobSynchronizerClientPtr jobControl,
    IInvokerPtr controlInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy

