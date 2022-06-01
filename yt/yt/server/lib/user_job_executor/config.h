#pragma once

#include "public.h"

#include <yt/yt/server/lib/user_job_synchronizer_client/public.h>

#include <yt/yt/library/process/pipe.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NUserJobExecutor {

////////////////////////////////////////////////////////////////////////////////

class TUserJobExecutorConfig
    : public NYTree::TYsonStruct
{
public:
    //! Command to execute.
    TString Command;

    //! Pipes to redirect into user job.
    std::vector<NPipes::TNamedPipeConfigPtr> Pipes;

    //! Id of the running job.
    TString JobId;

    //! Environment variables in format "key=value" to set in user job.
    std::vector<TString> Environment;

    //! User to impersonate before spawning a child process.
    int Uid = -1;

    //! Whether to adjust resource limits to allow core dumps.
    bool EnableCoreDump = false;

    //! Config of the connection between user job executor and job proxy.
    NUserJobSynchronizerClient::TUserJobSynchronizerConnectionConfigPtr UserJobSynchronizerConnectionConfig;

    REGISTER_YSON_STRUCT(TUserJobExecutorConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJobExector
