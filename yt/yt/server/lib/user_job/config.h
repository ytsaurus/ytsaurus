#pragma once

#include "public.h"

#include <yt/yt/library/pipe_io/pipe.h>

#include <yt/yt/core/bus/tcp/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NUserJob {

////////////////////////////////////////////////////////////////////////////////

inline const std::string DefaultExecutorStderrPath("logs/ytserver_exec_stderr");

////////////////////////////////////////////////////////////////////////////////

struct TUserJobSynchronizerConnectionConfig
    : public NYTree::TYsonStruct
{
    //! User job -> Job proxy connection config.
    NBus::TBusClientConfigPtr BusClientConfig;

    REGISTER_YSON_STRUCT(TUserJobSynchronizerConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobSynchronizerConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EStdoutUnusedAction,
    (Leave)
    (Close)
    (RedirrectToDevNull)
);

struct TUserJobExecutorConfig
    : public NYTree::TYsonStruct
{
    //! Command to execute.
    std::string Command;

    //! Pipes to redirect into user job.
    std::vector<NPipeIO::TNamedPipeConfigPtr> Pipes;

    //! Id of the running job.
    std::string JobId;

    //! Environment variables in format "key=value" to set in user job.
    std::vector<std::string> Environment;

    //! User to impersonate before spawning a child process.
    int Uid = -1;

    std::optional<int> Pty;

    //! Whether to adjust resource limits to allow core dumps.
    bool EnableCoreDump = false;

    std::string StderrPath;

    //! Config of the connection between user job executor and job proxy.
    NUserJob::TUserJobSynchronizerConnectionConfigPtr UserJobSynchronizerConnectionConfig;

    EStdoutUnusedAction StdoutUnusedAction;

    REGISTER_YSON_STRUCT(TUserJobExecutorConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJob
