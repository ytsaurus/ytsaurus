#pragma once

#include <yt/core/ytree/yson_serializable.h>

#include <yt/ytlib/chunk_client/config.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TJobSizeAdjusterConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MinJobTime;
    TDuration MaxJobTime;

    double ExecToPrepareTimeRatio;

    TJobSizeAdjusterConfig()
    {
        RegisterParameter("min_job_time", MinJobTime)
            .Default(TDuration::Seconds(60));

        RegisterParameter("max_job_time", MaxJobTime)
            .Default(TDuration::Minutes(10));

        RegisterParameter("exec_to_prepare_time_ratio", ExecToPrepareTimeRatio)
            .Default(20.0);
    }
};

DEFINE_REFCOUNTED_TYPE(TJobSizeAdjusterConfig)

////////////////////////////////////////////////////////////////////////////////

class TIntermediateChunkScraperConfig
    : public NChunkClient::TChunkScraperConfig
{
public:
    TDuration RestartTimeout;

    TIntermediateChunkScraperConfig()
    {
        RegisterParameter("restart_timeout", RestartTimeout)
            .Default(TDuration::Seconds(10));
    }
};

DEFINE_REFCOUNTED_TYPE(TIntermediateChunkScraperConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT