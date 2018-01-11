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

    TJobSizeAdjusterConfig();
};

DEFINE_REFCOUNTED_TYPE(TJobSizeAdjusterConfig)

////////////////////////////////////////////////////////////////////////////////

class TIntermediateChunkScraperConfig
    : public NChunkClient::TChunkScraperConfig
{
public:
    TDuration RestartTimeout;

    TIntermediateChunkScraperConfig();
};

DEFINE_REFCOUNTED_TYPE(TIntermediateChunkScraperConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
