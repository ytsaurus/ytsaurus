#pragma once

#include "public.h"
#include "private.h"

#include "config.h"

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TCondition
{
    bool Status = false;
    std::optional<TInstant> LastTransitionTime;
};

void FormatValue(TStringBuilderBase* builder, const TCondition& gpuInfo, TStringBuf /*format*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

struct TGpuInfo
{
    TInstant UpdateTime;

    int Index = -1;
    TString Name;

    double UtilizationGpuRate = 0.0;
    double UtilizationMemoryRate = 0.0;
    i64 MemoryUsed = 0;
    i64 MemoryTotal = 0;
    double PowerDraw = 0.0;
    double PowerLimit = 0.0;
    i64 ClocksSm = 0;
    i64 ClocksMaxSm = 0;
    double SmUtilizationRate = 0.0;
    double SmOccupancyRate = 0.0;
    NDetail::TCondition Stuck;
};

void FormatValue(TStringBuilderBase* builder, const TGpuInfo& gpuInfo, TStringBuf /*format*/);
void Serialize(const TGpuInfo& gpuInfo, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct IGpuInfoProvider
    : public TRefCounted
{
    virtual std::vector<TGpuInfo> GetGpuInfos(TDuration checkTimeout) = 0;
};

DEFINE_REFCOUNTED_TYPE(IGpuInfoProvider)

IGpuInfoProviderPtr CreateGpuInfoProvider(const TGpuInfoSourceConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
