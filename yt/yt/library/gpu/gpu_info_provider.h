#pragma once

#include "public.h"

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NGpu {

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
    i64 ClocksSM = 0;
    i64 ClocksMaxSM = 0;
    double SMUtilizationRate = 0.0;
    double SMOccupancyRate = 0.0;
    double NvlinkRxByteRate = 0.0;
    double NvlinkTxByteRate = 0.0;
    double PcieRxByteRate = 0.0;
    double PcieTxByteRate = 0.0;

    struct
    {
        bool Status = false;
        std::optional<TInstant> LastTransitionTime;
    } Stuck;
};

void FormatValue(TStringBuilderBase* builder, const TGpuInfo& gpuInfo, TStringBuf /*format*/);
void Serialize(const TGpuInfo& gpuInfo, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TRdmaDeviceInfo
{
    TString Name;
    double RxByteRate = 0.0;
    double TxByteRate = 0.0;
};

////////////////////////////////////////////////////////////////////////////////

struct IGpuInfoProvider
    : public TRefCounted
{
    virtual std::vector<TGpuInfo> GetGpuInfos(TDuration timeout) const = 0;
    virtual std::vector<TRdmaDeviceInfo> GetRdmaDeviceInfos(TDuration timeout) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IGpuInfoProvider)

////////////////////////////////////////////////////////////////////////////////

IGpuInfoProviderPtr CreateGpuInfoProvider(TGpuInfoSourceConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
