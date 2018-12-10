#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TSignalerArg
    : public NYTree::TYsonSerializable
{
    std::vector<int> Pids;
    TString SignalName;

    TSignalerArg();
};

DEFINE_REFCOUNTED_TYPE(TSignalerArg)

////////////////////////////////////////////////////////////////////////////////

void SendSignal(const std::vector<int>& pids, const TString& signalName);
std::optional<int> FindSignalIdBySignalName(const TString& signalName);
void ValidateSignalName(const TString& signalName);

////////////////////////////////////////////////////////////////////////////////

struct TSignalerTool
{
    void operator()(const TSignalerArgPtr& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
