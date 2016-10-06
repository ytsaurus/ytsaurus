#pragma once

#include "public.h"

#include <yt/core/ytree/serialize.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TSignalerArg
    : public NYTree::TYsonSerializable
{
    std::vector<int> Pids;
    Stroka SignalName;

    TSignalerArg();
};

DEFINE_REFCOUNTED_TYPE(TSignalerArg)

////////////////////////////////////////////////////////////////////////////////

void SendSignal(const std::vector<int>& pids, const Stroka& signalName);
TNullable<int> FindSignalIdBySignalName(const Stroka& signalName);
void ValidateSignalName(const Stroka& signalName);

////////////////////////////////////////////////////////////////////////////////

struct TSignalerTool
{
    void operator()(const TSignalerArgPtr& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
