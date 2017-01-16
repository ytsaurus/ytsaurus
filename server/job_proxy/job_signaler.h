#pragma once

#include "public.h"

#include <yt/core/ytree/serialize.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TJobSignalerArg
    : public NYTree::TYsonSerializable
{
    std::vector<int> Pids;
    Stroka SignalName;

    TJobSignalerArg();
};

DEFINE_REFCOUNTED_TYPE(TJobSignalerArg)

////////////////////////////////////////////////////////////////////////////////

void SendSignal(const std::vector<int>& pids, const Stroka& signalName);

////////////////////////////////////////////////////////////////////////////////

struct TJobSignalerTool
{
    void operator()(const TJobSignalerArgPtr& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
