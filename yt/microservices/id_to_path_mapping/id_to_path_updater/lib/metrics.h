#pragma once

#include <yt/cpp/roren/interface/roren.h>

#include <yt/microservices/id_to_path_mapping/id_to_path_updater/lib/messages.pb.h>

class TClusterMetricsFn
    : public NRoren::IDoFn<TIdToPathRow&&, TIdToPathRow>
{
public:
    TClusterMetricsFn() = default;
    TClusterMetricsFn(TString counterName);

    void Do(TInputRow&& row, NRoren::TOutput<TOutputRow>& output);

private:
    NYT::NProfiling::TCounter& GetClusterCounter(TString cluster);

private:
    TString CounterName_;
    THashMap<TString, NYT::NProfiling::TCounter> Counters_;

    Y_SAVELOAD_DEFINE_OVERRIDE(CounterName_);
};
