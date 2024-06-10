#pragma once

#include "stateful_timer_impl/stateful_timer_par_do.h"
#include "environment.h"

#include <bigrt/lib/consuming_system/interface/interface.h>

#include <yt/cpp/roren/interface/executor.h>
#include <yt/cpp/roren/library/timers/timers.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TBigRtPipelineExecutor: public IExecutor
{
public:
    TBigRtPipelineExecutor(TBigRtEnvironmentPtr env);

    void Start(const TPipeline& pipeline);
    void Finish();

    void Run(const TPipeline& pipeline) override;

    NPrivate::TTimers& GetTimersOrThrow(uint64_t shardId);
    NPrivate::TTimers& GetTimersOrCreate(const TString& cluster, const TString& ytPath, uint64_t shardId);

private:
    TBigRtEnvironmentPtr Env_;
    THashMap<uint64_t, std::unique_ptr<NPrivate::TTimers>> Timers_;

    TVector<NBigRT::TConsumingSystemPtr> ConsSystems_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
