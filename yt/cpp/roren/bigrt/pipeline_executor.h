#pragma once

#include <yt/cpp/roren/bigrt/proto/config.pb.h>
#include <yt/cpp/roren/interface/executor.h>
#include <yt/yt/core/actions/public.h>
#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/tag.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TBigRtPipelineExecutor: public IExecutor
{
public:
    TBigRtPipelineExecutor(
        const TBigRtProgramConfig& config,
        const NYT::NProfiling::TProfiler& profiler,
        const NYT::NProfiling::TTagSet& profilerTags,
        const NYT::TCancelableContextPtr& cancelableContext);

    void Run(const TPipeline& pipeline) override;

private:
    const TBigRtProgramConfig Config_;

    NYT::NProfiling::TProfiler Profiler_;
    NYT::NProfiling::TTagSet ProfilerTags_;

    NYT::TCancelableContextPtr CancelableContext_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
