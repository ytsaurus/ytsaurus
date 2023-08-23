#pragma once

#include <yt/cpp/roren/interface/fwd.h>
#include <yt/cpp/roren/yt/proto/config.pb.h>

#include "yt_read.h"
#include "yt_write.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TPipeline MakeYtPipeline(const TString& cluster, const TString& workingDir);
TPipeline MakeYtPipeline(TYtPipelineConfig config);

////////////////////////////////////////////////////////////////////////////////

template <typename TState>
TPState<typename TState::TKey, TState> MakeYtProfilePState(const TPipeline& YtPipeline/*, const NBigRT::TProfileStateManagerConfig& managerConfig, const TBigRtStateConfig& config*/)
{
    Y_UNUSED(YtPipeline);
#if 0
    auto factory = NYT::New<NBigRT::TProfileManagerFactory<TState>>(managerConfig, NYT::GetCurrentInvoker());
    return NRoren::MakeBigRtPState<typename TState::TKey, TState>(
        bigRtPipeline,
        [factory=std::move(factory), config=config](ui64 shard, NSFStats::TSolomonContext solomonContext) -> NBigRT::TProfileManagerBasePtr<TState>
        {
            return factory->Make(shard, solomonContext);
        },
        config
    );
#endif
    auto rawPipeline = NPrivate::GetRawPipeline(YtPipeline);
    auto pState = NPrivate::MakePState<typename TState::TKey, TState>(rawPipeline);
    // TODO: make pState for Yt pipeline
    return pState;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
