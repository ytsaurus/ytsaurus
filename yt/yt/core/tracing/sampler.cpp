#include "sampler.h"

#include "config.h"

namespace NYT::NTracing {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSampler::TSampler()
    : Config_(New<TSamplingConfig>())
{ }

TSampler::TSampler(const TSamplingConfigPtr& config)
    : Config_(config)
{ }

bool TSampler::IsTraceSampled(const TString& user)
{
    TSamplingConfigPtr config;

    {
        auto guard = ReaderGuard(Lock_);
        config = Config_;
    }

    if (config->GlobalSampleRate != 0.0) {
        auto p = RandomNumber<double>();
        if (p < config->GlobalSampleRate) {
            return true;
        }
    }

    auto it = config->UserSampleRate.find(user);
    if (it != config->UserSampleRate.end()) {
        auto p = RandomNumber<double>();
        if (p < it->second) {
            return true;
        }
    }

    if (config->MinUserTraceCount != 0) {
        TIntrusivePtr<TUserState> state;
        {
            auto guard = ReaderGuard(Lock_);
            if (auto it = UserState_.find(user); it != UserState_.end()) {
                state = it->second;
            }
        }

        if (!state) {
            auto guard = WriterGuard(Lock_);
            if (auto it = UserState_.find(user); it != UserState_.end()) {
                state = it->second;
            } else {
                state = New<TUserState>();
                UserState_[user] = state;
            }
        }

        if (state->SampleCount.fetch_add(1) < static_cast<ui64>(config->MinUserTraceCount)) {
            return true;
        }
    }

    return false;
}

void TSampler::ResetPerUserLimits()
{
    auto guard = ReaderGuard(Lock_);
    for (auto& state : UserState_) {
        state.second->SampleCount = 0;
    }
}

void TSampler::UpdateConfig(const TSamplingConfigPtr& config)
{
    TSamplingConfigPtr newConfig = config;

    {
        auto guard = WriterGuard(Lock_);
        std::swap(Config_, newConfig);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
