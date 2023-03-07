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
        TReaderGuard guard(Lock_);
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
            TReaderGuard guard(Lock_);
            if (auto it = UserState_.find(user); it != UserState_.end()) {
                state = it->second;
            }
        }

        if (!state) {
            TWriterGuard guard(Lock_);
            if (auto it = UserState_.find(user); it != UserState_.end()) {
                state = it->second;
            } else {
                state = New<TUserState>();
                UserState_[user] = state;
            }
        }

        if (state->SampleCount.fetch_add(1) < config->MinUserTraceCount) {
            return true;
        }
    }
    
    return false;
}

void TSampler::ResetPerUserLimits()
{
    TReaderGuard guard(Lock_);
    for (auto& state : UserState_) {
        state.second->SampleCount = 0;
    }
}

void TSampler::UpdateConfig(const TSamplingConfigPtr& config)
{
    TSamplingConfigPtr newConfig = config;

    {
        TWriterGuard guard(Lock_);
        std::swap(Config_, newConfig);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
