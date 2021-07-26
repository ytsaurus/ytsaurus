#include "sampler.h"

namespace NYT::NTracing {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static NProfiling::TProfiler Profiler{"/jaeger"};

////////////////////////////////////////////////////////////////////////////////

bool TSampler::TUserState::TrySampleByMinCount(ui64 minCount, TCpuDuration period)
{
    if (minCount == 0) {
        return false;
    }

    auto lastReset = LastReset.load();
    auto now = NProfiling::GetCpuInstant();
    if (now - lastReset > period) {
        if (LastReset.compare_exchange_strong(lastReset, now)) {
            Sampled.store(0);
        }
    }

    return Sampled.fetch_add(1) < minCount;
}

TSampler::TSampler()
    : Config_(New<TSamplerConfig>())
    , TracesSampled_(Profiler.WithHot().Counter("/traces_sampled"))
{ }

TSampler::TSampler(const TSamplerConfigPtr& config)
    : Config_(config)
{ }

void TSampler::SampleTraceContext(const TString& user, const TTraceContextPtr& traceContext)
{
    TSamplerConfigPtr config = Config_.Load();

    auto [userState, inserted] = Users_.FindOrInsert(user, [&user] {
        auto state = New<TUserState>();

        auto profiler = Profiler.WithSparse().WithHot().WithTag("user", user);
        state->TracesSampledByUser = profiler.WithSparse().Counter("/traces_sampled_by_user");
        state->TracesSampledByProbability = profiler.WithSparse().Counter("/traces_sampled_by_probability");

        return state;
    });

    if (traceContext->IsSampled()) {
        userState->Get()->TracesSampledByUser.Increment();

        if (config->ClearSampledFlag.find(user) != config->ClearSampledFlag.end()) {
            traceContext->SetSampled(false);
        } else {
            TracesSampled_.Increment();
            return;
        }
    }

    if (config->GlobalSampleRate != 0.0) {
        auto p = RandomNumber<double>();
        if (p < config->GlobalSampleRate) {
            userState->Get()->TracesSampledByProbability.Increment();
            TracesSampled_.Increment();
            traceContext->SetSampled(true);
            return;
        }
    }

    auto it = config->UserSampleRate.find(user);
    if (it != config->UserSampleRate.end()) {
        auto p = RandomNumber<double>();
        if (p < it->second) {
            userState->Get()->TracesSampledByProbability.Increment();
            TracesSampled_.Increment();
            traceContext->SetSampled(true);
            return;
        }
    }

    if (userState->Get()->TrySampleByMinCount(config->MinPerUserSamples, DurationToCpuDuration(config->MinPerUserSamplesPeriod))) {
        TracesSampled_.Increment();
        traceContext->SetSampled(true);
        return;
    }
}

void TSampler::UpdateConfig(const TSamplerConfigPtr& config)
{
    Config_.Store(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
