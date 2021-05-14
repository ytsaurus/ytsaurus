#include "sampler.h"

namespace NYT::NTracing {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSampler::TSampler()
    : Config_(New<TSamplerConfig>())
{ }

TSampler::TSampler(const TSamplerConfigPtr& config)
    : Config_(config)
{ }

bool TSampler::IsTraceSampled(const TString& user)
{
    TSamplerConfigPtr config = Config_.Load();

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

    return false;
}

void TSampler::UpdateConfig(const TSamplerConfigPtr& config)
{
    Config_.Store(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
