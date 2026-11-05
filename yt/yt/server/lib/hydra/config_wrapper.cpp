#include "config_wrapper.h"

#include "config.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TConfigWrapper::TConfigWrapper(TDistributedHydraManagerConfigPtr config)
    : Config_(config)
{ }

void TConfigWrapper::Set(TDistributedHydraManagerConfigPtr config)
{
    Config_.Store(config);
    Changed_.Fire(config);
}

TDistributedHydraManagerConfigPtr TConfigWrapper::Get() const
{
    return Config_.Acquire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
