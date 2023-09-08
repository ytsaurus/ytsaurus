#include "config.h"
#include "cri_api.h"

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

void TCriExecutorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("runtime_endpoint", &TThis::RuntimeEndpoint)
        .Default(TString(DefaultCriEndpoint));

    registrar.Parameter("image_endpoint", &TThis::ImageEndpoint)
        .Default(TString(DefaultCriEndpoint));

    registrar.Parameter("namespace", &TThis::Namespace)
        .NonEmpty();

    registrar.Parameter("runtime_handler", &TThis::RuntimeHandler)
        .Optional();

    registrar.Parameter("base_cgroup", &TThis::BaseCgroup)
        .NonEmpty();

    registrar.Parameter("cpu_period", &TThis::CpuPeriod)
        .Default(TDuration::MilliSeconds(100));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
