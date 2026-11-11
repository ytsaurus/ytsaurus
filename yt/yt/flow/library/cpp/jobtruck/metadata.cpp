#include "metadata.h"

namespace NYT::NFlow::NJobtruck {

////////////////////////////////////////////////////////////////////////////////

void TSourceMeta::Register(TRegistrar registrar)
{
    registrar.Parameter("kind", &TThis::Kind);
    registrar.Parameter("display_name", &TThis::DisplayName);
    registrar.Parameter("node_info", &TThis::NodeInfo);
}

void TLogConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("log_level", &TThis::LogLevel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NJobtruck
