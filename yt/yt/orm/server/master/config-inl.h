#ifndef CONFIG_INL_H_
#error "Direct inclusion of this file is not allowed, include config.h"
// For the sake of sane code completion.
#include "config.h"
#endif

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

template <bool AuthenticationRequired>
void TDataModelYTConnectorConfig<AuthenticationRequired>::Register(TRegistrar registrar)
{
    TYTConnectorConfig::DoRegister(registrar, AuthenticationRequired);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
