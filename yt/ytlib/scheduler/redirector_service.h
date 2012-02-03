#pragma once

#include <ytlib/rpc/redirector_service_base.h>
#include <ytlib/cypress/cypress_manager.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TRedirectorService
    : public NRpc::TRedirectorServiceBase
{
public:
    TRedirectorService(NCypress::TCypressManager* cypressManager);

protected:
    class TConfig;

    NCypress::TCypressManager::TPtr CypressManager;

    virtual TRedirectParams GetRedirectParams(NRpc::IServiceContext* context) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
