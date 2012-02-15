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
    typedef TIntrusivePtr<TRedirectorService> TPtr;

    TRedirectorService(NCypress::TCypressManager* cypressManager);

protected:
    NCypress::TCypressManager::TPtr CypressManager;

    virtual TAsyncRedirectResult HandleRedirect(NRpc::IServiceContext* context);
    TRedirectResult DoHandleRedirect(NRpc::IServiceContext::TPtr context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
