#pragma once

#include "public.h"
#include "tvm_base.h"

#include <library/cpp/tvmauth/client/facade.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

using TTvmClientPtr = std::shared_ptr<NTvmAuth::TTvmClient>;

class TServiceTicketClientAuth
    : public IServiceTicketAuth
{
public:
    explicit TServiceTicketClientAuth(TTvmClientPtr tvmClient);

    TString IssueServiceTicket() override;

private:
    static constexpr NTvmAuth::TTvmId ProxyTvmId = 2031010;

    const TTvmClientPtr TvmClient_;
};

DEFINE_REFCOUNTED_TYPE(TServiceTicketClientAuth)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
