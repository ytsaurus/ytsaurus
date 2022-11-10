#include "tvm.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

TServiceTicketClientAuth::TServiceTicketClientAuth(const TTvmClientPtr& tvmClient)
    : TvmClient_(tvmClient)
{ }

TString TServiceTicketClientAuth::IssueServiceTicket()
{
    return TvmClient_->GetServiceTicketFor(ProxyTvmId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
