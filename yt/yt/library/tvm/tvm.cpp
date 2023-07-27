#include "tvm.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

TServiceTicketClientAuth::TServiceTicketClientAuth(TTvmClientPtr tvmClient)
    : TvmClient_(std::move(tvmClient))
{ }

TString TServiceTicketClientAuth::IssueServiceTicket()
{
    return TvmClient_->GetServiceTicketFor(ProxyTvmId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
