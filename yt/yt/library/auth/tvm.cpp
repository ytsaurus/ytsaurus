#include "tvm.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

TServiceTicketClientAuth::TServiceTicketClientAuth(const TTvmClientPtr& tvmClient)
    : TvmClient_(tvmClient)
{ }

TString TServiceTicketClientAuth::IssueServiceTicket() {
    return TvmClient_->GetServiceTicketFor(PROXY_TVM_ID);
}

////////////////////////////////////////////////////////////////////////////////

TServiceTicketFixedAuth::TServiceTicketFixedAuth(const TString& ticket)
    : Ticket_(ticket)
{ }

TString TServiceTicketFixedAuth::IssueServiceTicket() {
    return Ticket_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth