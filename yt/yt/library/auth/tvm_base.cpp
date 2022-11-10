#include "tvm_base.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

TServiceTicketFixedAuth::TServiceTicketFixedAuth(const TString& ticket)
    : Ticket_(ticket)
{ }

TString TServiceTicketFixedAuth::IssueServiceTicket()
{
    return Ticket_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
