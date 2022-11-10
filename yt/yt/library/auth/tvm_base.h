#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct IServiceTicketAuth
    : public virtual TRefCounted
{
    virtual TString IssueServiceTicket() = 0;
};

DEFINE_REFCOUNTED_TYPE(IServiceTicketAuth)

////////////////////////////////////////////////////////////////////////////////

class TServiceTicketFixedAuth
    : public IServiceTicketAuth
{
public:
    explicit TServiceTicketFixedAuth(const TString& ticket);

    TString IssueServiceTicket() override;

private:
    TString Ticket_;
};

DEFINE_REFCOUNTED_TYPE(TServiceTicketFixedAuth)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
