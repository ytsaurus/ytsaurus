#pragma once

#include "public.h"

#include <library/cpp/tvmauth/client/facade.h>

namespace NYT::NAuth {

using TTvmClientPtr = std::shared_ptr<NTvmAuth::TTvmClient>;

////////////////////////////////////////////////////////////////////////////////

struct IServiceTicketAuth
    : public virtual TRefCounted
{
    virtual TString IssueServiceTicket() = 0;
};

DEFINE_REFCOUNTED_TYPE(IServiceTicketAuth)

////////////////////////////////////////////////////////////////////////////////

class TServiceTicketClientAuth
    : public IServiceTicketAuth
{
public:
    explicit TServiceTicketClientAuth(const TTvmClientPtr& tvmClient);

    TString IssueServiceTicket() override;

private:
    static constexpr NTvmAuth::TTvmId ProxyTvmId = 2031010;

    TTvmClientPtr TvmClient_;
};

DEFINE_REFCOUNTED_TYPE(TServiceTicketClientAuth)

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
