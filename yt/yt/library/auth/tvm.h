#pragma once

#include "public.h"

#include <library/cpp/tvmauth/client/facade.h>

namespace NYT::NAuth {

using TTvmClientPtr = std::shared_ptr<NTvmAuth::TTvmClient>;

////////////////////////////////////////////////////////////////////////////////

class IServiceTicketAuth
    : public virtual TRefCounted
{
public:
    virtual TString IssueServiceTicket() = 0;
};
DEFINE_REFCOUNTED_TYPE(IServiceTicketAuth)

////////////////////////////////////////////////////////////////////////////////

class TServiceTicketClientAuth
    : public IServiceTicketAuth
{
public:
    TServiceTicketClientAuth(const TTvmClientPtr& tvmClient);

    virtual TString IssueServiceTicket() override;

private:
    static constexpr NTvmAuth::TTvmId PROXY_TVM_ID = 2031010;

    TTvmClientPtr TvmClient_;
};
DEFINE_REFCOUNTED_TYPE(TServiceTicketClientAuth)

////////////////////////////////////////////////////////////////////////////////

class TServiceTicketFixedAuth
    : public IServiceTicketAuth
{
public:
    TServiceTicketFixedAuth(const TString& ticket);

    virtual TString IssueServiceTicket() override;

private:
    TString Ticket_;
};
DEFINE_REFCOUNTED_TYPE(TServiceTicketFixedAuth)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy