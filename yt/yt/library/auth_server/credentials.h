#pragma once

#include "public.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct TTokenCredentials
{
    TString Token;
    NNet::TNetworkAddress UserIP;

    bool operator==(const TTokenCredentials&) const = default;
};

struct TCookieCredentials
{
    // NB: Since requests are caching, pass only required
    // subset of cookies here.
    THashMap<TString, TString> Cookies;
    NNet::TNetworkAddress UserIP;

    bool operator==(const TCookieCredentials&) const = default;
};

struct TTicketCredentials
{
    TString Ticket;
};

struct TServiceTicketCredentials
{
    TString Ticket;
};

struct TAuthenticationResult
{
    TString Login;
    TString Realm;
    TString UserTicket;

    //! If set, client is advised to set this cookie.
    std::optional<TString> SetCookie;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

template <>
struct THash<NYT::NAuth::TTokenCredentials>
{
    size_t operator()(const NYT::NAuth::TTokenCredentials& credentials) const;
};

template <>
struct THash<NYT::NAuth::TCookieCredentials>
{
    size_t operator()(const NYT::NAuth::TCookieCredentials& credentials) const;
};
