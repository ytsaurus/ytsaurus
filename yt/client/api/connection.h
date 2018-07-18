#pragma once

#include "public.h"

#include <yt/client/chunk_client/public.h>

#include <yt/client/object_client/public.h>

#include <yt/client/security_client/public.h>

#include <yt/client/tablet_client/public.h>

#include <yt/client/hive/public.h>

#include <yt/core/actions/callback.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct TAdminOptions
{ };

struct TClientOptions
{
    explicit TClientOptions(const TString& user = NSecurityClient::GuestUserName)
        : User(user)
    { }

    TString User;
    TNullable<TString> Token;
    TNullable<TString> SessionId;
    TNullable<TString> SslSessionId;
};

struct TTransactionParticipantOptions
{
    TDuration RpcTimeout = TDuration::Seconds(5);
};

////////////////////////////////////////////////////////////////////////////////

//! Represents an established connection with a YT cluster.
/*
 *  IConnection instance caches most of the stuff needed for fast interaction
 *  with the cluster (e.g. connection channels, mount info etc).
 *
 *  Thread affinity: any
 */
struct IConnection
    : public virtual TRefCounted
{
    virtual NObjectClient::TCellTag GetCellTag() = 0;

    virtual IInvokerPtr GetInvoker() = 0;

    virtual IAdminPtr CreateAdmin(const TAdminOptions& options = TAdminOptions()) = 0;
    virtual IClientPtr CreateClient(const TClientOptions& options = TClientOptions()) = 0;
    virtual NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
        const NHiveClient::TCellId& cellId,
        const TTransactionParticipantOptions& options = TTransactionParticipantOptions()) = 0;

    virtual void ClearMetadataCaches() = 0;

    virtual void Terminate() = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

