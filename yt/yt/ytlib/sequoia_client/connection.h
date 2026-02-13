#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NSequoiaClient {

///////////////////////////////////////////////////////////////////////////////

struct ISequoiaConnection
    : public TRefCounted
{
    virtual void Reconfigure(NApi::NNative::TSequoiaConnectionConfigPtr config) = 0;

    virtual ISequoiaClientPtr CreateClient(const NRpc::TAuthenticationIdentity& authenticationIdentity) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaConnection)

////////////////////////////////////////////////////////////////////////////////

ISequoiaConnectionPtr CreateSequoiaConnection(
    NApi::NNative::TSequoiaConnectionConfigPtr config,
    TWeakPtr<NApi::NNative::IConnection> localConnection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
