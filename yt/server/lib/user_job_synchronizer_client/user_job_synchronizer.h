#pragma once

#include "public.h"

#include <yt/core/bus/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NUserJobSynchronizerClient {

////////////////////////////////////////////////////////////////////////////////

class TUserJobSynchronizerConnectionConfig
    : public NYTree::TYsonSerializable
{
public:
    //! User job -> Job proxy connection config.
    NBus::TTcpBusClientConfigPtr BusClientConfig;

    TUserJobSynchronizerConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TUserJobSynchronizerConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

//! Represents the "client side", where user job can send notifications.
struct IUserJobSynchronizerClient
    : public TRefCounted
{
    virtual void NotifyExecutorPrepared() = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobSynchronizerClient)

////////////////////////////////////////////////////////////////////////////////

IUserJobSynchronizerClientPtr CreateUserJobSynchronizerClient(TUserJobSynchronizerConnectionConfigPtr connectionConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJobSynchronizerClient
