#pragma once

#include "public.h"

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NUserJobSynchronizerClient {

////////////////////////////////////////////////////////////////////////////////

class TUserJobSynchronizerConnectionConfig
    : public NYTree::TYsonStruct
{
public:
    //! User job -> Job proxy connection config.
    NBus::TTcpBusClientConfigPtr BusClientConfig;

    REGISTER_YSON_STRUCT(TUserJobSynchronizerConnectionConfig);

    static void Register(TRegistrar registrar);
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
