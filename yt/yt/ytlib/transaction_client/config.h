#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

struct TPingBatcherConfig
    : public NYTree::TYsonStruct
{
    // COMPAT(gryzlov-ad): Remove when all masters support PingTransactions RPC.
    bool Enable;

    TDuration BatchPeriod;
    i64 BatchSize;

    REGISTER_YSON_STRUCT(TPingBatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPingBatcherConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTransactionManagerConfig
    : public NRpc::TRetryingChannelConfig
{
    //! Timeout for all RPC requests to participants.
    TDuration RpcTimeout;

    //! Default internal between consecutive transaction pings.
    TDuration DefaultPingPeriod;

    //! Default transaction timeout to be used if not given explicitly on
    //! transaction start.
    TDuration DefaultTransactionTimeout;

    bool UseCypressTransactionService;

    TPingBatcherConfigPtr PingBatcher;

    REGISTER_YSON_STRUCT(TTransactionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TClockManagerConfig
    : public NYTree::TYsonStruct
{
    NObjectClient::TCellTag ClockClusterTag;

    TClockManagerConfigPtr ApplyDynamic(const TDynamicClockManagerConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TClockManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClockManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicClockManagerConfig
    : public NYTree::TYsonStruct
{
    std::optional<NObjectClient::TCellTag> ClockClusterTag;

    REGISTER_YSON_STRUCT(TDynamicClockManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicClockManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
