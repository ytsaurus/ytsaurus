#pragma once

#include "public.h"

#include <yt/yt/server/lib/cellar_agent/config.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

class TChaosManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TChaosManagerConfig()
    { }
};

DEFINE_REFCOUNTED_TYPE(TChaosManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxTransactionTimeout;
    int MaxAbortedTransactionPoolSize;

    TTransactionManagerConfig()
    {
        RegisterParameter("max_transaction_timeout", MaxTransactionTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(60));
        RegisterParameter("max_aborted_transaction_pool_size", MaxAbortedTransactionPoolSize)
            .Default(1000);
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TChaosNodeConfig
    : public NYTree::TYsonSerializable
{
public:
    NCellarAgent::TCellarOccupantConfigPtr CellarOccupant;

    TTransactionManagerConfigPtr TransactionManager;

    TChaosManagerConfigPtr ChaosManager;

    TDuration SlotScanPeriod;

    TChaosNodeConfig()
    {
        RegisterParameter("cellar_occupant", CellarOccupant)
            .DefaultNew();
        RegisterParameter("transaction_manager", TransactionManager)
            .DefaultNew();
        RegisterParameter("chaos_manager", ChaosManager)
            .DefaultNew();
        RegisterParameter("slot_scan_period", SlotScanPeriod)
            .Default(TDuration::Seconds(1));
    }
};

DEFINE_REFCOUNTED_TYPE(TChaosNodeConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
