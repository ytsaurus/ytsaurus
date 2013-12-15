#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/hive/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct IConnection
    : public virtual TRefCounted
{
    virtual NRpc::IChannelPtr GetMasterChannel() = 0;
    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual NTransactionClient::TTransactionManagerPtr GetTransactionManager() = 0;
    virtual NChunkClient::IBlockCachePtr GetBlockCache() = 0;
    virtual TTableMountCachePtr GetTableMountCache() = 0;
    virtual NHive::ITimestampProviderPtr GetTimestampProvider() = 0;
    virtual NHive::TCellDirectoryPtr GetCellDirectory() = 0;
};

IConnectionPtr CreateConnection(TConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

