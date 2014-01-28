#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/hive/public.h>

#include <ytlib/tablet_client/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct IConnection
    : public virtual TRefCounted
{
    virtual TConnectionConfigPtr GetConfig() = 0;
    virtual NRpc::IChannelPtr GetMasterChannel() = 0;
    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual NChunkClient::IBlockCachePtr GetBlockCache() = 0;
    virtual NTabletClient::TTableMountCachePtr GetTableMountCache() = 0;
    virtual NHive::ITimestampProviderPtr GetTimestampProvider() = 0;
    virtual NHive::TCellDirectoryPtr GetCellDirectory() = 0;
};

IConnectionPtr CreateConnection(TConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

