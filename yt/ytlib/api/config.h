#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <ytlib/hydra/config.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/file_client/config.h>

#include <ytlib/table_client/config.h>

#include <ytlib/chunk_client/config.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/scheduler/public.h>

#include <ytlib/hive/public.h>

#include <ytlib/tablet_client/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectionConfig
    : public NHydra::TPeerConnectionConfig
{
public:
    //! Timeout for RPC requests to masters.
    TDuration RpcTimeout;

    TMasterConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public virtual TYsonSerializable
{
public:
    TMasterConnectionConfigPtr Master;
    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;
    TMasterConnectionConfigPtr MasterCache;
    NHive::TCellDirectoryConfigPtr CellDirectory;
    NScheduler::TSchedulerConnectionConfigPtr Scheduler;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NChunkClient::TClientBlockCacheConfigPtr BlockCache;
    NTabletClient::TTableMountCacheConfigPtr TableMountCache;

    TConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TFileReaderConfig
    : public NChunkClient::TMultiChunkReaderConfig
{ };

DEFINE_REFCOUNTED_TYPE(TFileReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TFileWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public NFileClient::TFileChunkWriterConfig
{ };

DEFINE_REFCOUNTED_TYPE(TFileWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

