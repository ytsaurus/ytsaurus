#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/ypath/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessWriterPtr CreateSchemalessBufferedTableWriter(
    TBufferedTableWriterConfigPtr config,
    NChunkClient::TRemoteWriterOptionsPtr options,
    NRpc::IChannelPtr masterChannel,
    NTransactionClient::TTransactionManagerPtr transactionManager,
    TNameTablePtr nameTable,
    const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
