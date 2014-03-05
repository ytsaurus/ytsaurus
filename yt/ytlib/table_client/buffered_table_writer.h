#pragma once

#include "public.h"

#include <ytlib/transaction_client/public.h>
#include <ytlib/ypath/rich.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr CreateBufferedTableWriter(
    TBufferedTableWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NTransactionClient::TTransactionManagerPtr transactionManager,
    const NYPath::TRichYPath richPath,
    const TNullable<TKeyColumns>& keyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
