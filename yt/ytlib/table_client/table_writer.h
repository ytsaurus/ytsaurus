#pragma once

#include "public.h"
#include "sync_writer.h"
#include "async_writer.h"
#include "table_ypath_proxy.h"

#include <core/concurrency/thread_affinity.h>

#include <core/logging/log.h>

#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <ytlib/ypath/rich.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for writing tables.
/*!
 *  The client must first call #Open.
 *
 *  For each row to be written, the client must add its entries by calling #WriteRow.
 *
 *  Finally the client must call #Close.
 *  After this call the writer is no longer usable.
 */
IAsyncWriterPtr CreateAsyncTableWriter(
    TTableWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NTransactionClient::TTransactionPtr transaction,
    NTransactionClient::TTransactionManagerPtr transactionManager,
    const NYPath::TRichYPath& richPath,
    const TNullable<TKeyColumns>& keyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
