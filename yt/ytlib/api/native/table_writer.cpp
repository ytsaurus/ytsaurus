#include "table_writer.h"
#include "client.h"

#include <yt/client/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/config.h>

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NApi {
namespace NNative {

using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TFuture<ISchemalessWriterPtr> CreateTableWriter(
    const IClientPtr& client,
    const NYPath::TRichYPath& path,
    const TTableWriterOptions& options)
{
    auto nameTable = New<TNameTable>();
    nameTable->SetEnableColumnNameValidation();

    auto writerOptions = New<NTableClient::TTableWriterOptions>();
    writerOptions->EnableValidationOptions();

    NApi::ITransactionPtr transaction;
    if (options.TransactionId) {
        TTransactionAttachOptions transactionOptions;
        transactionOptions.Ping = options.Ping;
        transactionOptions.PingAncestors = options.PingAncestors;
        transaction = client->AttachTransaction(options.TransactionId, transactionOptions);
    }

    return CreateSchemalessTableWriter(
        options.Config,
        writerOptions,
        path,
        nameTable,
        client,
        transaction);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT

