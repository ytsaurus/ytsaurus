#include "event_log.h"
#include "config.h"

#include <yt/yt/ytlib/table_client/schemaless_buffered_table_writer.h>
#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/library/event_log/event_log.h>

#include <yt/yt/client/table_client/name_table.h>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

IEventLogWriterPtr CreateStaticTableEventLogWriter(
    TStaticTableEventLogManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions)
{
    auto nameTable = New<NTableClient::TNameTable>();
    auto options = New<NTableClient::TTableWriterOptions>();
    options->EnableValidationOptions();

    auto logWriter = CreateSchemalessBufferedTableWriter(
        config,
        options,
        client,
        nameTable,
        config->Path,
        std::move(writeBlocksOptions));

    return CreateEventLogWriter(std::move(config), std::move(invoker), std::move(logWriter), EventLogWriterLogger());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
