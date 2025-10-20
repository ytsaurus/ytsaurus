#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>

#include <yt/yt/library/event_log/event_log.h>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, EventLogWriterLogger, "EventLogWriter");

////////////////////////////////////////////////////////////////////////////////

IEventLogWriterPtr CreateStaticTableEventLogWriter(
    TStaticTableEventLogManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
