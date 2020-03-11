#pragma once

#include "private.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/client/table_client/schema.h>

#include <yt/core/logging/log.h>

#include <DataStreams/IBlockInputStream.h>

#include <Storages/SelectQueryInfo.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreateBlockInputStream(
    NTableClient::ISchemalessReaderPtr reader,
    NTableClient::TTableSchema readSchema,
    NTracing::TTraceContextPtr traceContext,
    TBootstrap* bootstrap,
    NLogging::TLogger logger,
    DB::PrewhereInfoPtr prewhereInfo);

DB::BlockInputStreamPtr CreateBlockInputStreamLoggingAdapter(
    DB::BlockInputStreamPtr blockInputStream,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

}
