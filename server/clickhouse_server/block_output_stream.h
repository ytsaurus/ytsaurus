#pragma once

#include <yt/ytlib/table_client/public.h>

#include <yt/core/logging/log.h>

#include <DataStreams/IBlockOutputStream.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Create CH wrapper around our unversioned writer.
DB::BlockOutputStreamPtr CreateBlockOutputStream(NTableClient::IUnversionedWriterPtr writer, const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
