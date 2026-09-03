#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/public.h>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

//! Presents a shuffle writer as an ordinary table writer, so a mapper can write into the
//! shuffle through the usual output path. The shuffle chunks belong to the controller
//! agent, so the adapter reports no written chunk specs.
/*!
 *  The records are partitioned and decoded by value position, so the adapter writes rows
 *  against #schema and derives its name table from it.
 */
NTableClient::ISchemalessMultiChunkWriterPtr CreateShuffleWriterAdapter(
    IPushBasedShuffleWriterPtr underlyingWriter,
    NTableClient::TTableSchemaPtr schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
