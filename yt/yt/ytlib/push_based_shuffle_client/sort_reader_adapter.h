#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/public.h>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

//! Presents a sort reader as an ordinary table reader, so a user job can consume the
//! shuffled rows through the usual input path. The rows are produced by the sort reader
//! itself, so the adapter supports neither interruption nor data slice introspection.
//! Batch size follows the sort reader's own configuration; the caller's read options are
//! ignored.
/*!
 *  The shuffled values are addressed by position, so #nameTable must give every shuffled
 *  schema column the id of its position, plus the identity column ids in
 *  identity-preserving mode.
 */
NTableClient::ISchemalessMultiChunkReaderPtr CreateSortReaderAdapter(
    ISortReaderPtr underlyingReader,
    NTableClient::TNameTablePtr nameTable,
    i64 totalRowCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
