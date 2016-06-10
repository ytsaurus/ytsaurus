#pragma once

#include "public.h"

#include "stream_writer.h"
#include "data_block_writer.h"

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

// TODO(psushin): this is WIP for non-strict unversioned chunks.

std::unique_ptr<IValueStreamWriter> CreateSchemalessStreamWriter(
    const std::vector<int>& ignoreIds,
    TDataBlockWriter* dataBlockWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
