#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/chunk_writer_base.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IChunkWriterBase
    : public NChunkClient::IChunkWriterBase
{
    virtual NProto::TBoundaryKeysExt GetBoundaryKeys() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
